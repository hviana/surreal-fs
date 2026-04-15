/**
 * @module SurrealFS
 * @description A resilient file system abstraction built on SurrealDB.
 *
 * Files are split into 64 KB chunks and stored across two tables:
 *   • `surreal_fs_files`  – one row per logical file (path, metadata, size, status)
 *   • `surreal_fs_chunks` – one row per 64 KB chunk, linked by `upload_id`
 *
 * Key capabilities:
 *   - Chunked upload / download with configurable KB/s rate limiting
 *   - Multi-factor concurrency control driven by path sub-elements
 *   - Atomic file replacement: old chunks are removed only after new upload completes
 *   - Crash-resilient garbage collector that cleans up incomplete operations on init
 *   - Cursor-based directory pagination (no table scans)
 *   - Purely web-standard APIs (ReadableStream, TransformStream, TextEncoder, crypto)
 *
 * Created by: Henrique Emanoel Viana
 * GitHub: https://github.com/hviana
 * Page: https://sites.google.com/view/henriqueviana
 * cel: +55 (41) 99999-4664
 *
 * @example
 * ```ts
 * import { SurrealFS } from "jsr:@hviana/surreal-fs";
 *
 * const db = new Surreal();
 * await db.connect("http://localhost:8000/rpc");
 * await db.signin({ username: "root", password: "root" });
 * await db.use({ namespace: "test", database: "test" });
 *
 * const fs = new SurrealFS(db);
 * await fs.init();
 *
 * await fs.save({ path: ["users", "u1", "avatar.png"], content: imageBytes });
 * const file = await fs.read({ path: ["users", "u1", "avatar.png"] });
 * ```
 */

// ─────────────────────────────────────────────────────────────────────────────
// Interfaces
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Result returned by the save control callback.
 * Controls access, throughput, concurrency, size limits and extension filtering.
 */
export interface SaveControlResult {
  /** Whether the caller is allowed to save at this path. Defaults to `true`. */
  accessAllowed?: boolean;
  /**
   * Write throughput limit in **kilobytes per second** (1 KB = 1024 bytes).
   * Converted internally to whole 64 KB chunks per second
   * (`ceil(kbytesPerSecond / 64)`, min 1).
   * Defaults to `Number.MAX_SAFE_INTEGER` (no limit).
   */
  kbytesPerSecond?: number;
  /**
   * Sub-elements of the path used as concurrency identifiers.
   * Each identifier is incremented by 1 in the save concurrency map when the
   * operation starts and decremented when it finishes. If it reaches 0 the key
   * is removed from the map.
   */
  concurrencyIdentifiers?: string[];
  /** Maximum file size in bytes. Defaults to `Number.MAX_SAFE_INTEGER`. */
  maxFileSizeBytes?: number;
  /** Allowed file extensions. An empty array means all extensions are allowed. */
  allowedExtensions?: string[];
}

/**
 * Result returned by the read control callback.
 * Controls access, throughput, concurrency and pagination.
 */
export interface ReadControlResult {
  /** Whether the caller is allowed to read at this path. Defaults to `true`. */
  accessAllowed?: boolean;
  /**
   * Read throughput limit in **kilobytes per second** (1 KB = 1024 bytes).
   * Converted internally to whole 64 KB chunks per second
   * (`ceil(kbytesPerSecond / 64)`, min 1).
   * Defaults to `Number.MAX_SAFE_INTEGER` (no limit).
   */
  kbytesPerSecond?: number;
  /**
   * Sub-elements of the path used as concurrency identifiers.
   * Same semantics as {@link SaveControlResult.concurrencyIdentifiers}.
   */
  concurrencyIdentifiers?: string[];
  /** Maximum number of entries returned per page in `readDir`. Defaults to `1000`. */
  maxPageSize?: number;
  /** Opaque cursor for `readDir` pagination. */
  cursor?: any;
}

/**
 * Options for saving a file.
 */
export interface SaveOptions {
  /**
   * Hierarchical path identifying the file.
   * Each element is URI-encoded when building the storage key.
   * @example `["company", "proj", "docs", "report.pdf"]`
   */
  path: string[];
  /** File content as a stream, byte array, or UTF-8 string. */
  content: ReadableStream<Uint8Array> | Uint8Array | string;
  /** Arbitrary JSON-serialisable metadata attached to the file. Max 60 KB. */
  metadata?: Record<string, any>;
  /**
   * Control callback invoked before saving.
   * Receives the file path and the current save concurrency map.
   * Must return a {@link SaveControlResult}.
   */
  control?: (
    path: string[],
    saveConcurrencyMap: Record<string, number | undefined>,
  ) => Promise<SaveControlResult> | SaveControlResult;
}

/**
 * Options for reading, listing, or deleting files.
 */
export interface ReadOptions {
  /**
   * Hierarchical path identifying the file (or directory prefix for readDir).
   */
  path: string[];
  /**
   * Control callback invoked before reading.
   * Receives the file path and the current read concurrency map.
   * Must return a {@link ReadControlResult}.
   */
  control?: (
    path: string[],
    readConcurrencyMap: Record<string, number | undefined>,
  ) => Promise<ReadControlResult> | ReadControlResult;
  /**
   * Byte offset to start reading from. The returned `content` stream will
   * begin at this position in the file, as if the first `skip` bytes had
   * been discarded. Only honoured by {@link SurrealFS.read} — ignored by
   * {@link SurrealFS.readDir}.
   *
   * Internally, the seek is chunk-aware: only the chunk containing the
   * starting offset is fetched and sliced; earlier chunks are never read
   * from the database.
   *
   * - `0` or omitted  → stream the full file (default).
   * - `size`          → returns an empty stream (end of file).
   * - `> size`        → clamped to `size` (empty stream).
   * - `< 0`           → treated as `0`.
   */
  skip?: number;
}

/**
 * Returned on successful save — contains the total size in bytes.
 */
export interface SaveResult {
  size: number;
}

/**
 * Represents a stored file, optionally with a content stream.
 */
export interface File {
  /** Hierarchical path. */
  path: string[];
  /** Total size in bytes. */
  size: number;
  /** Lazily-constructed ReadableStream of the file content. */
  content?: ReadableStream<Uint8Array>;
  /** URI-encoded representation of `path`. */
  URIComponent?: string;
  /** User-defined metadata. */
  metadata?: Record<string, any>;
  /**
   * SHA3-512 digest of the full (unsliced) file content, as 128 lowercase
   * hex characters. Computed iteratively during {@link SurrealFS.save}.
   *
   * Remains the digest of the complete file even when the content stream is
   * obtained with a non-zero `skip`.
   */
  hash?: string;
}

/**
 * A paginated directory listing.
 */
export interface DirList {
  /** Files (complete or in-progress) within the directory. */
  files: (File | FileStatus)[];
  /** Total byte size of all listed files. */
  size: number;
  /** Opaque cursor — pass to the next readDir call for the next page. */
  cursor?: any;
}

/**
 * Status of an in-progress or failed file operation.
 */
export interface FileStatus {
  /** URI-encoded path. */
  URIComponent: string;
  /** Hierarchical path. */
  path: string[];
  /** Bytes processed so far. */
  progress: number;
  /** Current operation state. */
  status: "saving" | "deleting" | "error";
  /** Optional human-readable message. */
  msg?: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Defaults
// ─────────────────────────────────────────────────────────────────────────────

/** @internal Default save control — everything allowed, no limits. */
const defaultSaveControl = (
  _path: string[],
  _map: Record<string, number | undefined>,
): SaveControlResult => ({
  accessAllowed: true,
  kbytesPerSecond: Number.MAX_SAFE_INTEGER,
  concurrencyIdentifiers: [],
  maxFileSizeBytes: Number.MAX_SAFE_INTEGER,
  allowedExtensions: [],
});

/** @internal Default read control — everything allowed, no limits. */
const defaultReadControl = (
  _path: string[],
  _map: Record<string, number | undefined>,
): ReadControlResult => ({
  accessAllowed: true,
  kbytesPerSecond: Number.MAX_SAFE_INTEGER,
  concurrencyIdentifiers: [],
  maxPageSize: 1000,
  cursor: undefined,
});

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/** @internal Shared TextEncoder instance. */
const ENC = new TextEncoder();

/** @internal Shared TextDecoder instance. */
const DEC = new TextDecoder();

/** @internal 64 KB chunk size. */
const CHUNK_SIZE = 65_536;

/** @internal Chunk size in kilobytes (1 KB = 1024 bytes). */
const CHUNK_SIZE_KB = CHUNK_SIZE / 1024;

/**
 * Convert a user-supplied KB/s throughput limit into the integer
 * chunks-per-second value consumed by {@link throttleTransform}.
 *
 * One chunk is {@link CHUNK_SIZE_KB} KB, so the conversion is
 * `ceil(kbps / CHUNK_SIZE_KB)`. A floor of 1 ensures any positive KB/s
 * setting permits forward progress (sub-chunk rates simply mean one chunk
 * per second). `Number.MAX_SAFE_INTEGER` is passed through untouched to
 * preserve the "no limit" sentinel.
 *
 * @internal
 */
function kbpsToChunksPerSecond(kbps: number): number {
  if (!Number.isFinite(kbps) || kbps >= Number.MAX_SAFE_INTEGER) {
    return Number.MAX_SAFE_INTEGER;
  }
  if (kbps <= 0) return 1;
  return Math.max(1, Math.ceil(kbps / CHUNK_SIZE_KB));
}

/**
 * Generate a universally unique upload identifier.
 * @internal
 */
function newUploadId(): string {
  return crypto.randomUUID();
}

/**
 * Safely extract an array of records from a `db.query()` result set.
 * Handles both the raw array format and the `{result: [...]}` wrapper.
 * @internal
 */
function queryRows<T = any>(result: any[], statementIndex = 0): T[] {
  const r = result[statementIndex];
  if (Array.isArray(r)) return r as T[];
  if (r && typeof r === "object" && "result" in r) return r.result as T[];
  return [];
}

/**
 * Create a {@link TransformStream} that re-chunks an arbitrary byte stream
 * into exact {@link CHUNK_SIZE} pieces (the final piece may be shorter).
 * @internal
 */
function chunkingTransform(): TransformStream<Uint8Array, Uint8Array> {
  let buf = new Uint8Array(0);
  return new TransformStream<Uint8Array, Uint8Array>({
    transform(incoming, ctl) {
      // Merge incoming data with the leftover buffer.
      const merged = new Uint8Array(buf.length + incoming.length);
      merged.set(buf);
      merged.set(incoming, buf.length);
      buf = merged;

      // Emit as many full chunks as possible.
      while (buf.length >= CHUNK_SIZE) {
        ctl.enqueue(buf.slice(0, CHUNK_SIZE));
        buf = buf.slice(CHUNK_SIZE);
      }
    },
    flush(ctl) {
      // Emit any remaining bytes as the final (shorter) chunk.
      if (buf.length > 0) {
        ctl.enqueue(buf);
        buf = new Uint8Array(0);
      }
    },
  });
}

/**
 * Create a {@link TransformStream} that throttles throughput to at most
 * `chunksPerSecond` chunks per 1-second sliding window.
 *
 * Uses `setTimeout` (web-standard).
 * @internal
 */
function throttleTransform(
  chunksPerSecond: number,
): TransformStream<Uint8Array, Uint8Array> {
  let count = 0;
  let windowStart = Date.now();
  return new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, ctl) {
      const now = Date.now();
      const elapsed = now - windowStart;
      if (elapsed >= 1000) {
        // New window.
        count = 0;
        windowStart = now;
      }
      count++;
      if (count > chunksPerSecond) {
        // Wait for the current window to expire.
        const wait = 1000 - (Date.now() - windowStart);
        if (wait > 0) {
          await new Promise<void>((r) => setTimeout(r, wait));
        }
        count = 1;
        windowStart = Date.now();
      }
      ctl.enqueue(chunk);
    },
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SHA3-512
//
// Faithful TypeScript port of the SHA3-512 variant of emn178/js-sha3
// (https://github.com/emn178/js-sha3, MIT). Only the SHA3-512 path is
// reproduced — constants, padding, state layout, update/finalize logic and
// the Keccak-f[1600] permutation are identical to the reference. Output is
// always lowercase hex.
//
// The class exposes an incremental API so large payloads may be hashed while
// streaming, without buffering the entire input in memory.
// ─────────────────────────────────────────────────────────────────────────────

/** @internal Lowercase hex alphabet used by {@link Sha3_512.hex}. */
const HEX_CHARS = "0123456789abcdef".split("");

/** @internal Byte-to-word shift lookup (little-endian lane packing). */
const SHIFT = [0, 8, 16, 24];

/** @internal SHA3 domain-separation padding (reference: `PADDING`). */
const SHA3_PADDING = [6, 1536, 393216, 100663296];

/** @internal Round constants for Keccak-f[1600] (lo/hi word pairs). */
const RC = [
  1,
  0,
  32898,
  0,
  32906,
  2147483648,
  2147516416,
  2147483648,
  32907,
  0,
  2147483649,
  0,
  2147516545,
  2147483648,
  32777,
  2147483648,
  138,
  0,
  136,
  0,
  2147516425,
  0,
  2147483658,
  0,
  2147516555,
  0,
  139,
  2147483648,
  32905,
  2147483648,
  32771,
  2147483648,
  32770,
  2147483648,
  128,
  2147483648,
  32778,
  0,
  2147483658,
  2147483648,
  2147516545,
  2147483648,
  32896,
  2147483648,
  2147483649,
  0,
  2147516424,
  2147483648,
];

/**
 * Keccak-f[1600] permutation — 24 rounds unrolled in pairs.
 * Operates in-place on a 50-element lo/hi word state.
 * @internal
 */
function keccakF(s: number[]): void {
  let h: number, l: number, n: number;
  let c0: number,
    c1: number,
    c2: number,
    c3: number,
    c4: number,
    c5: number,
    c6: number,
    c7: number,
    c8: number,
    c9: number;
  let b0: number,
    b1: number,
    b2: number,
    b3: number,
    b4: number,
    b5: number,
    b6: number,
    b7: number,
    b8: number,
    b9: number,
    b10: number,
    b11: number,
    b12: number,
    b13: number,
    b14: number,
    b15: number,
    b16: number,
    b17: number,
    b18: number,
    b19: number,
    b20: number,
    b21: number,
    b22: number,
    b23: number,
    b24: number,
    b25: number,
    b26: number,
    b27: number,
    b28: number,
    b29: number,
    b30: number,
    b31: number,
    b32: number,
    b33: number,
    b34: number,
    b35: number,
    b36: number,
    b37: number,
    b38: number,
    b39: number,
    b40: number,
    b41: number,
    b42: number,
    b43: number,
    b44: number,
    b45: number,
    b46: number,
    b47: number,
    b48: number,
    b49: number;

  for (n = 0; n < 48; n += 2) {
    c0 = s[0] ^ s[10] ^ s[20] ^ s[30] ^ s[40];
    c1 = s[1] ^ s[11] ^ s[21] ^ s[31] ^ s[41];
    c2 = s[2] ^ s[12] ^ s[22] ^ s[32] ^ s[42];
    c3 = s[3] ^ s[13] ^ s[23] ^ s[33] ^ s[43];
    c4 = s[4] ^ s[14] ^ s[24] ^ s[34] ^ s[44];
    c5 = s[5] ^ s[15] ^ s[25] ^ s[35] ^ s[45];
    c6 = s[6] ^ s[16] ^ s[26] ^ s[36] ^ s[46];
    c7 = s[7] ^ s[17] ^ s[27] ^ s[37] ^ s[47];
    c8 = s[8] ^ s[18] ^ s[28] ^ s[38] ^ s[48];
    c9 = s[9] ^ s[19] ^ s[29] ^ s[39] ^ s[49];

    h = c8 ^ ((c2 << 1) | (c3 >>> 31));
    l = c9 ^ ((c3 << 1) | (c2 >>> 31));
    s[0] ^= h;
    s[1] ^= l;
    s[10] ^= h;
    s[11] ^= l;
    s[20] ^= h;
    s[21] ^= l;
    s[30] ^= h;
    s[31] ^= l;
    s[40] ^= h;
    s[41] ^= l;
    h = c0 ^ ((c4 << 1) | (c5 >>> 31));
    l = c1 ^ ((c5 << 1) | (c4 >>> 31));
    s[2] ^= h;
    s[3] ^= l;
    s[12] ^= h;
    s[13] ^= l;
    s[22] ^= h;
    s[23] ^= l;
    s[32] ^= h;
    s[33] ^= l;
    s[42] ^= h;
    s[43] ^= l;
    h = c2 ^ ((c6 << 1) | (c7 >>> 31));
    l = c3 ^ ((c7 << 1) | (c6 >>> 31));
    s[4] ^= h;
    s[5] ^= l;
    s[14] ^= h;
    s[15] ^= l;
    s[24] ^= h;
    s[25] ^= l;
    s[34] ^= h;
    s[35] ^= l;
    s[44] ^= h;
    s[45] ^= l;
    h = c4 ^ ((c8 << 1) | (c9 >>> 31));
    l = c5 ^ ((c9 << 1) | (c8 >>> 31));
    s[6] ^= h;
    s[7] ^= l;
    s[16] ^= h;
    s[17] ^= l;
    s[26] ^= h;
    s[27] ^= l;
    s[36] ^= h;
    s[37] ^= l;
    s[46] ^= h;
    s[47] ^= l;
    h = c6 ^ ((c0 << 1) | (c1 >>> 31));
    l = c7 ^ ((c1 << 1) | (c0 >>> 31));
    s[8] ^= h;
    s[9] ^= l;
    s[18] ^= h;
    s[19] ^= l;
    s[28] ^= h;
    s[29] ^= l;
    s[38] ^= h;
    s[39] ^= l;
    s[48] ^= h;
    s[49] ^= l;

    b0 = s[0];
    b1 = s[1];
    b32 = (s[11] << 4) | (s[10] >>> 28);
    b33 = (s[10] << 4) | (s[11] >>> 28);
    b14 = (s[20] << 3) | (s[21] >>> 29);
    b15 = (s[21] << 3) | (s[20] >>> 29);
    b46 = (s[31] << 9) | (s[30] >>> 23);
    b47 = (s[30] << 9) | (s[31] >>> 23);
    b28 = (s[40] << 18) | (s[41] >>> 14);
    b29 = (s[41] << 18) | (s[40] >>> 14);
    b20 = (s[2] << 1) | (s[3] >>> 31);
    b21 = (s[3] << 1) | (s[2] >>> 31);
    b2 = (s[13] << 12) | (s[12] >>> 20);
    b3 = (s[12] << 12) | (s[13] >>> 20);
    b34 = (s[22] << 10) | (s[23] >>> 22);
    b35 = (s[23] << 10) | (s[22] >>> 22);
    b16 = (s[33] << 13) | (s[32] >>> 19);
    b17 = (s[32] << 13) | (s[33] >>> 19);
    b48 = (s[42] << 2) | (s[43] >>> 30);
    b49 = (s[43] << 2) | (s[42] >>> 30);
    b40 = (s[5] << 30) | (s[4] >>> 2);
    b41 = (s[4] << 30) | (s[5] >>> 2);
    b22 = (s[14] << 6) | (s[15] >>> 26);
    b23 = (s[15] << 6) | (s[14] >>> 26);
    b4 = (s[25] << 11) | (s[24] >>> 21);
    b5 = (s[24] << 11) | (s[25] >>> 21);
    b36 = (s[34] << 15) | (s[35] >>> 17);
    b37 = (s[35] << 15) | (s[34] >>> 17);
    b18 = (s[45] << 29) | (s[44] >>> 3);
    b19 = (s[44] << 29) | (s[45] >>> 3);
    b10 = (s[6] << 28) | (s[7] >>> 4);
    b11 = (s[7] << 28) | (s[6] >>> 4);
    b42 = (s[17] << 23) | (s[16] >>> 9);
    b43 = (s[16] << 23) | (s[17] >>> 9);
    b24 = (s[26] << 25) | (s[27] >>> 7);
    b25 = (s[27] << 25) | (s[26] >>> 7);
    b6 = (s[36] << 21) | (s[37] >>> 11);
    b7 = (s[37] << 21) | (s[36] >>> 11);
    b38 = (s[47] << 24) | (s[46] >>> 8);
    b39 = (s[46] << 24) | (s[47] >>> 8);
    b30 = (s[8] << 27) | (s[9] >>> 5);
    b31 = (s[9] << 27) | (s[8] >>> 5);
    b12 = (s[18] << 20) | (s[19] >>> 12);
    b13 = (s[19] << 20) | (s[18] >>> 12);
    b44 = (s[29] << 7) | (s[28] >>> 25);
    b45 = (s[28] << 7) | (s[29] >>> 25);
    b26 = (s[38] << 8) | (s[39] >>> 24);
    b27 = (s[39] << 8) | (s[38] >>> 24);
    b8 = (s[48] << 14) | (s[49] >>> 18);
    b9 = (s[49] << 14) | (s[48] >>> 18);

    s[0] = b0 ^ (~b2 & b4);
    s[1] = b1 ^ (~b3 & b5);
    s[10] = b10 ^ (~b12 & b14);
    s[11] = b11 ^ (~b13 & b15);
    s[20] = b20 ^ (~b22 & b24);
    s[21] = b21 ^ (~b23 & b25);
    s[30] = b30 ^ (~b32 & b34);
    s[31] = b31 ^ (~b33 & b35);
    s[40] = b40 ^ (~b42 & b44);
    s[41] = b41 ^ (~b43 & b45);
    s[2] = b2 ^ (~b4 & b6);
    s[3] = b3 ^ (~b5 & b7);
    s[12] = b12 ^ (~b14 & b16);
    s[13] = b13 ^ (~b15 & b17);
    s[22] = b22 ^ (~b24 & b26);
    s[23] = b23 ^ (~b25 & b27);
    s[32] = b32 ^ (~b34 & b36);
    s[33] = b33 ^ (~b35 & b37);
    s[42] = b42 ^ (~b44 & b46);
    s[43] = b43 ^ (~b45 & b47);
    s[4] = b4 ^ (~b6 & b8);
    s[5] = b5 ^ (~b7 & b9);
    s[14] = b14 ^ (~b16 & b18);
    s[15] = b15 ^ (~b17 & b19);
    s[24] = b24 ^ (~b26 & b28);
    s[25] = b25 ^ (~b27 & b29);
    s[34] = b34 ^ (~b36 & b38);
    s[35] = b35 ^ (~b37 & b39);
    s[44] = b44 ^ (~b46 & b48);
    s[45] = b45 ^ (~b47 & b49);
    s[6] = b6 ^ (~b8 & b0);
    s[7] = b7 ^ (~b9 & b1);
    s[16] = b16 ^ (~b18 & b10);
    s[17] = b17 ^ (~b19 & b11);
    s[26] = b26 ^ (~b28 & b20);
    s[27] = b27 ^ (~b29 & b21);
    s[36] = b36 ^ (~b38 & b30);
    s[37] = b37 ^ (~b39 & b31);
    s[46] = b46 ^ (~b48 & b40);
    s[47] = b47 ^ (~b49 & b41);
    s[8] = b8 ^ (~b0 & b2);
    s[9] = b9 ^ (~b1 & b3);
    s[18] = b18 ^ (~b10 & b12);
    s[19] = b19 ^ (~b11 & b13);
    s[28] = b28 ^ (~b20 & b22);
    s[29] = b29 ^ (~b21 & b23);
    s[38] = b38 ^ (~b30 & b32);
    s[39] = b39 ^ (~b31 & b33);
    s[48] = b48 ^ (~b40 & b42);
    s[49] = b49 ^ (~b41 & b43);

    s[0] ^= RC[n];
    s[1] ^= RC[n + 1];
  }
}

/**
 * Incremental SHA3-512 hasher.
 *
 * Mirrors the SHA3-512 variant of the emn178/js-sha3 reference implementation
 * and produces byte-identical digests. The algorithm parameters are fixed:
 *
 *   - rate      : 576 bits (18 × 32-bit words, 72 bytes per block)
 *   - capacity  : 1024 bits
 *   - output    : 512 bits (128 lowercase hex characters)
 *   - padding   : SHA3 domain separation (`0x06 … 0x80`)
 *
 * Usage:
 *
 * ```ts
 * const h = new Sha3_512();
 * h.update(firstChunk);
 * h.update(secondChunk);
 * const digest = h.hex(); // 128-char lowercase hex
 * ```
 *
 * After {@link hex} is called the instance is finalized — any subsequent
 * {@link update} call throws.
 */
export class Sha3_512 {
  /** @internal 19-word message-scheduling buffer. */
  private blocks: number[] = [];
  /** @internal 50-word (25 × 64-bit) Keccak state, stored as lo/hi pairs. */
  private s: number[];
  /** @internal Whether the buffer needs to be zeroed before the next absorb. */
  private reset = true;
  /** @internal Whether {@link hex} has already been called. */
  private finalized = false;
  /** @internal Carry-over word between update() calls. */
  private block = 0;
  /** @internal Next byte-offset to write inside {@link blocks}. */
  private start = 0;
  /** @internal Byte offset reached inside the current block (for finalize). */
  private lastByteIndex = 0;

  /** @internal Rate in 32-bit words: (1600 − 2·512) ÷ 32 = 18. */
  private readonly blockCount = 18;
  /** @internal Rate in bytes: {@link blockCount} · 4 = 72. */
  private readonly byteCount = 72;
  /** @internal Output size in 32-bit words: 512 ÷ 32 = 16. */
  private readonly outputBlocks = 16;

  /**
   * Create a fresh SHA3-512 hashing context.
   */
  constructor() {
    this.s = new Array<number>(50);
    for (let i = 0; i < 50; ++i) this.s[i] = 0;
  }

  /**
   * Absorb more bytes into the hash.
   *
   * May be called repeatedly with any slice sizes; the absorb phase is
   * buffered internally so input need not be block-aligned.
   *
   * @param message - Bytes to feed into the sponge.
   * @returns `this`, for chaining.
   * @throws If called after {@link hex}.
   */
  update(message: Uint8Array): this {
    if (this.finalized) throw new Error("finalize already called");
    const blocks = this.blocks;
    const byteCount = this.byteCount;
    const blockCount = this.blockCount;
    const length = message.length;
    const s = this.s;
    let index = 0;
    let i: number;

    while (index < length) {
      if (this.reset) {
        this.reset = false;
        blocks[0] = this.block;
        for (i = 1; i < blockCount + 1; ++i) blocks[i] = 0;
      }
      for (i = this.start; index < length && i < byteCount; ++index) {
        blocks[i >> 2] |= message[index] << SHIFT[i++ & 3];
      }
      this.lastByteIndex = i;
      if (i >= byteCount) {
        this.start = i - byteCount;
        this.block = blocks[blockCount];
        for (i = 0; i < blockCount; ++i) s[i] ^= blocks[i];
        keccakF(s);
        this.reset = true;
      } else {
        this.start = i;
      }
    }
    return this;
  }

  /**
   * Apply SHA3 padding and run the final permutation.
   * @internal
   */
  private finalize(): void {
    if (this.finalized) return;
    this.finalized = true;
    const blocks = this.blocks;
    const blockCount = this.blockCount;
    const s = this.s;
    let i = this.lastByteIndex;
    blocks[i >> 2] |= SHA3_PADDING[i & 3];
    if (this.lastByteIndex === this.byteCount) {
      blocks[0] = blocks[blockCount];
      for (i = 1; i < blockCount + 1; ++i) blocks[i] = 0;
    }
    blocks[blockCount - 1] |= 0x80000000;
    for (i = 0; i < blockCount; ++i) s[i] ^= blocks[i];
    keccakF(s);
  }

  /**
   * Finalize and return the 128-character lowercase hex digest.
   *
   * After this call the instance is sealed — further {@link update} calls
   * will throw.
   *
   * @returns The SHA3-512 hash as hex (128 lowercase characters).
   */
  hex(): string {
    this.finalize();
    const blockCount = this.blockCount;
    const outputBlocks = this.outputBlocks;
    const s = this.s;
    let hex = "";
    let block: number;
    // For SHA3-512, outputBlocks (16) < blockCount (18), so a single
    // squeeze round covers the full digest — no re-permutation needed.
    for (let i = 0; i < blockCount && i < outputBlocks; ++i) {
      block = s[i];
      hex += HEX_CHARS[(block >> 4) & 0x0F] + HEX_CHARS[block & 0x0F] +
        HEX_CHARS[(block >> 12) & 0x0F] + HEX_CHARS[(block >> 8) & 0x0F] +
        HEX_CHARS[(block >> 20) & 0x0F] + HEX_CHARS[(block >> 16) & 0x0F] +
        HEX_CHARS[(block >> 28) & 0x0F] + HEX_CHARS[(block >> 24) & 0x0F];
    }
    return hex;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SurrealFS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * A resilient, chunked file system built on top of SurrealDB.
 *
 * ## Two-table schema
 *
 * | Table                | Purpose                                      |
 * | -------------------- | -------------------------------------------- |
 * | `surreal_fs_files`   | One row per logical file (path, meta, size)  |
 * | `surreal_fs_chunks`  | One row per 64 KB chunk, keyed by upload_id  |
 *
 * ## Lifecycle
 *
 * ```
 * const fs = new SurrealFS(db);
 * await fs.init();          // Creates tables, indexes, runs GC
 * await fs.save({...});     // Upload
 * const f = await fs.read({...}); // Download (content is a ReadableStream)
 * await fs.delete({...});   // Remove
 * ```
 */
export class SurrealFS {
  // ── Private state ────────────────────────────────────────────────────────

  /** SurrealDB connection. */
  private db: any;

  /**
   * In-memory concurrency map for **save** operations.
   * Keys are the `concurrencyIdentifiers` supplied by the control callback.
   * Values are the number of concurrent saves using that identifier.
   */
  private _saveConcurrencyMap: Record<string, number | undefined> = {};

  /**
   * In-memory concurrency map for **read** / **delete** operations.
   * Same semantics as `_saveConcurrencyMap`.
   */
  private _readConcurrencyMap: Record<string, number | undefined> = {};

  /**
   * URIComponent → bytes saved so far for files currently being uploaded.
   * Presence in this map means the file is locked for writing.
   */
  private _savingFiles: Record<string, number> = {};

  /**
   * URIComponent → bytes deleted so far for files currently being removed.
   * Presence in this map means the file is locked for deletion.
   */
  private _deletingFiles: Record<string, number> = {};

  // ── Public ───────────────────────────────────────────────────────────────

  /**
   * Callback invoked whenever a file operation makes progress or finishes.
   * Override to implement progress bars, logging, etc.
   *
   * @example
   * ```ts
   * fs.onFileProgress = (s) => console.log(`${s.status} ${s.URIComponent} ${s.progress}`);
   * ```
   */
  onFileProgress: (status: FileStatus) => void = () => {};

  // ── Constructor ──────────────────────────────────────────────────────────

  /**
   * Create a new SurrealFS instance.
   *
   * @param db - An already-connected `Surreal` instance.
   *             The caller is responsible for `connect`, `signin`, and `use`.
   */
  constructor(db: any) {
    this.db = db;
  }

  // ── Initialisation ───────────────────────────────────────────────────────

  /**
   * Idempotently create tables and indexes, then run the garbage collector
   * to clean up any incomplete operations left by a previous crash.
   *
   * **Must be called once** before any other method.
   *
   * @example
   * ```ts
   * const fs = new SurrealFS(db);
   * await fs.init();
   * ```
   */
  async init(): Promise<void> {
    // ── Schema definition ────────────────────────────────────────────────
    // Uses IF NOT EXISTS so it is safe to call init() on every startup.
    await this.db.query(`
      -- File metadata table
      DEFINE TABLE IF NOT EXISTS surreal_fs_files SCHEMALESS;
      DEFINE INDEX IF NOT EXISTS idx_sfs_files_uri
        ON TABLE surreal_fs_files FIELDS uri UNIQUE;
      DEFINE INDEX IF NOT EXISTS idx_sfs_files_status
        ON TABLE surreal_fs_files FIELDS status;

      -- Chunk data table
      DEFINE TABLE IF NOT EXISTS surreal_fs_chunks SCHEMALESS;
      DEFINE INDEX IF NOT EXISTS idx_sfs_chunks_upload
        ON TABLE surreal_fs_chunks FIELDS upload_id, chunk_index UNIQUE;
      DEFINE INDEX IF NOT EXISTS idx_sfs_chunks_upload_id
        ON TABLE surreal_fs_chunks FIELDS upload_id;
    `);

    // ── Garbage collection (runs concurrently) ───────────────────────────
    this.garbageCollect();
  }

  // ── Static utility methods ─────────────────────────────────────────────

  /**
   * Read an entire ReadableStream into a single Uint8Array.
   *
   * @param stream - A ReadableStream of bytes.
   * @returns The concatenated Uint8Array.
   *
   * @example
   * ```ts
   * const bytes = await SurrealFS.readStream(file.content!);
   * ```
   */
  static async readStream(
    stream: ReadableStream<Uint8Array>,
  ): Promise<Uint8Array> {
    return new Uint8Array(await new Response(stream).arrayBuffer());
  }

  /**
   * Read an entire ReadableStream into a UTF-8 string.
   *
   * @param stream - A ReadableStream of bytes.
   * @returns The decoded string.
   *
   * @example
   * ```ts
   * const text = await SurrealFS.readStreamAsString(file.content!);
   * ```
   */
  static async readStreamAsString(
    stream: ReadableStream<Uint8Array>,
  ): Promise<string> {
    return DEC.decode(await SurrealFS.readStream(stream));
  }

  // ── Path / URI helpers ─────────────────────────────────────────────────

  /**
   * Convert a hierarchical path array into a URI-encoded string.
   * Each path element is individually encoded; elements are joined by `/`.
   *
   * @example
   * ```ts
   * fs.pathToURIComponent(["users", "café", "photo.jpg"])
   * // → "users/caf%C3%A9/photo.jpg"
   * ```
   */
  pathToURIComponent(path: string[]): string {
    return path.map(encodeURIComponent).join("/");
  }

  /**
   * Convert a URI-encoded string back into a path array.
   *
   * @example
   * ```ts
   * fs.URIComponentToPath("users/caf%C3%A9/photo.jpg")
   * // → ["users", "café", "photo.jpg"]
   * ```
   */
  URIComponentToPath(uri: string): string[] {
    return uri.split("/").map(decodeURIComponent);
  }

  // ── Status inspection ──────────────────────────────────────────────────

  /**
   * Return a snapshot of every file currently being saved or deleted.
   *
   * @returns An array of {@link FileStatus} entries.
   *
   * @example
   * ```ts
   * for (const s of fs.getAllFilesStatuses()) {
   *   console.log(s.status, s.URIComponent, s.progress);
   * }
   * ```
   */
  getAllFilesStatuses(): FileStatus[] {
    const res: FileStatus[] = [];
    for (const uri in this._savingFiles) {
      res.push({
        URIComponent: uri,
        path: this.URIComponentToPath(uri),
        progress: this._savingFiles[uri],
        status: "saving",
      });
    }
    for (const uri in this._deletingFiles) {
      res.push({
        URIComponent: uri,
        path: this.URIComponentToPath(uri),
        progress: this._deletingFiles[uri],
        status: "deleting",
      });
    }
    return res;
  }

  /**
   * Returns a read-only snapshot of the save concurrency map.
   * Useful for monitoring or debugging concurrency.
   */
  getSaveConcurrencyMap(): Readonly<Record<string, number | undefined>> {
    return { ...this._saveConcurrencyMap };
  }

  /**
   * Returns a read-only snapshot of the read concurrency map.
   */
  getReadConcurrencyMap(): Readonly<Record<string, number | undefined>> {
    return { ...this._readConcurrencyMap };
  }

  // ── SAVE ───────────────────────────────────────────────────────────────

  /**
   * Save a file to SurrealDB.
   *
   * If the file already exists, it is **atomically replaced**: the old chunks
   * are deleted only after the new upload is fully committed.
   *
   * Content may be a {@link ReadableStream}, a {@link Uint8Array}, or a plain
   * string (automatically UTF-8 encoded).
   *
   * @param options - Save parameters including path, content, metadata, and control.
   * @returns A {@link File} on success, or a {@link FileStatus} on error / in-progress.
   *
   * @example
   * ```ts
   * // Save a text file with rate limiting
   * const result = await fs.save({
   *   path: ["tenant-1", "reports", "q4.txt"],
   *   content: "Revenue is up!",
   *   metadata: { author: "CFO" },
   *   control: (path, map) => ({
   *     accessAllowed: true,
   *     kbytesPerSecond: 3200, // ~50 chunks/s
   *     concurrencyIdentifiers: [path[0]],
   *     maxFileSizeBytes: 10 * 1024 * 1024,
   *     allowedExtensions: ["txt", "pdf"],
   *   }),
   * });
   * ```
   */
  async save(options: SaveOptions): Promise<FileStatus | File> {
    const uri = this.pathToURIComponent(options.path);

    // ── If the file is already in-progress, return its status ─────────
    const existing = this.fileStatus(uri);
    if (existing) return existing;

    // ── Validate metadata size (max 60 KB) ───────────────────────────
    if (options.metadata) {
      const metaSize = ENC.encode(JSON.stringify(options.metadata)).length;
      if (metaSize > 60 * 1024) {
        return this.emitError(
          uri,
          options.path,
          "Metadata exceeds 60 KB limit",
        );
      }
    }

    // ── Run control callback ─────────────────────────────────────────
    const controlFn = options.control ?? defaultSaveControl;
    const ctrl = await controlFn(options.path, this._saveConcurrencyMap);

    if (ctrl.accessAllowed === false) {
      return this.emitError(uri, options.path, "Forbidden");
    }

    // ── Validate file extension ──────────────────────────────────────
    const allowedExts = ctrl.allowedExtensions ?? [];
    if (allowedExts.length > 0) {
      const fileName = options.path[options.path.length - 1];
      const ext = fileName.split(".").pop() ?? "";
      if (!allowedExts.includes(ext)) {
        return this.emitError(
          uri,
          options.path,
          `Extension not allowed (${ext} in ${fileName}), allowed: ${
            allowedExts.join(", ")
          }`,
        );
      }
    }

    // ── Increment concurrency counters ───────────────────────────────
    const identifiers = ctrl.concurrencyIdentifiers ?? [];
    this.incrementConcurrency(identifiers, this._saveConcurrencyMap);
    this._savingFiles[uri] = 0;

    const chunksPerSecond = kbpsToChunksPerSecond(
      ctrl.kbytesPerSecond ?? Number.MAX_SAFE_INTEGER,
    );
    const maxSize = ctrl.maxFileSizeBytes ?? Number.MAX_SAFE_INTEGER;
    const uploadId = newUploadId();

    try {
      // ── Check for existing file (atomic replacement) ─────────────
      const existingFile = await this.getFileRecord(uri);
      const oldUploadId = existingFile?.upload_id ?? null;

      if (existingFile) {
        // Mark that a replacement is in progress.
        await this.db.query(
          `UPDATE surreal_fs_files SET pending_upload_id = $uid, updated_at = time::now() WHERE uri = $uri`,
          { uid: uploadId, uri },
        );
      } else {
        // New file — create a placeholder record with status = "saving".
        await this.db.query(
          `CREATE surreal_fs_files SET
             uri            = $uri,
             path           = $path,
             size           = 0,
             metadata       = $meta,
             upload_id      = $uid,
             pending_upload_id = NONE,
             status         = 'saving',
             created_at     = time::now(),
             updated_at     = time::now()`,
          {
            uri,
            path: options.path,
            meta: options.metadata ?? {},
            uid: uploadId,
          },
        );
      }

      // ── Normalise content to a ReadableStream ──────────────────────
      let stream: ReadableStream<Uint8Array>;
      if (typeof options.content === "string") {
        const encoded = ENC.encode(options.content);
        stream = new ReadableStream({
          start(c) {
            c.enqueue(encoded);
            c.close();
          },
        });
      } else if (options.content instanceof Uint8Array) {
        const bytes = options.content;
        stream = new ReadableStream({
          start(c) {
            c.enqueue(bytes);
            c.close();
          },
        });
      } else {
        stream = options.content;
      }

      // ── Pipe through chunking → throttle ───────────────────────────
      const processed = stream
        .pipeThrough(chunkingTransform())
        .pipeThrough(throttleTransform(chunksPerSecond));

      const reader = processed.getReader();
      let chunkIndex = 0;
      let sizeBytes = 0;
      let exceededLimit = false;
      // Iterative SHA3-512 hasher — fed one chunk at a time so the entire
      // file never has to be held in memory.
      const hasher = new Sha3_512();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        sizeBytes += value.length;
        if (sizeBytes > maxSize) {
          exceededLimit = true;
          break;
        }

        hasher.update(value);

        chunkIndex++;
        await this.db.query(
          `CREATE surreal_fs_chunks SET
             upload_id   = $uid,
             chunk_index = $idx,
             data        = $data`,
          { uid: uploadId, idx: chunkIndex, data: value },
        );

        this._savingFiles[uri] = sizeBytes;
        this.onFileProgress(this.fileStatus(uri)!);
      }
      const fileHash = exceededLimit ? "" : hasher.hex();

      if (exceededLimit) {
        // Clean up the partial upload.
        await this.deleteChunksByUploadId(uploadId);
        if (!existingFile) {
          await this.db.query(`DELETE surreal_fs_files WHERE uri = $uri`, {
            uri,
          });
        } else {
          await this.db.query(
            `UPDATE surreal_fs_files SET pending_upload_id = NONE, updated_at = time::now() WHERE uri = $uri`,
            { uri },
          );
        }
        this.cleanupSave(identifiers, uri);
        return this.emitError(
          uri,
          options.path,
          `File exceeded the maximum allowed size of ${maxSize} bytes`,
        );
      }

      // ── Commit the file record ────────────────────────────────────
      await this.db.query(
        `UPDATE surreal_fs_files SET
           upload_id          = $uid,
           pending_upload_id  = NONE,
           path               = $path,
           size               = $size,
           metadata           = $meta,
           hash               = $hash,
           status             = 'complete',
           updated_at         = time::now()
         WHERE uri = $uri`,
        {
          uid: uploadId,
          path: options.path,
          size: sizeBytes,
          meta: options.metadata ?? {},
          hash: fileHash,
          uri,
        },
      );

      // ── Delete old chunks (if replacing) ───────────────────────────
      if (oldUploadId) {
        // Fire-and-forget: old chunks cleaned in background.
        this.deleteChunksByUploadId(oldUploadId).catch(() => {});
      }

      const file: File = {
        path: options.path,
        URIComponent: uri,
        metadata: options.metadata ?? {},
        size: sizeBytes,
        hash: fileHash,
      };

      this.cleanupSave(identifiers, uri);
      this.onFileProgress({
        URIComponent: uri,
        path: options.path,
        progress: sizeBytes,
        status: "saving",
      });
      return file;
    } catch (e: any) {
      // ── Rollback on error ──────────────────────────────────────────
      try {
        await this.deleteChunksByUploadId(uploadId);
        const rec = await this.getFileRecord(uri);
        if (rec && rec.status === "saving") {
          await this.db.query(`DELETE surreal_fs_files WHERE uri = $uri`, {
            uri,
          });
        } else if (rec && rec.pending_upload_id) {
          await this.db.query(
            `UPDATE surreal_fs_files SET pending_upload_id = NONE, updated_at = time::now() WHERE uri = $uri`,
            { uri },
          );
        }
      } catch { /* best-effort cleanup */ }

      this.cleanupSave(identifiers, uri);
      console.error("SurrealFS save error:", e);
      return this.emitError(uri, options.path, e.message ?? JSON.stringify(e));
    }
  }

  // ── READ ───────────────────────────────────────────────────────────────

  /**
   * Read a single file from SurrealDB.
   *
   * The returned {@link File} object's `content` property is a lazy
   * {@link ReadableStream} — data is only fetched from the database as you
   * consume the stream.
   *
   * @param options - Read parameters including path and optional control.
   * @returns A {@link File} if found, a {@link FileStatus} if in-progress, or `null` if missing.
   *
   * @example
   * ```ts
   * const file = await fs.read({ path: ["users", "u1", "avatar.png"] });
   * if (file && "content" in file && file.content) {
   *   const bytes = await SurrealFS.readStream(file.content);
   * }
   * ```
   */
  async read(options: ReadOptions): Promise<File | FileStatus | null> {
    const uri = this.pathToURIComponent(options.path);

    const status = this.fileStatus(uri);
    if (status) return status;

    const controlFn = options.control ?? defaultReadControl;
    const ctrl = await controlFn(options.path, this._readConcurrencyMap);

    if (ctrl.accessAllowed === false) {
      return this.emitError(uri, options.path, "Forbidden");
    }

    const record = await this.getFileRecord(uri);
    if (!record || record.status !== "complete") return null;

    const file: File = {
      path: record.path,
      size: record.size,
      URIComponent: uri,
      metadata: record.metadata ?? {},
      hash: record.hash ?? undefined,
    };

    const identifiers = ctrl.concurrencyIdentifiers ?? [];
    const chunksPerSecond = kbpsToChunksPerSecond(
      ctrl.kbytesPerSecond ?? Number.MAX_SAFE_INTEGER,
    );

    // Clamp skip to [0, size]. A skip at or past EOF produces an empty stream.
    const rawSkip = options.skip ?? 0;
    const skip = Math.max(
      0,
      Math.min(rawSkip < 0 ? 0 : rawSkip, record.size ?? 0),
    );

    file.content = this.buildContentStream(
      record.upload_id,
      chunksPerSecond,
      identifiers,
      skip,
    );

    return file;
  }

  // ── READ DIR ───────────────────────────────────────────────────────────

  /**
   * List all files under a given directory path, with cursor-based pagination.
   *
   * @param options - The directory path and optional control callback.
   * @returns A {@link DirList} containing files / statuses and an optional cursor.
   *
   * @example
   * ```ts
   * // First page
   * let page = await fs.readDir({ path: ["tenant-1", "docs"] });
   * console.log(page.files);
   *
   * // Next page
   * if (page.cursor) {
   *   page = await fs.readDir({
   *     path: ["tenant-1", "docs"],
   *     control: (p, m) => ({ ...defaultCtrl, cursor: page.cursor }),
   *   });
   * }
   * ```
   */
  async readDir(options: ReadOptions): Promise<DirList> {
    const controlFn = options.control ?? defaultReadControl;
    const ctrl = await controlFn(options.path, this._readConcurrencyMap);

    if (ctrl.accessAllowed === false) {
      const s = this.emitError(
        this.pathToURIComponent(options.path),
        options.path,
        "Forbidden",
      );
      return { files: [s], size: 0 };
    }

    const maxPageSize = ctrl.maxPageSize ?? 1000;
    const cursor: string | undefined = ctrl.cursor;
    const uri = this.pathToURIComponent(options.path);
    const prefix = uri ? uri + "/" : "";

    // ── Build the SurQL query (cursor-based, no table scan) ──────────
    let sql: string;
    let params: Record<string, any>;

    if (prefix === "") {
      // Root listing — all files.
      if (cursor) {
        sql =
          `SELECT * FROM surreal_fs_files WHERE uri > $cursor ORDER BY uri LIMIT $lim`;
        params = { cursor, lim: maxPageSize };
      } else {
        sql = `SELECT * FROM surreal_fs_files ORDER BY uri LIMIT $lim`;
        params = { lim: maxPageSize };
      }
    } else {
      if (cursor) {
        sql = `SELECT * FROM surreal_fs_files
               WHERE string::starts_with(uri, $prefix) AND uri > $cursor
               ORDER BY uri LIMIT $lim`;
        params = { prefix, cursor, lim: maxPageSize };
      } else {
        sql = `SELECT * FROM surreal_fs_files
               WHERE string::starts_with(uri, $prefix)
               ORDER BY uri LIMIT $lim`;
        params = { prefix, lim: maxPageSize };
      }
    }

    const rows = queryRows<any>(await this.db.query(sql, params));
    const res: DirList = { files: [], size: 0 };
    const chunksPerSecond = kbpsToChunksPerSecond(
      ctrl.kbytesPerSecond ?? Number.MAX_SAFE_INTEGER,
    );
    const identifiers = ctrl.concurrencyIdentifiers ?? [];

    for (const row of rows) {
      const fileUri: string = row.uri;
      const inProgress = this.fileStatus(fileUri);

      if (inProgress) {
        if (inProgress.status === "saving" && inProgress.progress) {
          res.size += inProgress.progress;
        }
        res.files.push(inProgress);
      } else {
        const f: File = {
          path: row.path,
          size: row.size ?? 0,
          URIComponent: fileUri,
          metadata: row.metadata ?? {},
          hash: row.hash ?? undefined,
        };
        if (row.status === "complete" && row.upload_id) {
          f.content = this.buildContentStream(
            row.upload_id,
            chunksPerSecond,
            identifiers,
          );
        }
        res.size += f.size;
        res.files.push(f);
      }
    }

    // Set cursor for next page if we hit the limit.
    if (rows.length === maxPageSize) {
      const lastUri = rows[rows.length - 1].uri;
      res.cursor = lastUri;
    }

    return res;
  }

  // ── METADATA ───────────────────────────────────────────────────────────

  /**
   * Overwrite the metadata of an existing file.
   *
   * @param path  - The file path.
   * @param metadata - New metadata object (max 60 KB).
   * @throws If metadata exceeds 60 KB or the file does not exist.
   *
   * @example
   * ```ts
   * await fs.setMetadata(["reports", "q4.pdf"], { reviewed: true, reviewer: "Alice" });
   * ```
   */
  async setMetadata(
    path: string[],
    metadata: Record<string, any>,
  ): Promise<void> {
    const metaSize = ENC.encode(JSON.stringify(metadata)).length;
    if (metaSize > 60 * 1024) {
      throw new Error("Metadata exceeds 60 KB limit");
    }
    const uri = this.pathToURIComponent(path);
    await this.db.query(
      `UPDATE surreal_fs_files SET metadata = $meta, updated_at = time::now() WHERE uri = $uri`,
      { meta: metadata, uri },
    );
  }

  /**
   * Retrieve the metadata of an existing file.
   *
   * @param path - The file path.
   * @returns The metadata object, or `undefined` if the file does not exist.
   *
   * @example
   * ```ts
   * const meta = await fs.getMetadata(["reports", "q4.pdf"]);
   * console.log(meta?.reviewer); // "Alice"
   * ```
   */
  async getMetadata(path: string[]): Promise<Record<string, any> | undefined> {
    const uri = this.pathToURIComponent(path);
    const record = await this.getFileRecord(uri);
    return record?.metadata;
  }

  // ── DELETE ─────────────────────────────────────────────────────────────

  /**
   * Delete a single file and all its chunks.
   *
   * @param options - Delete parameters including path and optional control.
   * @returns `void` on success, or a {@link FileStatus} on error / in-progress.
   *
   * @example
   * ```ts
   * await fs.delete({ path: ["users", "u1", "avatar.png"] });
   * ```
   */
  async delete(options: ReadOptions): Promise<void | FileStatus> {
    const uri = this.pathToURIComponent(options.path);
    const status = this.fileStatus(uri);
    if (status) return status;

    const controlFn = options.control ?? defaultReadControl;
    const ctrl = await controlFn(options.path, this._readConcurrencyMap);

    if (ctrl.accessAllowed === false) {
      return this.emitError(uri, options.path, "Forbidden");
    }

    const identifiers = ctrl.concurrencyIdentifiers ?? [];
    this.incrementConcurrency(identifiers, this._readConcurrencyMap);
    this._deletingFiles[uri] = 0;

    try {
      const record = await this.getFileRecord(uri);
      if (!record) {
        this.cleanupDelete(identifiers, uri);
        return;
      }

      // Mark as deleting in the database.
      await this.db.query(
        `UPDATE surreal_fs_files SET status = 'deleting', updated_at = time::now() WHERE uri = $uri`,
        { uri },
      );

      // Delete all chunks for the active upload_id.
      if (record.upload_id) {
        await this.deleteChunksTracked(record.upload_id, uri);
      }
      // Also clean up any pending upload chunks.
      if (record.pending_upload_id) {
        await this.deleteChunksByUploadId(record.pending_upload_id);
      }

      // Finally remove the file record.
      await this.db.query(`DELETE surreal_fs_files WHERE uri = $uri`, { uri });

      this.cleanupDelete(identifiers, uri);
    } catch (e: any) {
      this.cleanupDelete(identifiers, uri);
      console.error("SurrealFS delete error:", e);
      return this.emitError(uri, options.path, e.message ?? JSON.stringify(e));
    }
  }

  /**
   * Recursively delete all files under a directory path.
   *
   * @param options - The directory path and optional control.
   * @returns An array of {@link FileStatus} for files that encountered errors.
   *
   * @example
   * ```ts
   * const errors = await fs.deleteDir({ path: ["tenant-1", "old-project"] });
   * if (errors.length) console.warn("Some deletions failed:", errors);
   * ```
   */
  async deleteDir(options: ReadOptions): Promise<FileStatus[]> {
    const controlFn = options.control ?? defaultReadControl;
    const ctrl = await controlFn(options.path, this._readConcurrencyMap);

    if (ctrl.accessAllowed === false) {
      return [this.emitError(
        this.pathToURIComponent(options.path),
        options.path,
        "Forbidden",
      )];
    }

    const uri = this.pathToURIComponent(options.path);
    const prefix = uri ? uri + "/" : "";
    const errors: FileStatus[] = [];

    // Paginate through all files under this prefix.
    let cursor: string | undefined;
    while (true) {
      let sql: string;
      let params: Record<string, any>;
      if (prefix === "") {
        sql = cursor
          ? `SELECT uri, path FROM surreal_fs_files WHERE uri > $cursor ORDER BY uri LIMIT 200`
          : `SELECT uri, path FROM surreal_fs_files ORDER BY uri LIMIT 200`;
        params = cursor ? { cursor } : {};
      } else {
        sql = cursor
          ? `SELECT uri, path FROM surreal_fs_files WHERE string::starts_with(uri, $prefix) AND uri > $cursor ORDER BY uri LIMIT 200`
          : `SELECT uri, path FROM surreal_fs_files WHERE string::starts_with(uri, $prefix) ORDER BY uri LIMIT 200`;
        params = cursor ? { prefix, cursor } : { prefix };
      }

      const rows = queryRows<any>(await this.db.query(sql, params));
      if (rows.length === 0) break;

      for (const row of rows) {
        const result = await this.delete({
          ...options,
          path: row.path ?? this.URIComponentToPath(row.uri),
        });
        if (result) errors.push(result);
      }

      if (rows.length < 200) break;
      cursor = rows[rows.length - 1].uri;
    }

    return errors;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Private methods
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Retrieve a file record from the database by URI.
   * @internal
   */
  private async getFileRecord(uri: string): Promise<any | null> {
    const rows = queryRows<any>(
      await this.db.query(
        `SELECT * FROM surreal_fs_files WHERE uri = $uri LIMIT 1`,
        { uri },
      ),
    );
    return rows.length > 0 ? rows[0] : null;
  }

  /**
   * Build a lazy {@link ReadableStream} that fetches chunks from the
   * database in order, applying rate limiting via a {@link TransformStream}.
   *
   * When `skip` is greater than zero the stream starts at the byte offset
   * `skip`: the chunk containing that byte is the first to be fetched
   * (earlier chunks are skipped at the query level, not read from storage),
   * and its leading bytes are sliced off so the consumer sees exactly
   * `size − skip` bytes. Chunks are stored 1-indexed and sized at
   * {@link CHUNK_SIZE}, so the first chunk fetched has index
   * `floor(skip / CHUNK_SIZE) + 1` and the intra-chunk offset is
   * `skip mod CHUNK_SIZE`.
   *
   * @param uploadId - The upload id used to locate chunks.
   * @param chunksPerSecond - Throughput cap applied to the output stream.
   * @param concurrencyIdentifiers - Concurrency map keys to increment on
   *   first pull and decrement on close/cancel/error.
   * @param skip - Byte offset at which to start. `0` means read from the
   *   start. Must be `>= 0` (callers are expected to clamp to `[0, size]`).
   * @internal
   */
  private buildContentStream(
    uploadId: string,
    chunksPerSecond: number,
    concurrencyIdentifiers: string[],
    skip: number = 0,
  ): ReadableStream<Uint8Array> {
    const db = this.db;
    // Chunks are 1-indexed; `lastIndex` tracks the last one delivered so
    // the next query asks for chunk_index > lastIndex. To begin at chunk
    // N we seed lastIndex to N − 1.
    const startChunkIndex = skip > 0 ? Math.floor(skip / CHUNK_SIZE) + 1 : 1;
    const firstChunkOffset = skip > 0 ? skip % CHUNK_SIZE : 0;
    let lastIndex = startChunkIndex - 1;
    let needsSlice = firstChunkOffset > 0;
    let started = false;
    const self = this;

    const source = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          if (!started) {
            started = true;
            self.incrementConcurrency(
              concurrencyIdentifiers,
              self._readConcurrencyMap,
            );
          }

          // Fetch the next chunk using cursor-based access (no table scan).
          const rows = queryRows<any>(
            await db.query(
              `SELECT * FROM surreal_fs_chunks
               WHERE upload_id = $uid AND chunk_index > $last
               ORDER BY chunk_index LIMIT 1`,
              { uid: uploadId, last: lastIndex },
            ),
          );

          if (rows.length === 0) {
            self.decrementConcurrency(
              concurrencyIdentifiers,
              self._readConcurrencyMap,
            );
            controller.close();
            return;
          }

          const row = rows[0];
          lastIndex = row.chunk_index;
          let bytes = row.data instanceof Uint8Array
            ? row.data
            : new Uint8Array(row.data);

          // If the consumer asked for a mid-chunk start, slice the first
          // chunk we emit so the stream begins at the exact byte.
          if (needsSlice) {
            needsSlice = false;
            if (firstChunkOffset >= bytes.length) {
              // Degenerate case: nothing left after the offset — pull again.
              return;
            }
            bytes = bytes.subarray(firstChunkOffset);
          }

          controller.enqueue(bytes);
        } catch (e) {
          self.decrementConcurrency(
            concurrencyIdentifiers,
            self._readConcurrencyMap,
          );
          controller.error(e);
        }
      },
      cancel() {
        if (started) {
          self.decrementConcurrency(
            concurrencyIdentifiers,
            self._readConcurrencyMap,
          );
        }
      },
    });

    // Apply throughput throttling.
    return source.pipeThrough(throttleTransform(chunksPerSecond));
  }

  /**
   * Delete all chunks for a given upload ID.
   * @internal
   */
  private async deleteChunksByUploadId(uploadId: string): Promise<void> {
    await this.db.query(
      `DELETE surreal_fs_chunks WHERE upload_id = $uid`,
      { uid: uploadId },
    );
  }

  /**
   * Delete chunks for a given upload ID while tracking deletion progress
   * in the _deletingFiles map.
   * @internal
   */
  private async deleteChunksTracked(
    uploadId: string,
    uri: string,
  ): Promise<void> {
    let cursor = 0;
    while (true) {
      const rows = queryRows<any>(
        await this.db.query(
          `SELECT * FROM surreal_fs_chunks
           WHERE upload_id = $uid AND chunk_index > $cursor
           ORDER BY chunk_index LIMIT 100`,
          { uid: uploadId, cursor },
        ),
      );
      if (rows.length === 0) break;

      for (const row of rows) {
        // Delete individual chunk and track progress.
        await this.db.query(
          `DELETE surreal_fs_chunks WHERE upload_id = $uid AND chunk_index = $idx`,
          { uid: uploadId, idx: row.chunk_index },
        );
        const chunkSize = row.data
          ? (row.data instanceof Uint8Array ? row.data.length : CHUNK_SIZE)
          : CHUNK_SIZE;
        if (uri in this._deletingFiles) {
          this._deletingFiles[uri] += chunkSize;
          this.onFileProgress(this.fileStatus(uri)!);
        }
        cursor = row.chunk_index;
      }

      if (rows.length < 100) break;
    }
  }

  /**
   * Garbage-collect incomplete operations left by crashes.
   *
   * 1. Files with `pending_upload_id` → delete pending chunks, clear field.
   * 2. Files with `status = "saving"` → delete chunks and file record.
   * 3. Files with `status = "deleting"` → finish chunk deletion, remove record.
   * @internal
   */
  private async garbageCollect(): Promise<void> {
    try {
      // 1. Interrupted replacements — clean up pending chunks.
      const pending = queryRows<any>(
        await this.db.query(
          `SELECT * FROM surreal_fs_files WHERE pending_upload_id != NONE`,
        ),
      );
      for (const f of pending) {
        await this.deleteChunksByUploadId(f.pending_upload_id);
        await this.db.query(
          `UPDATE surreal_fs_files SET pending_upload_id = NONE, updated_at = time::now() WHERE uri = $uri`,
          { uri: f.uri },
        );
      }

      // 2. Incomplete new saves.
      const saving = queryRows<any>(
        await this.db.query(
          `SELECT * FROM surreal_fs_files WHERE status = 'saving'`,
        ),
      );
      for (const f of saving) {
        if (f.upload_id) await this.deleteChunksByUploadId(f.upload_id);
        await this.db.query(`DELETE surreal_fs_files WHERE uri = $uri`, {
          uri: f.uri,
        });
      }

      // 3. Incomplete deletions.
      const deleting = queryRows<any>(
        await this.db.query(
          `SELECT * FROM surreal_fs_files WHERE status = 'deleting'`,
        ),
      );
      for (const f of deleting) {
        if (f.upload_id) await this.deleteChunksByUploadId(f.upload_id);
        if (f.pending_upload_id) {
          await this.deleteChunksByUploadId(f.pending_upload_id);
        }
        await this.db.query(`DELETE surreal_fs_files WHERE uri = $uri`, {
          uri: f.uri,
        });
      }
    } catch (e) {
      console.error("SurrealFS GC error:", e);
    }
  }

  /**
   * Return the in-memory status of a file if it is currently being
   * saved or deleted. Returns `undefined` otherwise.
   * @internal
   */
  private fileStatus(uri: string): FileStatus | undefined {
    if (uri in this._savingFiles) {
      return {
        URIComponent: uri,
        path: this.URIComponentToPath(uri),
        progress: this._savingFiles[uri],
        status: "saving",
      };
    }
    if (uri in this._deletingFiles) {
      return {
        URIComponent: uri,
        path: this.URIComponentToPath(uri),
        progress: this._deletingFiles[uri],
        status: "deleting",
      };
    }
    return undefined;
  }

  /**
   * Emit an error FileStatus and fire `onFileProgress`.
   * @internal
   */
  private emitError(uri: string, path: string[], msg: string): FileStatus {
    const s: FileStatus = {
      URIComponent: uri,
      path,
      status: "error",
      progress: 0,
      msg,
    };
    this.onFileProgress(s);
    return s;
  }

  /**
   * Increment each identifier in a concurrency map.
   * @internal
   */
  private incrementConcurrency(
    ids: string[],
    map: Record<string, number | undefined>,
  ): void {
    for (const id of ids) {
      map[id] = (map[id] ?? 0) + 1;
    }
  }

  /**
   * Decrement each identifier in a concurrency map, removing it when it hits 0.
   * @internal
   */
  private decrementConcurrency(
    ids: string[],
    map: Record<string, number | undefined>,
  ): void {
    for (const id of ids) {
      if (map[id] !== undefined) {
        (map as any)[id]--;
        if ((map[id] as number) <= 0) delete map[id];
      }
    }
  }

  /**
   * Clean up in-memory state after a save operation finishes.
   * @internal
   */
  private cleanupSave(identifiers: string[], uri: string): void {
    delete this._savingFiles[uri];
    this.decrementConcurrency(identifiers, this._saveConcurrencyMap);
  }

  /**
   * Clean up in-memory state after a delete operation finishes.
   * @internal
   */
  private cleanupDelete(identifiers: string[], uri: string): void {
    delete this._deletingFiles[uri];
    this.decrementConcurrency(identifiers, this._readConcurrencyMap);
  }
}
