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
  /** Max 64 KB chunks written per second. Defaults to `Number.MAX_SAFE_INTEGER`. */
  chunksPerSecond?: number;
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
  /** Max 64 KB chunks read per second. Defaults to `Number.MAX_SAFE_INTEGER`. */
  chunksPerSecond?: number;
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
  chunksPerSecond: Number.MAX_SAFE_INTEGER,
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
  chunksPerSecond: Number.MAX_SAFE_INTEGER,
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
   *     chunksPerSecond: 50,
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

    const chunksPerSecond = ctrl.chunksPerSecond ?? Number.MAX_SAFE_INTEGER;
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

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        sizeBytes += value.length;
        if (sizeBytes > maxSize) {
          exceededLimit = true;
          break;
        }

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
           status             = 'complete',
           updated_at         = time::now()
         WHERE uri = $uri`,
        {
          uid: uploadId,
          path: options.path,
          size: sizeBytes,
          meta: options.metadata ?? {},
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
    };

    const identifiers = ctrl.concurrencyIdentifiers ?? [];
    const chunksPerSecond = ctrl.chunksPerSecond ?? Number.MAX_SAFE_INTEGER;

    file.content = this.buildContentStream(
      record.upload_id,
      chunksPerSecond,
      identifiers,
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
    const chunksPerSecond = ctrl.chunksPerSecond ?? Number.MAX_SAFE_INTEGER;
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
   * Build a lazy ReadableStream that fetches chunks from the database
   * in order, applying rate limiting via a TransformStream.
   * @internal
   */
  private buildContentStream(
    uploadId: string,
    chunksPerSecond: number,
    concurrencyIdentifiers: string[],
  ): ReadableStream<Uint8Array> {
    const db = this.db;
    let lastIndex = 0;
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
          const bytes = row.data instanceof Uint8Array
            ? row.data
            : new Uint8Array(row.data);
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
