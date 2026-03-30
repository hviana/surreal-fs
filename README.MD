# 🗄️ SurrealFS

**A resilient, chunked file system built on SurrealDB.**

Store, stream, and manage files of any size using SurrealDB as the backend —
with 64 KB chunked storage, configurable rate limiting, multi-factor concurrency
control, and a crash-resilient garbage collector.

---

## ✨ Highlights

| Feature                    | Description                                                                            |
| -------------------------- | -------------------------------------------------------------------------------------- |
| 📦 **Chunked storage**     | Files split into 64 KB chunks across a dedicated table                                 |
| 🔄 **Atomic replacement**  | Old file data is removed only after a new upload fully commits                         |
| 🧹 **Garbage collector**   | Incomplete uploads/deletes are cleaned up automatically on init                        |
| 🚦 **Rate limiting**       | Configurable chunks-per-second via `TransformStream` (no `delay`)                      |
| 🔒 **Concurrency control** | Multi-factor maps keyed by path sub-elements — separate maps for uploads and downloads |
| 📁 **Directory semantics** | Hierarchical path arrays with cursor-based pagination for listings                     |
| 🌐 **Web-standard APIs**   | `ReadableStream`, `TransformStream`, `TextEncoder`, `crypto.randomUUID`                |

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        SurrealFS                             │
│                                                              │
│   save()  read()  readDir()  delete()  deleteDir()           │
│     │       │        │          │          │                 │
│     ▼       ▼        ▼          ▼          ▼                 │
│  ┌──────────────────────────────────────────────┐            │
│  │            Control Callback                  │            │
│  │  • Access check                              │            │
│  │  • Rate limit (chunks/s)                     │            │
│  │  • Concurrency identifiers                   │            │
│  │  • Size / extension limits                   │            │
│  └──────────────────────────────────────────────┘            │
│                          │                                   │
│     ┌────────────────────┴────────────────────┐              │
│     │         TransformStream Pipeline        │              │
│     │   Input → Chunking (64KB) → Throttle    │              │
│     └─────────────────────────────────────────┘              │
│                          │                                   │
│  ┌───────────────────────┴───────────────────────────┐       │
│  │                   SurrealDB                       │       │
│  │                                                   │       │
│  │   surreal_fs_files          surreal_fs_chunks     │       │
│  │   ┌──────────────┐         ┌─────────────────┐    │       │
│  │   │ uri  (UNIQUE)│         │ upload_id       │    │       │
│  │   │ path[]       │────────▶│ chunk_index     │    │       │
│  │   │ size         │         │ data (base64)   │    │       │
│  │   │ metadata     │         └─────────────────┘    │       │
│  │   │ upload_id    │                                │       │
│  │   │ status       │                                │       │
│  │   └──────────────┘                                │       │
│  └───────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Installation

```ts
import { SurrealFS } from "jsr:@hviana/surreal-fs";
```

### Setup

```ts
// 1. Connect to SurrealDB
const db = new Surreal();
await db.connect("http://localhost:8000/rpc");
await db.signin({ username: "root", password: "root" });
await db.use({ namespace: "myapp", database: "production" });

// 2. Create and initialise SurrealFS
const fs = new SurrealFS(db);
await fs.init(); // Creates tables, indexes, and runs GC
```

### Save a file

```ts
const result = await fs.save({
  path: ["users", "user_42", "documents", "report.pdf"],
  content: pdfBytes, // Uint8Array, ReadableStream, or string
  metadata: { author: "Alice", version: 3 },
});

if ("size" in result) {
  console.log(`Saved ${result.size} bytes`);
}
```

### Read a file

```ts
const file = await fs.read({
  path: ["users", "user_42", "documents", "report.pdf"],
});

if (file && "content" in file && file.content) {
  // Stream directly to the client
  return new Response(file.content, {
    headers: { "Content-Type": "application/pdf" },
  });

  // — or — read into memory
  const bytes = await SurrealFS.readStream(file.content);
}
```

### Delete a file

```ts
await fs.delete({
  path: ["users", "user_42", "documents", "report.pdf"],
});
```

---

## 📚 Full API Reference

### `constructor(db: Surreal)`

Create a new SurrealFS instance. The `db` parameter must be an already-connected
SurrealDB client.

```ts
const db = new Surreal();
await db.connect("http://localhost:8000/rpc");
await db.signin({ username: "root", password: "root" });
await db.use({ namespace: "test", database: "test" });

const fs = new SurrealFS(db);
```

---

### `init(): Promise<void>`

Idempotently create the required tables and indexes, then run the garbage
collector. **Must be called once before any other method.** Safe to call on
every server startup.

```ts
await fs.init();
// Tables created (if not exist):
//   • surreal_fs_files  — file metadata
//   • surreal_fs_chunks — 64 KB data chunks
//
// Indexes created:
//   • idx_sfs_files_uri    (UNIQUE on uri)
//   • idx_sfs_files_status (on status)
//   • idx_sfs_chunks_upload (UNIQUE on upload_id + chunk_index)
//   • idx_sfs_chunks_upload_id (on upload_id)
//
// Garbage collector cleans up:
//   • Interrupted uploads (status = "saving")
//   • Interrupted replacements (pending_upload_id set)
//   • Interrupted deletions (status = "deleting")
```

---

### `save(options: SaveOptions): Promise<FileStatus | File>`

Upload a file to SurrealDB.

#### Parameters — `SaveOptions`

| Field      | Type                                              | Required | Description                            |
| ---------- | ------------------------------------------------- | -------- | -------------------------------------- |
| `path`     | `string[]`                                        | ✅       | Hierarchical path identifying the file |
| `content`  | `ReadableStream \| Uint8Array \| string`          | ✅       | File content                           |
| `metadata` | `Record<string, any>`                             | ❌       | Arbitrary metadata (max 60 KB)         |
| `control`  | `(path, saveConcurrencyMap) => SaveControlResult` | ❌       | Access & rate control callback         |

#### `SaveControlResult`

| Field                    | Type       | Default            | Description                  |
| ------------------------ | ---------- | ------------------ | ---------------------------- |
| `accessAllowed`          | `boolean`  | `true`             | Allow or deny this operation |
| `chunksPerSecond`        | `number`   | `MAX_SAFE_INTEGER` | Throughput limit             |
| `concurrencyIdentifiers` | `string[]` | `[]`               | Concurrency map keys         |
| `maxFileSizeBytes`       | `number`   | `MAX_SAFE_INTEGER` | File size cap                |
| `allowedExtensions`      | `string[]` | `[]`               | Empty = all allowed          |

#### Examples

**Basic save (string content):**

```ts
await fs.save({
  path: ["configs", "app.json"],
  content: JSON.stringify({ theme: "dark", lang: "en" }),
});
```

**Save with stream content:**

```ts
const response = await fetch("https://example.com/large-file.zip");

await fs.save({
  path: ["downloads", "archive.zip"],
  content: response.body!, // ReadableStream
  metadata: { source: "https://example.com/large-file.zip" },
});
```

**Save with full control:**

```ts
const result = await fs.save({
  path: ["company_abc", "project_x", "assets", "logo.png"],
  content: logoBytes,
  metadata: { uploadedBy: "user_42", mimeType: "image/png" },
  control: (path, concurrencyMap) => {
    const companyId = path[0]; // "company_abc"
    const projectId = `${path[0]}/${path[1]}`; // "company_abc/project_x"

    return {
      accessAllowed: userHasPermission(companyId),
      chunksPerSecond: 100, // ~6.4 MB/s
      concurrencyIdentifiers: [companyId, projectId],
      maxFileSizeBytes: 50 * 1024 * 1024, // 50 MB
      allowedExtensions: ["png", "jpg", "webp", "svg"],
    };
  },
});

if ("status" in result && result.status === "error") {
  console.error("Upload failed:", result.msg);
} else {
  console.log("Upload OK, size:", (result as any).size);
}
```

**Atomic file replacement:**

```ts
// If logo.png already exists, the old chunks are kept until the
// new upload completes, then they are removed in the background.
// If the server crashes mid-upload, the GC cleans up on next init().

await fs.save({
  path: ["company_abc", "project_x", "assets", "logo.png"],
  content: newLogoBytes,
});
```

---

### `read(options: ReadOptions): Promise<File | FileStatus | null>`

Read a single file. Returns a `File` with a lazy `ReadableStream` for the
content.

#### Parameters — `ReadOptions`

| Field     | Type                                              | Required | Description                    |
| --------- | ------------------------------------------------- | -------- | ------------------------------ |
| `path`    | `string[]`                                        | ✅       | Path to the file               |
| `control` | `(path, readConcurrencyMap) => ReadControlResult` | ❌       | Access & rate control callback |

#### `ReadControlResult`

| Field                    | Type       | Default            | Description                       |
| ------------------------ | ---------- | ------------------ | --------------------------------- |
| `accessAllowed`          | `boolean`  | `true`             | Allow or deny                     |
| `chunksPerSecond`        | `number`   | `MAX_SAFE_INTEGER` | Read throughput limit             |
| `concurrencyIdentifiers` | `string[]` | `[]`               | Concurrency map keys              |
| `maxPageSize`            | `number`   | `1000`             | Entries per page (for `readDir`)  |
| `cursor`                 | `any`      | `undefined`        | Pagination cursor (for `readDir`) |

#### Examples

**Basic read:**

```ts
const file = await fs.read({
  path: ["users", "user_42", "documents", "report.pdf"],
});

if (!file) {
  console.log("File not found");
} else if ("status" in file) {
  console.log("File is busy:", file.status, file.msg);
} else {
  console.log("Path:", file.path);
  console.log("Size:", file.size, "bytes");
  console.log("Metadata:", file.metadata);

  const bytes = await SurrealFS.readStream(file.content!);
}
```

**Read as string:**

```ts
const file = await fs.read({ path: ["configs", "app.json"] });
if (file && "content" in file && file.content) {
  const json = await SurrealFS.readStreamAsString(file.content);
  const config = JSON.parse(json);
  console.log(config.theme); // "dark"
}
```

**Rate-limited read:**

```ts
const file = await fs.read({
  path: ["videos", "presentation.mp4"],
  control: (path, map) => ({
    accessAllowed: true,
    chunksPerSecond: 32, // ~2 MB/s
    concurrencyIdentifiers: ["video-streaming"],
  }),
});
```

**Serve as HTTP response (Deno / web framework):**

```ts
async function handleDownload(req: Request): Promise<Response> {
  const file = await fs.read({
    path: ["public", "assets", "hero.jpg"],
  });

  if (!file || "status" in file) {
    return new Response("Not found", { status: 404 });
  }

  return new Response(file.content, {
    headers: {
      "Content-Type": "image/jpeg",
      "Content-Length": String(file.size),
    },
  });
}
```

---

### `readDir(options: ReadOptions): Promise<DirList>`

List all files under a directory prefix, with cursor-based pagination.

#### Return value — `DirList`

| Field    | Type                     | Description                             |
| -------- | ------------------------ | --------------------------------------- |
| `files`  | `(File \| FileStatus)[]` | Files and in-progress operations        |
| `size`   | `number`                 | Total byte size                         |
| `cursor` | `any \| undefined`       | Pass to the next call for the next page |

#### Examples

**List a directory:**

```ts
const listing = await fs.readDir({
  path: ["company_abc", "project_x"],
});

for (const entry of listing.files) {
  if ("status" in entry) {
    console.log(
      `[${entry.status}] ${entry.URIComponent} (${entry.progress} bytes)`,
    );
  } else {
    console.log(`${entry.URIComponent} — ${entry.size} bytes`);
  }
}

console.log("Total size:", listing.size, "bytes");
```

**Paginated listing:**

```ts
let cursor: any = undefined;
let page = 1;

do {
  const listing = await fs.readDir({
    path: ["company_abc"],
    control: (_path, _map) => ({
      accessAllowed: true,
      maxPageSize: 50,
      cursor, // pass the cursor from the previous page
    }),
  });

  console.log(`--- Page ${page} (${listing.files.length} files) ---`);
  for (const f of listing.files) {
    if (!("status" in f)) console.log(f.URIComponent);
  }

  cursor = listing.cursor;
  page++;
} while (cursor);
```

**List root (all files):**

```ts
const everything = await fs.readDir({ path: [] });
console.log(`Total files: ${everything.files.length}`);
```

---

### `delete(options: ReadOptions): Promise<void | FileStatus>`

Delete a single file and all its chunks.

```ts
const result = await fs.delete({
  path: ["users", "user_42", "temp", "draft.txt"],
});

if (result) {
  console.error("Delete failed:", result.msg);
}
```

---

### `deleteDir(options: ReadOptions): Promise<FileStatus[]>`

Recursively delete all files under a directory.

```ts
const errors = await fs.deleteDir({
  path: ["users", "user_42", "temp"],
});

if (errors.length > 0) {
  console.warn(`${errors.length} files could not be deleted`);
}
```

---

### `setMetadata(path: string[], metadata: Record<string, any>): Promise<void>`

Update the metadata of an existing file. Max 60 KB.

```ts
await fs.setMetadata(
  ["reports", "q4.pdf"],
  {
    reviewed: true,
    reviewer: "Bob",
    tags: ["finance", "quarterly"],
  },
);
```

---

### `getMetadata(path: string[]): Promise<Record<string, any> | undefined>`

Retrieve the metadata of an existing file.

```ts
const meta = await fs.getMetadata(["reports", "q4.pdf"]);
console.log(meta?.reviewer); // "Bob"
console.log(meta?.tags); // ["finance", "quarterly"]
```

---

### `pathToURIComponent(path: string[]): string`

Encode a path array into a URI-safe string.

```ts
fs.pathToURIComponent(["users", "café owner", "photo.jpg"]);
// → "users/caf%C3%A9%20owner/photo.jpg"
```

### `URIComponentToPath(uri: string): string[]`

Decode a URI string back into a path array.

```ts
fs.URIComponentToPath("users/caf%C3%A9%20owner/photo.jpg");
// → ["users", "café owner", "photo.jpg"]
```

---

### `getAllFilesStatuses(): FileStatus[]`

Get a snapshot of all in-progress operations.

```ts
const statuses = fs.getAllFilesStatuses();

for (const s of statuses) {
  console.log(`${s.status}: ${s.URIComponent} — ${s.progress} bytes`);
}
```

---

### `onFileProgress: (status: FileStatus) => void`

Callback fired whenever an operation makes progress or completes.

```ts
fs.onFileProgress = (status) => {
  const pct = status.progress; // bytes processed
  console.log(`[${status.status}] ${status.URIComponent}: ${pct} bytes`);

  if (status.msg) {
    console.log(`  Message: ${status.msg}`);
  }
};
```

---

### `getSaveConcurrencyMap() / getReadConcurrencyMap()`

Inspect the current concurrency counters for debugging.

```ts
console.log("Active uploads:", fs.getSaveConcurrencyMap());
// { "company_abc": 2, "company_abc/project_x": 1 }

console.log("Active downloads:", fs.getReadConcurrencyMap());
// { "video-streaming": 5 }
```

---

### Static Helpers

**`SurrealFS.readStream(stream)`** — Consume a stream into `Uint8Array`:

```ts
const bytes = await SurrealFS.readStream(file.content!);
```

**`SurrealFS.readStreamAsString(stream)`** — Consume a stream into a UTF-8
string:

```ts
const text = await SurrealFS.readStreamAsString(file.content!);
```

---

## 🔒 Concurrency Control Deep Dive

The `control` callback is the single point of policy enforcement. It receives
the file path and the current concurrency map, and returns limits.

### Multi-factor concurrency

```ts
const saveControl = (
  path: string[],
  concurrencyMap: Record<string, number | undefined>,
) => {
  const userId = path[0]; // e.g. "user_42"
  const tenantId = path[1]; // e.g. "company_abc"

  // Check current load
  const userUploads = concurrencyMap[userId] ?? 0;
  const tenantUploads = concurrencyMap[tenantId] ?? 0;

  return {
    accessAllowed: userUploads < 5 && tenantUploads < 20,
    chunksPerSecond: tenantUploads > 10 ? 25 : 100,
    concurrencyIdentifiers: [userId, tenantId],
    maxFileSizeBytes: 100 * 1024 * 1024,
    allowedExtensions: [],
  };
};

await fs.save({
  path: ["user_42", "company_abc", "data.csv"],
  content: csvStream,
  control: saveControl,
});
```

### How identifiers work

```
Operation starts:
  concurrencyMap["user_42"]      1 → 2
  concurrencyMap["company_abc"]  3 → 4

Operation ends:
  concurrencyMap["user_42"]      2 → 1
  concurrencyMap["company_abc"]  4 → 3

Last operation for "user_42" ends:
  concurrencyMap["user_42"]      1 → (deleted)
```

---

## 🧹 Garbage Collector

The GC runs automatically when you call `init()`. It handles three scenarios:

| Scenario                    | Cause                          | Action                                            |
| --------------------------- | ------------------------------ | ------------------------------------------------- |
| **Interrupted new upload**  | Server crash during first save | Delete chunks + file record                       |
| **Interrupted replacement** | Server crash during overwrite  | Delete new (pending) chunks, keep old file intact |
| **Interrupted deletion**    | Server crash during delete     | Finish deleting chunks + file record              |

```ts
// The GC is resilient: if init() crashes too, running it again
// will simply pick up where it left off. No data is lost.
const fs = new SurrealFS(db);
await fs.init(); // ← GC runs here, concurrently
```

---

## 🗃️ Database Schema

### `surreal_fs_files`

| Column              | Type             | Description                               |
| ------------------- | ---------------- | ----------------------------------------- |
| `uri`               | `string`         | URI-encoded path (UNIQUE index)           |
| `path`              | `array<string>`  | Original path array                       |
| `size`              | `int`            | Total file size in bytes                  |
| `metadata`          | `object`         | User-defined metadata                     |
| `upload_id`         | `string`         | UUID of the current active chunk set      |
| `pending_upload_id` | `option<string>` | UUID of an in-progress replacement        |
| `status`            | `string`         | `"complete"`, `"saving"`, or `"deleting"` |
| `created_at`        | `datetime`       | Row creation time                         |
| `updated_at`        | `datetime`       | Last modification time                    |

### `surreal_fs_chunks`

| Column        | Type     | Description                    |
| ------------- | -------- | ------------------------------ |
| `upload_id`   | `string` | Links to a file's upload cycle |
| `chunk_index` | `int`    | 1-based chunk ordering         |
| `data`        | `bytes`  | Raw 64 KB chunk                |

### Indexes

```sql
-- Files
DEFINE INDEX idx_sfs_files_uri    ON surreal_fs_files FIELDS uri UNIQUE;
DEFINE INDEX idx_sfs_files_status ON surreal_fs_files FIELDS status;

-- Chunks
DEFINE INDEX idx_sfs_chunks_upload    ON surreal_fs_chunks FIELDS upload_id, chunk_index UNIQUE;
DEFINE INDEX idx_sfs_chunks_upload_id ON surreal_fs_chunks FIELDS upload_id;
```

---

## 🌊 Streaming Pipeline

Uploads flow through a `TransformStream` pipeline — no `delay()` calls anywhere:

```
Input (ReadableStream | Uint8Array | string)
  │
  ▼
┌──────────────────────────┐
│  Chunking TransformStream │  ← Splits into exact 64 KB pieces
└──────────────────────────┘
  │
  ▼
┌──────────────────────────┐
│  Throttle TransformStream │  ← Enforces chunks/second limit
└──────────────────────────┘
  │
  ▼
  reader.read() loop → INSERT into surreal_fs_chunks
```

Downloads use a pull-based `ReadableStream` piped through the same throttle
transform:

```
surreal_fs_chunks (cursor-based fetch, 1 row at a time)
  │
  ▼
┌──────────────────────────┐
│  Source ReadableStream     │  ← pull() fetches next chunk from DB
└──────────────────────────┘
  │
  ▼
┌──────────────────────────┐
│  Throttle TransformStream │  ← Enforces chunks/second limit
└──────────────────────────┘
  │
  ▼
  Consumer (Response body, file writer, etc.)
```

---

## 💡 Recipes

### Multi-tenant file server

```ts
import { serve } from "https://deno.land/std/http/mod.ts";

const tenantLimits: Record<string, number> = {
  free: 10 * 1024 * 1024, // 10 MB
  pro: 500 * 1024 * 1024, // 500 MB
  enterprise: 10 * 1024 * 1024 * 1024, // 10 GB
};

const uploadControl = (
  path: string[],
  map: Record<string, number | undefined>,
) => {
  const tenantId = path[0];
  const plan = getTenantPlan(tenantId); // your lookup function

  return {
    accessAllowed: true,
    chunksPerSecond: plan === "free" ? 16 : 256,
    concurrencyIdentifiers: [tenantId],
    maxFileSizeBytes: tenantLimits[plan] ?? tenantLimits.free,
    allowedExtensions: [],
  };
};

serve(async (req) => {
  const url = new URL(req.url);

  if (req.method === "PUT") {
    const path = url.pathname.split("/").filter(Boolean);
    const result = await fs.save({
      path,
      content: req.body!,
      control: uploadControl,
    });
    return Response.json(result);
  }

  if (req.method === "GET") {
    const path = url.pathname.split("/").filter(Boolean);
    const file = await fs.read({ path });
    if (!file || "status" in file) {
      return new Response("Not found", { status: 404 });
    }
    return new Response(file.content, {
      headers: { "Content-Length": String(file.size) },
    });
  }

  return new Response("Method not allowed", { status: 405 });
});
```

### Progress monitoring

```ts
fs.onFileProgress = (status) => {
  switch (status.status) {
    case "saving":
      console.log(
        `⬆️  Uploading ${status.URIComponent}: ${status.progress} bytes`,
      );
      break;
    case "deleting":
      console.log(
        `🗑️  Deleting ${status.URIComponent}: ${status.progress} bytes freed`,
      );
      break;
    case "error":
      console.error(`❌  Error on ${status.URIComponent}: ${status.msg}`);
      break;
  }
};
```

### Backup all files in a directory

```ts
async function backupDir(srcPath: string[], destPath: string[]) {
  const listing = await fs.readDir({ path: srcPath });

  for (const entry of listing.files) {
    if ("status" in entry) continue; // skip in-progress files

    const bytes = await SurrealFS.readStream(entry.content!);
    const relativePath = entry.path.slice(srcPath.length);

    await fs.save({
      path: [...destPath, ...relativePath],
      content: bytes,
      metadata: { ...entry.metadata, backedUpAt: new Date().toISOString() },
    });
  }
}

await backupDir(
  ["company_abc", "production"],
  ["company_abc", "backups", "2025-01-15"],
);
```

---

## 📄 License

MIT — Created by [Henrique Emanoel Viana](https://github.com/hviana)
