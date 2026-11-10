import { createReadStream, createWriteStream } from "node:fs";
import { open, stat } from "node:fs/promises";
import { pipeline } from "node:stream/promises";
import * as Schema from "effect/Schema";

/** What a parcel carries, written by the source and trusted only after it is checked. */
export const Manifest = Schema.Struct({
  id: Schema.String,
  threadId: Schema.String,
  title: Schema.String,
  repository: Schema.String,
  branch: Schema.NullOr(Schema.String),
  upstream: Schema.NullOr(Schema.String),
  head: Schema.String,
  snapshot: Schema.String,
  snapshotRef: Schema.String,
  source: Schema.Struct({ environmentId: Schema.String, environmentLabel: Schema.String }),
  messages: Schema.Int,
  omittedMessages: Schema.Int,
});
export type Manifest = typeof Manifest.Type;

const Header = Schema.Struct({
  version: Schema.Literal(1),
  manifest: Manifest,
  conversationBytes: Schema.Int,
  bundleBytes: Schema.Int,
});
const encodeHeader = Schema.encodeSync(Schema.fromJsonString(Header));
const decodeHeader = Schema.decodeUnknownSync(Schema.fromJsonString(Header));
const MAX_HEADER_BYTES = 64 * 1024;

/**
 * One file for one move: a JSON header line, the host's conversation file, then the Git
 * bundle. Both transports carry it as plain bytes, in chunks or as one response body.
 */
export async function writeParcel(input: {
  readonly file: string;
  readonly manifest: Manifest;
  readonly conversationFile: string;
  readonly bundleFile: string;
}): Promise<number> {
  const header = encodeHeader({
    version: 1,
    manifest: input.manifest,
    conversationBytes: (await stat(input.conversationFile)).size,
    bundleBytes: (await stat(input.bundleFile)).size,
  });
  const out = createWriteStream(input.file);
  out.write(`${header}\n`);
  await pipeline(createReadStream(input.conversationFile), out, { end: false });
  await pipeline(createReadStream(input.bundleFile), out);
  return (await stat(input.file)).size;
}

export async function openParcel(input: {
  readonly file: string;
  readonly conversationFile: string;
  readonly bundleFile: string;
}): Promise<Manifest> {
  const handle = await open(input.file, "r");
  let headerBytes: number;
  let header: typeof Header.Type;
  try {
    const { buffer, bytesRead } = await handle.read(Buffer.alloc(MAX_HEADER_BYTES), 0);
    const newline = buffer.subarray(0, bytesRead).indexOf(0x0a);
    if (newline === -1) throw new Error("Parcel header missing");
    header = decodeHeader(buffer.subarray(0, newline).toString("utf8"));
    headerBytes = newline + 1;
  } finally {
    await handle.close();
  }
  const bundleStart = headerBytes + header.conversationBytes;
  if ((await stat(input.file)).size !== bundleStart + header.bundleBytes)
    throw new Error("Parcel is incomplete");
  const slice = (target: string, start: number, bytes: number) =>
    bytes === 0
      ? pipeline(async function* () {}, createWriteStream(target))
      : pipeline(
          createReadStream(input.file, { start, end: start + bytes - 1 }),
          createWriteStream(target),
        );
  await slice(input.conversationFile, headerBytes, header.conversationBytes);
  await slice(input.bundleFile, bundleStart, header.bundleBytes);
  return header.manifest;
}
