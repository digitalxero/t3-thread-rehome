import { randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import * as Schema from "effect/Schema";
import type { Config } from "./config.ts";
import {
  MoveError,
  Offer,
  toFailure,
  type Destination,
  type MoveErrorCode,
  type MoveFailure,
  type MovedThread,
  type Source,
} from "./moves.ts";

/** Where the host mounts this plugin's HTTP handler on every T3 server. */
export const HTTP_NAMESPACE = "/api/plugins/rehome";
/** A stream to disk has no chunk overhead, so a pull carries far more than a client can. */
export const PULL_LIMIT_BYTES = 2 * 1024 * 1024 * 1024;
/** A turn gets a minute to stop, and bundling a large repository takes a while. */
export const EXPORT_TIMEOUT_MS = 10 * 60_000;

const OfferRequest = Schema.Struct({ threadId: Schema.String });
const ExportRequest = Schema.Struct({ id: Schema.String, haves: Schema.Array(Schema.String) });
const FinishRequest = Schema.Struct({
  id: Schema.String,
  thread: Schema.Struct({ environmentLabel: Schema.String, threadId: Schema.String }),
});
const AbortRequest = Schema.Struct({ id: Schema.String });
const ErrorBody = Schema.Struct({
  error: Schema.Struct({ code: Schema.String, message: Schema.String }),
});

const STATUS: Partial<Record<MoveErrorCode, number>> = {
  invalid: 400,
  unauthorized: 401,
  "pull-disabled": 403,
  "not-found": 404,
  busy: 409,
  internal: 500,
};
const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
const failure = (error: MoveFailure) => json(STATUS[error.code] ?? 422, { error });

async function body<A>(request: Request, schema: Schema.Codec<A, unknown>): Promise<A> {
  try {
    return Schema.decodeUnknownSync(schema)(await request.json());
  } catch {
    throw new MoveError("invalid", "The request was not understood.");
  }
}

/**
 * The source's side of a pull. The host gives this route no session, so every request must
 * carry the transfer key. Without a key configured, nothing can be pulled.
 */
export function createPullServer(source: Source, config: Config) {
  return async (request: Request): Promise<Response> => {
    try {
      const presented = /^Bearer (.+)$/.exec(request.headers.get("authorization") ?? "")?.[1];
      const access = await config.accepts(presented ?? null);
      if (access === "disabled")
        throw new MoveError(
          "pull-disabled",
          "The source has no transfer key, so threads cannot be pulled from it. Create one in Settings, Extensions, Rehome on that machine.",
        );
      if (access === "wrong")
        throw new MoveError("unauthorized", "The source refused the transfer key.");
      const route = new URL(request.url).pathname.split(HTTP_NAMESPACE)[1] ?? "";
      if (request.method !== "POST") throw new MoveError("invalid", "Use POST.");
      if (route === "/offer")
        return json(200, await source.offer((await body(request, OfferRequest)).threadId));
      if (route === "/export") {
        const input = await body(request, ExportRequest);
        const prepared = await source.prepare(input.id, input.haves, PULL_LIMIT_BYTES);
        return new Response(Readable.toWeb(createReadStream(prepared.file)) as ReadableStream, {
          status: 200,
          headers: {
            "content-type": "application/octet-stream",
            "content-length": String(prepared.size),
          },
        });
      }
      if (route === "/finish") {
        const input = await body(request, FinishRequest);
        await source.finish(input.id, input.thread);
        return json(200, { archived: true });
      }
      if (route === "/abort") {
        await source.abort((await body(request, AbortRequest)).id);
        return json(200, {});
      }
      throw new MoveError("not-found", "Unknown route.");
    } catch (error) {
      return failure(toFailure(error));
    }
  };
}

/** The stable result of "pull" and "status". A type alias so it is a JSON value. */
export type PullStatus = {
  readonly ok: true;
  readonly moveId: string;
  readonly state: "running" | "done" | "failed";
  readonly step: "offer" | "export" | "import" | "finish";
  readonly thread?: MovedThread;
  readonly source?: {
    readonly environmentId: string;
    readonly environmentLabel: string;
    readonly threadId: string;
    readonly archived: boolean;
  };
  readonly warnings: readonly string[];
  readonly error?: MoveFailure;
};
interface PullJob {
  status: PullStatus;
  readonly settled: Promise<void>;
}
const JOB_LIMIT = 50;

/** The destination's side of a pull: a background job a caller starts, then waits on. */
export class Puller {
  readonly #destination: Destination;
  readonly #config: Config;
  readonly #fetch: typeof fetch;
  readonly #onSettled: () => void;
  readonly #jobs = new Map<string, PullJob>();
  constructor(
    destination: Destination,
    config: Config,
    options: { readonly fetch?: typeof fetch; readonly onSettled?: () => void } = {},
  ) {
    this.#destination = destination;
    this.#config = config;
    this.#fetch = options.fetch ?? fetch;
    this.#onSettled = options.onSettled ?? (() => {});
  }

  /** Validates the request, then returns at once; the move continues in the background. */
  async start(input: { readonly threadId: unknown; readonly source: unknown }): Promise<string> {
    if (typeof input.threadId !== "string" || input.threadId === "")
      throw new MoveError("invalid", 'Pass the source thread as "threadId".');
    const source = await this.#config.resolve(input.source);
    const moveId = randomUUID();
    const job: PullJob = {
      status: { ok: true, moveId, state: "running", step: "offer", warnings: [] },
      settled: Promise.resolve(),
    };
    Object.assign(job, {
      settled: this.#run(job, input.threadId, source)
        .catch((error: unknown) => {
          job.status = { ...job.status, state: "failed", error: toFailure(error) };
        })
        .finally(this.#onSettled),
    });
    this.#jobs.set(moveId, job);
    for (const id of this.#jobs.keys()) {
      if (this.#jobs.size <= JOB_LIMIT) break;
      if (this.#jobs.get(id)?.status.state !== "running") this.#jobs.delete(id);
    }
    return moveId;
  }

  /** The job's status now, or once it settles if that happens within `waitMs`. */
  async wait(moveId: unknown, waitMs: number): Promise<PullStatus> {
    const job = typeof moveId === "string" ? this.#jobs.get(moveId) : undefined;
    if (!job) throw new MoveError("not-found", "No move has that id on this machine.");
    if (waitMs > 0 && job.status.state === "running") {
      const timer = new AbortController();
      await Promise.race([
        job.settled,
        new Promise<void>((resolve) => {
          const timeout = setTimeout(resolve, waitMs);
          timer.signal.addEventListener("abort", () => clearTimeout(timeout));
        }),
      ]);
      timer.abort();
    }
    return job.status;
  }

  async #post(
    source: { readonly url: string; readonly key: string },
    route: string,
    payload: unknown,
    timeoutMs: number,
  ): Promise<Response> {
    const response = await this.#fetch(`${source.url}${HTTP_NAMESPACE}${route}`, {
      method: "POST",
      headers: { authorization: `Bearer ${source.key}`, "content-type": "application/json" },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(timeoutMs),
    }).catch(() => {
      throw new MoveError(
        "source",
        "The source could not be reached. Check its address and that this machine can connect to it.",
      );
    });
    if (response.ok) return response;
    const detail = await response
      .json()
      .then(Schema.decodeUnknownSync(ErrorBody))
      .catch(() => null);
    throw new MoveError(
      response.status === 401
        ? "unauthorized"
        : response.status === 403
          ? "pull-disabled"
          : "source",
      detail?.error.message ??
        `The source answered with an error (${response.status}). Check that Rehome is installed and enabled there.`,
    );
  }

  async #run(
    job: PullJob,
    threadId: string,
    source: { readonly url: string; readonly key: string },
  ): Promise<void> {
    const step = (next: PullStatus["step"]) => {
      job.status = { ...job.status, step: next };
    };
    const offer = await this.#post(source, "/offer", { threadId }, 30_000)
      .then((response) => response.json())
      .then(Schema.decodeUnknownSync(Offer))
      .catch((error: unknown) => {
        throw error instanceof MoveError
          ? error
          : new MoveError("source", "The source sent an answer this version does not understand.");
      });
    try {
      const haves = await this.#destination.accept(offer);
      step("export");
      const parcel = await this.#post(
        source,
        "/export",
        { id: offer.id, haves },
        EXPORT_TIMEOUT_MS,
      );
      if (!parcel.body) throw new MoveError("source", "The source sent no work to copy.");
      let received = 0;
      await pipeline(
        Readable.fromWeb(parcel.body as Parameters<typeof Readable.fromWeb>[0]),
        new Transform({
          transform(chunk: Buffer, _encoding, callback) {
            received += chunk.length;
            callback(
              received > PULL_LIMIT_BYTES
                ? new MoveError("too-large", "The work to copy is over the 2 GB limit.")
                : null,
              chunk,
            );
          },
        }),
        createWriteStream(this.#destination.parcelFile(offer.id)),
      );
      step("import");
      const imported = await this.#destination.import(offer.id);
      step("finish");
      const archived = await this.#post(
        source,
        "/finish",
        { id: offer.id, thread: imported.thread },
        60_000,
      ).then(
        () => true,
        () => false,
      );
      job.status = {
        ...job.status,
        state: "done",
        thread: imported.thread,
        source: { ...offer.source, threadId: offer.threadId, archived },
        warnings: archived
          ? imported.warnings
          : [
              ...imported.warnings,
              "The thread was copied, but the original could not be archived. Archive it on the source.",
            ],
      };
    } catch (error) {
      await this.#destination.discard(offer.id);
      await this.#post(source, "/abort", { id: offer.id }, 30_000).catch(() => undefined);
      throw error;
    }
  }
}
