import {
  PluginError,
  type PluginContext,
  type PluginInput,
  type T3Plugin,
} from "@t3tools/plugin-sdk";
import * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import { createCarryCommands } from "./carry.ts";
import { Destination, MoveError, Source, toFailure, type ThreadHost } from "./moves.ts";
import { EXPORT_TIMEOUT_MS, Puller, createPullServer } from "./pull.ts";
import { VERSION } from "./version.ts";

const REQUIRED_CAPABILITIES = ["thread-details", "thread-portability"];
/** A command is killed at 30 seconds, so a caller waits less and asks again. */
const MAX_WAIT_MS = 20_000;

export const MOVE_CONFIRMATION = [
  "Copies this thread to another machine: its conversation, its branch with commits that were never pushed, and its uncommitted changes, including new files. The thread is then archived here. Nothing is deleted.",
  "Not copied: files Git ignores (such as .env, node_modules and build output), attachments, terminals and previews, and the provider's own session. The copy starts a new provider session that is given the conversation.",
  "A running turn is stopped first.",
].join("\n\n");

/** Runs a host call and rejects with the host's user-facing reason. */
async function hostCall<A>(effect: Effect.Effect<A, PluginError>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(effect));
  if (result._tag === "Success") return result.success;
  const { cause } = result.failure;
  throw new MoveError("host", cause instanceof Error ? cause.message : "The host refused.");
}

function threadHost(context: PluginContext): ThreadHost {
  const threads = context.threads;
  if (!threads) throw new Error("Thread portability unavailable");
  return {
    describe: (threadId) => hostCall(threads.describe(threadId)),
    export: (input) => hostCall(threads.export(input)),
    projects: (repository) => hostCall(threads.projects(repository)),
    import: (input) => hostCall(threads.import(input)),
    archive: (input) => hostCall(threads.archive(input)),
  };
}

const call = <A>(operation: string, run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new PluginError({ operation, cause }) });

export default {
  apiVersion: 1,
  id: "rehome",
  name: "Rehome",
  version: VERSION,
  description:
    "Move a started thread to another machine, with its conversation and uncommitted work.",
  activate: (context: PluginContext) =>
    Effect.gen(function* () {
      const missing = REQUIRED_CAPABILITIES.filter(
        (capability) => !context.capabilities?.includes(capability),
      );
      if (missing.length > 0 || !context.threads)
        return yield* new PluginError({
          operation: "activate",
          cause: new Error(`This T3 Code server lacks: ${missing.join(", ")}`),
        });
      const host = threadHost(context);
      const source = new Source(host, context.dataDirectory);
      const destination = new Destination(host, context.dataDirectory);
      const puller = new Puller(destination);
      yield* call("activate", async () => {
        await source.reset();
        await destination.reset();
      });

      const carry = createCarryCommands(source, destination);
      /** A carried step answers with its failure instead of throwing, so the user sees why. */
      const step = (id: keyof typeof carry, title: string) => ({
        id,
        title,
        description: `${title}. One step of a move started from a thread's details; not called directly.`,
        permission: "orchestration:operate" as const,
        agent: false,
        run: (input: PluginInput) =>
          call(id, () =>
            carry[id](input).catch((error: unknown): Schema.Json => ({
              $t3: "notice",
              tone: "danger",
              text: toFailure(error).message,
            })),
          ),
      });
      /** A headless command reports failures in its result, where its caller can read them. */
      const headless =
        (run: (input: PluginInput) => Promise<Schema.Json>) => (input: PluginInput) =>
          call("move", () =>
            run(input).catch((error: unknown): Schema.Json => ({
              ok: false,
              error: toFailure(error),
            })),
          );
      const waitFor = (input: PluginInput, fallback: number) =>
        Math.min(
          MAX_WAIT_MS,
          Math.max(0, typeof input.waitMs === "number" ? input.waitMs : fallback),
        );

      return {
        commands: [
          {
            id: "pull",
            title: "Pull a thread from another machine",
            description:
              'Moves a thread from another T3 Code machine to this one, with its conversation, unpushed commits and uncommitted changes, then archives the original. Run it on the destination. Input: { threadId, source: { url, key }, waitMs? }. "threadId" is the thread on the source. "source.url" is the source server\'s address and "source.key" is the transfer key that server was started with. Returns { ok: true, moveId, state: "running" | "done" | "failed", step, thread?, source?, warnings, error? }, or { ok: false, error: { code, message } } when the request is refused. The move continues in the background; it waits up to waitMs (default and maximum 20000) for it to finish. While state is "running", call "status" with the moveId.',
            permission: "orchestration:operate",
            agent: true,
            run: headless(async (input) => {
              const moveId = await puller.start({ threadId: input.threadId, source: input.source });
              return { ...(await puller.wait(moveId, waitFor(input, MAX_WAIT_MS))) };
            }),
          },
          {
            id: "status",
            title: "Read a pulled move's status",
            description:
              'Reads a move started by "pull" on this machine. Input: { moveId, waitMs? }. Returns the same object as "pull". Pass waitMs (up to 20000) to wait for the move to finish instead of asking again at once.',
            permission: "orchestration:read",
            agent: true,
            run: headless(async (input) => ({
              ...(await puller.wait(input.moveId, waitFor(input, 0))),
            })),
          },
          step("move", "Move a thread to another machine"),
          step("accept", "Accept an incoming thread"),
          step("export", "Pack a thread's work"),
          step("send", "Send packed work"),
          step("receive", "Receive packed work"),
          step("import", "Create the moved thread"),
          step("finish", "Archive the original thread"),
          step("abort", "Cancel an outgoing move"),
          step("discard", "Cancel an incoming move"),
        ],
        views: [
          {
            id: "thread",
            title: "Rehome",
            location: "thread-details" as const,
            permission: "orchestration:operate" as const,
            read: () =>
              Effect.succeed({
                title: "Rehome",
                blocks: [
                  {
                    id: "move",
                    kind: "status" as const,
                    actions: [
                      {
                        id: "move",
                        label: "Move thread…",
                        command: "move",
                        input: {},
                        confirm: MOVE_CONFIRMATION,
                        environment: { name: "destination", label: "Move to" },
                      },
                    ],
                  },
                ],
              }),
          },
        ],
        handleHttp: (request: Request) => call("http", () => createPullServer(source)(request)),
        httpResponseTimeoutMs: EXPORT_TIMEOUT_MS + 60_000,
      };
    }),
} satisfies T3Plugin;
