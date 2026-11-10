import {
  PluginError,
  type PluginContext,
  type PluginInput,
  type PluginView,
  type T3Plugin,
} from "@t3tools/plugin-sdk";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import type * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { createCarryCommands } from "./carry.ts";
import { Config, TRANSFER_KEY_VARIABLE, type Secrets } from "./config.ts";
import { Destination, Journal, MoveError, Source, toFailure, type ThreadHost } from "./moves.ts";
import { EXPORT_TIMEOUT_MS, Puller, createPullServer } from "./pull.ts";
import { VERSION } from "./version.ts";

const REQUIRED_CAPABILITIES = ["thread-details", "thread-portability"];
/** A command is killed at 30 seconds, so a caller waits less and asks again. */
const MAX_WAIT_MS = 20_000;

const WHAT_TRAVELS =
  "Copies this thread to another machine: its conversation, its branch with commits that were never pushed, and its uncommitted changes, including new files. The thread is then archived here. Nothing is deleted.";
const WHAT_STAYS =
  "Not copied: files Git ignores (such as .env, node_modules and build output), attachments, terminals and previews, and the provider's own session. The copy starts a new provider session that is given the conversation.";
export const MOVE_CONFIRMATION = `${WHAT_TRAVELS}\n\n${WHAT_STAYS}\n\nA running turn is stopped first.`;

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
const secretStore = (context: PluginContext): Secrets => ({
  get: (key) => hostCall(context.secrets.get(key)),
  set: (key, value) => hostCall(context.secrets.set(key, value)),
  remove: (key) => hostCall(context.secrets.remove(key)),
});

const call = <A>(operation: string, run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new PluginError({ operation, cause }) });
const text = (value: unknown) => (typeof value === "string" ? value : "");

async function settingsView(config: Config, journal: Journal): Promise<PluginView> {
  const key = await config.transferKey();
  const sources = await config.sources();
  const moves = await journal.read();
  return {
    title: "Rehome",
    blocks: [
      {
        id: "intro",
        kind: "text",
        text: `Move a thread to another machine from its details, next to the machine name. A move copies the conversation, the branch with commits that were never pushed, and the uncommitted changes, including new files. The original is then archived. Nothing is deleted.\n\n${WHAT_STAYS}\n\nBoth machines need Rehome and a project for the same repository. Threads with subagent threads cannot be moved yet.`,
      },
      {
        id: "pull",
        kind: "text",
        text: `Pulling moves a thread with no app in the middle: this machine connects to the source and copies the thread here. Use it when the source cannot reach this machine, such as a cloud sandbox. The source needs a transfer key, and this machine needs the source's address and that key.\n\nTransfer key on this machine: ${
          key === null
            ? "none. Other machines cannot pull threads from here."
            : key.from === "environment"
              ? `set by ${TRANSFER_KEY_VARIABLE}.`
              : "set. Anyone holding it can copy threads from this machine."
        }`,
      },
      {
        id: "key",
        kind: "actions",
        actions: [
          { id: "create", label: "Create a new transfer key", command: "create-key", input: {} },
          { id: "remove", label: "Remove the transfer key", command: "remove-key", input: {} },
        ],
      },
      {
        id: "sources",
        kind: "table",
        columns: ["Source", "Address"],
        rows: sources.map((source) => ({
          id: source.name,
          cells: [source.name, source.url],
          actions: [
            {
              id: "remove",
              label: "Remove",
              command: "remove-source",
              input: { name: source.name },
            },
          ],
        })),
      },
      {
        id: "add-source",
        kind: "form",
        label: "Save a source to pull from",
        command: "save-source",
        input: {},
        fields: [
          { name: "name", label: "Name, such as sandbox", kind: "text", required: true },
          {
            name: "url",
            label: "Address of its T3 server, such as http://host:3773",
            kind: "text",
            required: true,
          },
          { name: "key", label: "Its transfer key", kind: "password", required: true },
        ],
      },
      {
        id: "moves",
        kind: "table",
        columns: ["When", "Thread", "Direction", "Result"],
        rows: moves.map((move, index) => ({
          id: `${move.at}-${index}`,
          cells: [
            move.at,
            move.title,
            move.direction === "in" ? `From ${move.peer}` : `To ${move.peer}`,
            move.outcome,
          ],
          actions: [],
        })),
      },
    ],
  };
}

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
      const changes = yield* PubSub.unbounded<void>();
      const journal = new Journal(context.dataDirectory);
      const host = threadHost(context);
      const config = new Config(context.dataDirectory, secretStore(context));
      const source = new Source(host, context.dataDirectory, journal);
      const destination = new Destination(host, context.dataDirectory, journal);
      const puller = new Puller(destination, config, {
        onSettled: () => void Effect.runPromise(PubSub.publish(changes, undefined)),
      });
      yield* call("activate", async () => {
        await source.reset();
        await destination.reset();
      });
      yield* Effect.addFinalizer(() => PubSub.shutdown(changes));

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
      const waitFor = (input: PluginInput) =>
        Math.min(MAX_WAIT_MS, Math.max(0, typeof input.waitMs === "number" ? input.waitMs : 0));
      const setting = (
        id: string,
        title: string,
        run: (input: PluginInput) => Promise<Schema.Json>,
      ) => ({
        id,
        title,
        description: title,
        permission: "environment:maintain" as const,
        agent: false,
        run: (input: PluginInput) => call(id, () => run(input)),
      });

      return {
        commands: [
          {
            id: "pull",
            title: "Pull a thread from another machine",
            description:
              'Moves a thread from another T3 Code machine to this one, with its conversation, unpushed commits and uncommitted changes, then archives the original. Run it on the destination. Input: { threadId, source, waitMs? }. "threadId" is the thread on the source. "source" is the name of a source saved in Rehome\'s settings, or { url, key } with the source server\'s address and transfer key. Returns { ok: true, moveId, state: "running" | "done" | "failed", step, thread?, source?, warnings, error? }, or { ok: false, error: { code, message } } when the request is refused. The move continues in the background; it waits up to waitMs (default and maximum 20000) for it to finish. While state is "running", call "status" with the moveId.',
            permission: "orchestration:operate",
            agent: true,
            run: headless(async (input) => {
              const moveId = await puller.start({ threadId: input.threadId, source: input.source });
              return {
                ...(await puller.wait(
                  moveId,
                  input.waitMs === undefined ? MAX_WAIT_MS : waitFor(input),
                )),
              };
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
              ...(await puller.wait(input.moveId, waitFor(input))),
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
          {
            ...setting("create-key", "Create a new transfer key", async () => ({
              $t3: "secret",
              label: "Transfer key",
              value: await config.createKey(),
            })),
          },
          setting("remove-key", "Remove the transfer key", async () => {
            await config.removeKey();
            return null;
          }),
          setting("save-source", "Save a source to pull from", async (input) => {
            await config.saveSource({
              name: text(input.name),
              url: text(input.url),
              key: text(input.key),
            });
            return null;
          }),
          setting("remove-source", "Remove a saved source", async (input) => {
            await config.removeSource(text(input.name));
            return null;
          }),
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
          {
            id: "settings",
            title: "Rehome",
            location: "settings-page" as const,
            permission: "environment:maintain" as const,
            read: () => call("settings", () => settingsView(config, journal)),
          },
        ],
        handleHttp: (request: Request) =>
          call("http", () => createPullServer(source, config)(request)),
        httpResponseTimeoutMs: EXPORT_TIMEOUT_MS + 60_000,
        changes: Stream.fromPubSub(changes),
      };
    }),
} satisfies T3Plugin;
