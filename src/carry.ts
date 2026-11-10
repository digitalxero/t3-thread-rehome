import type { PluginInput, PluginNoticeResult, PluginRelayResult } from "@t3tools/plugin-sdk";
import * as Schema from "effect/Schema";
import { MoveError, Offer, toFailure, type Destination, type Source } from "./moves.ts";

/**
 * A command result holds 256 KiB of JSON. A chunk of this size, as base64 with its step
 * around it, stays under that.
 */
const CHUNK_BYTES = 160 * 1024;
/** Each chunk is two requests through the client, so a carried move is kept modest. */
export const CARRY_LIMIT_BYTES = 64 * 1024 * 1024;
/** A command is killed at 30 seconds, so a step waits less and asks to be called again. */
const STEP_WAIT_MS = 15_000;

type Step = PluginRelayResult | PluginNoticeResult;
const relay = (
  environmentId: string | null,
  command: string,
  input: PluginInput,
  status?: string,
): PluginRelayResult => ({
  $t3: "relay",
  ...(environmentId === null ? {} : { environmentId }),
  command,
  input,
  ...(status === undefined ? {} : { status }),
});
const megabytes = (bytes: number) => (bytes / (1024 * 1024)).toFixed(1);

const pending = Symbol("pending");
/** The promise's value, or `pending` if it is still working when the step must answer. */
async function within<A>(work: Promise<A>, milliseconds: number): Promise<A | typeof pending> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<typeof pending>((resolve) => {
        timeout = setTimeout(() => resolve(pending), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

const decode =
  <A>(schema: Schema.Codec<A, unknown>) =>
  (input: PluginInput): A => {
    try {
      return Schema.decodeUnknownSync(schema)(input);
    } catch {
      throw new MoveError("invalid", "This step was not understood. Start the move again.");
    }
  };
const MoveInput = decode(Schema.Struct({ threadId: Schema.String, destination: Schema.String }));
const AcceptInput = decode(Schema.Struct({ offer: Offer }));
const ExportInput = decode(
  Schema.Struct({ id: Schema.String, haves: Schema.optionalKey(Schema.Array(Schema.String)) }),
);
const SendInput = decode(Schema.Struct({ id: Schema.String, offset: Schema.Int }));
const ReceiveInput = decode(
  Schema.Struct({
    id: Schema.String,
    offset: Schema.Int,
    data: Schema.String,
    done: Schema.Boolean,
  }),
);
const IdInput = decode(Schema.Struct({ id: Schema.String }));
const FinishInput = decode(
  Schema.Struct({
    id: Schema.String,
    thread: Schema.Struct({
      environmentId: Schema.String,
      environmentLabel: Schema.String,
      threadId: Schema.String,
    }),
    warnings: Schema.Array(Schema.String),
  }),
);
const StopInput = decode(Schema.Struct({ id: Schema.String, message: Schema.String }));

/**
 * The move as a chain of short commands the client carries between the two environments,
 * so it works over any connection the client has, including when neither environment can
 * reach the other. Steps prefixed "source" run where the thread lives.
 *
 * A step never throws. A failure tells the other side to clean up, and that side's answer
 * ends the chain with the reason.
 */
export function createCarryCommands(source: Source, destination: Destination) {
  /** Where each outgoing move is headed, so later source steps know where to relay. */
  const destinations = new Map<string, string>();
  const origins = new Map<string, string>();
  const sourceFailed = async (id: string, error: unknown): Promise<Step> => {
    await source.abort(id);
    const target = destinations.get(id);
    destinations.delete(id);
    const { message } = toFailure(error);
    return target === undefined
      ? { $t3: "notice", tone: "danger", text: message }
      : relay(target, "discard", { id, message });
  };
  const destinationFailed = async (id: string, error: unknown): Promise<Step> => {
    await destination.discard(id);
    const origin = origins.get(id);
    origins.delete(id);
    const { message } = toFailure(error);
    return origin === undefined
      ? { $t3: "notice", tone: "danger", text: message }
      : relay(origin, "abort", { id, message });
  };
  const chunk = async (id: string, offset: number, size: number): Promise<Step> => {
    const data = await source.read(id, offset, CHUNK_BYTES);
    const end = offset + data.length;
    return relay(
      destinations.get(id) ?? null,
      "receive",
      { id, offset, data: data.toString("base64"), done: end >= size },
      `Copying work (${megabytes(end)} of ${megabytes(size)} MB)`,
    );
  };

  return {
    /** Source. Checks the thread, then asks the destination whether it has the project. */
    move: async (input: PluginInput): Promise<Step> => {
      try {
        const { threadId, destination: target } = MoveInput(input);
        const offer = await source.offer(threadId);
        destinations.set(offer.id, target);
        return relay(target, "accept", { offer }, "Checking the destination");
      } catch (error) {
        return { $t3: "notice", tone: "danger", text: toFailure(error).message };
      }
    },
    /** Destination. Finds the project and answers with the history it already has. */
    accept: async (input: PluginInput): Promise<Step> => {
      let offer: Offer | undefined;
      try {
        offer = AcceptInput(input).offer;
        origins.set(offer.id, offer.source.environmentId);
        const haves = await destination.accept(offer);
        return relay(
          offer.source.environmentId,
          "export",
          { id: offer.id, haves: [...haves] },
          "Stopping the turn and packing the work",
        );
      } catch (error) {
        return offer === undefined
          ? { $t3: "notice", tone: "danger", text: toFailure(error).message }
          : destinationFailed(offer.id, error);
      }
    },
    /** Source. Packs the work in the background and sends the first chunk when it is ready. */
    export: async (input: PluginInput): Promise<Step> => {
      const { id, haves } = ExportInput(input);
      try {
        const prepared = await within(
          source.prepare(id, haves ?? [], CARRY_LIMIT_BYTES),
          STEP_WAIT_MS,
        );
        return prepared === pending
          ? relay(null, "export", { id }, "Stopping the turn and packing the work")
          : await chunk(id, 0, prepared.size);
      } catch (error) {
        return sourceFailed(id, error);
      }
    },
    /** Source. Sends the chunk at `offset`. */
    send: async (input: PluginInput): Promise<Step> => {
      const { id, offset } = SendInput(input);
      try {
        const prepared = await source.prepare(id, [], CARRY_LIMIT_BYTES);
        return await chunk(id, offset, prepared.size);
      } catch (error) {
        return sourceFailed(id, error);
      }
    },
    /** Destination. Stores a chunk, then asks for the next or starts creating the thread. */
    receive: async (input: PluginInput): Promise<Step> => {
      const { id, offset, data, done } = ReceiveInput(input);
      try {
        const received = await destination.write(id, offset, Buffer.from(data, "base64"));
        if (!done) return relay(origins.get(id) ?? null, "send", { id, offset: received });
        void destination.import(id).catch(() => undefined);
        return relay(null, "import", { id }, "Creating the thread and its worktree");
      } catch (error) {
        return destinationFailed(id, error);
      }
    },
    /** Destination. Waits for the thread, then tells the source it is safe to archive. */
    import: async (input: PluginInput): Promise<Step> => {
      const { id } = IdInput(input);
      try {
        const imported = await within(destination.import(id), STEP_WAIT_MS);
        if (imported === pending)
          return relay(null, "import", { id }, "Creating the thread and its worktree");
        const origin = origins.get(id) ?? null;
        origins.delete(id);
        const { environmentId, environmentLabel, threadId } = imported.thread;
        return relay(
          origin,
          "finish",
          {
            id,
            thread: { environmentId, environmentLabel, threadId },
            warnings: [...imported.warnings],
          },
          "Archiving the original",
        );
      } catch (error) {
        return destinationFailed(id, error);
      }
    },
    /** Source. The copy exists, so the original is archived. This ends a successful move. */
    finish: async (input: PluginInput): Promise<PluginNoticeResult> => {
      const { id, thread, warnings } = FinishInput(input);
      destinations.delete(id);
      const moved = `Moved to ${thread.environmentLabel}.`;
      const open = { environmentId: thread.environmentId, threadId: thread.threadId };
      try {
        await source.finish(id, {
          environmentLabel: thread.environmentLabel,
          threadId: thread.threadId,
        });
        return {
          $t3: "notice",
          tone: warnings.length > 0 ? "warning" : "success",
          text: [`${moved} The original was archived here.`, ...warnings].join("\n\n"),
          thread: open,
        };
      } catch (error) {
        return {
          $t3: "notice",
          tone: "warning",
          text: [
            `${moved} The original could not be archived here: ${toFailure(error).message}`,
            ...warnings,
          ].join("\n\n"),
          thread: open,
        };
      }
    },
    /** Source. The destination failed; nothing here changed, so only the files are freed. */
    abort: async (input: PluginInput): Promise<PluginNoticeResult> => {
      const { id, message } = StopInput(input);
      destinations.delete(id);
      await source.abort(id);
      return { $t3: "notice", tone: "danger", text: `${message}\n\nThe thread was not moved.` };
    },
    /** Destination. The source failed; the partial copy is removed. */
    discard: async (input: PluginInput): Promise<PluginNoticeResult> => {
      const { id, message } = StopInput(input);
      origins.delete(id);
      await destination.discard(id);
      return { $t3: "notice", tone: "danger", text: `${message}\n\nThe thread was not moved.` };
    },
  };
}
