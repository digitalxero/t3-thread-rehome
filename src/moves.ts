import { randomUUID } from "node:crypto";
import { appendFile, mkdir, open, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type {
  PluginThreadExport,
  PluginThreadImport,
  PluginThreadProject,
  PluginThreadSummary,
} from "@t3tools/plugin-sdk";
import * as Schema from "effect/Schema";
import {
  GitError,
  createBundle,
  currentBranch,
  deleteRef,
  listHaves,
  receiveBundle,
  restoreBranch,
  settleWorktree,
  upstreamOf,
} from "./git.ts";
import { openParcel, writeParcel, type Manifest } from "./parcel.ts";

/** The host's thread operations as promises. A rejection's message is safe to show. */
export interface ThreadHost {
  readonly describe: (threadId: string) => Promise<PluginThreadSummary>;
  readonly export: (input: {
    readonly threadId: string;
    readonly file: string;
  }) => Promise<PluginThreadExport>;
  readonly projects: (repository: string) => Promise<readonly PluginThreadProject[]>;
  readonly import: (input: {
    readonly file: string;
    readonly projectId: string;
    readonly branch: string;
  }) => Promise<PluginThreadImport>;
  readonly archive: (input: {
    readonly threadId: string;
    readonly movedTo: { readonly environmentLabel: string; readonly threadId: string };
  }) => Promise<void>;
}

export type MoveErrorCode =
  | "invalid"
  | "not-found"
  | "archived"
  | "not-git"
  | "has-subagents"
  | "busy"
  | "no-project"
  | "too-large"
  | "git"
  | "host"
  | "unauthorized"
  | "pull-disabled"
  | "source"
  | "internal";

/** Every failure a user can see. The message is written for them. */
export class MoveError extends Error {
  readonly code: MoveErrorCode;
  constructor(code: MoveErrorCode, message: string) {
    super(message);
    this.code = code;
  }
}

/** A type alias, not an interface, so it is a JSON value a command can return. */
export type MoveFailure = {
  readonly code: MoveErrorCode;
  readonly message: string;
};
export function toFailure(error: unknown): MoveFailure {
  if (error instanceof MoveError) return { code: error.code, message: error.message };
  if (error instanceof GitError) return { code: "git", message: error.message };
  return { code: "internal", message: "The move failed unexpectedly. Nothing was changed." };
}
/** Host rejections carry text meant for the user; anything else stays private. */
const fromHost = (error: unknown) =>
  error instanceof MoveError || error instanceof GitError
    ? error
    : new MoveError("host", error instanceof Error ? error.message : "The host refused.");

export const Offer = Schema.Struct({
  id: Schema.String,
  threadId: Schema.String,
  title: Schema.String,
  repository: Schema.String,
  source: Schema.Struct({ environmentId: Schema.String, environmentLabel: Schema.String }),
});
export type Offer = typeof Offer.Type;

export interface Prepared {
  readonly manifest: Manifest;
  readonly file: string;
  readonly size: number;
}
export type MovedThread = {
  readonly environmentId: string;
  readonly environmentLabel: string;
  readonly threadId: string;
  readonly projectId: string;
  readonly directory: string;
  readonly branch: string;
  readonly title: string;
};
export interface Imported {
  readonly thread: MovedThread;
  readonly warnings: readonly string[];
}

const megabytes = (bytes: number) => Math.max(1, Math.ceil(bytes / (1024 * 1024)));
/** An abandoned move frees its files after this long. */
const TRANSFER_TTL_MS = 30 * 60_000;
const isSafeRef = (ref: string) => /^refs\/[A-Za-z0-9._/-]+$/.test(ref) && !ref.includes("..");

interface JournalEntry {
  readonly at: string;
  readonly direction: "out" | "in";
  readonly title: string;
  readonly peer: string;
  readonly outcome: string;
}
const JOURNAL_LIMIT = 30;

/** A short record for the settings page. Titles and machine names only. */
export class Journal {
  readonly #file: string;
  constructor(dataDirectory: string) {
    this.#file = join(dataDirectory, "moves.jsonl");
  }
  async record(entry: Omit<JournalEntry, "at">): Promise<void> {
    await appendFile(this.#file, `${JSON.stringify({ ...entry, at: new Date().toISOString() })}\n`)
      .then(async () => {
        const entries = await this.read();
        if (entries.length > JOURNAL_LIMIT * 2)
          await writeFile(
            this.#file,
            entries
              .slice(0, JOURNAL_LIMIT)
              .reverse()
              .map((item) => `${JSON.stringify(item)}\n`)
              .join(""),
          );
      })
      .catch(() => undefined);
  }
  /** Newest first. */
  async read(): Promise<JournalEntry[]> {
    const text = await readFile(this.#file, "utf8").catch(() => "");
    return text
      .split("\n")
      .flatMap((line) => {
        try {
          return line === "" ? [] : [JSON.parse(line) as JournalEntry];
        } catch {
          return [];
        }
      })
      .reverse();
  }
}

abstract class Transfers<State extends { readonly createdAt: number; readonly directory: string }> {
  protected readonly transfers = new Map<string, State>();
  protected readonly root: string;
  constructor(dataDirectory: string, kind: string) {
    this.root = join(dataDirectory, "transfers", kind);
  }
  protected abstract running(state: State): boolean;
  protected async allocate(id: string): Promise<string> {
    await this.sweep();
    const directory = join(this.root, id);
    await mkdir(directory, { recursive: true });
    return directory;
  }
  protected get(id: string): State {
    const state = this.transfers.get(id);
    if (!state)
      throw new MoveError("not-found", "This move is no longer in progress. Start it again.");
    return state;
  }
  protected async drop(id: string): Promise<void> {
    const state = this.transfers.get(id);
    this.transfers.delete(id);
    if (state) await rm(state.directory, { recursive: true, force: true });
  }
  /** Frees moves a closed dialog or lost connection left behind. */
  async sweep(): Promise<void> {
    const cutoff = Date.now() - TRANSFER_TTL_MS;
    for (const [id, state] of this.transfers)
      if (state.createdAt < cutoff && !this.running(state)) await this.drop(id);
  }
  /** Files from a previous process have no owner left. */
  async reset(): Promise<void> {
    await rm(this.root, { recursive: true, force: true });
  }
}

interface Outgoing {
  readonly offer: Offer;
  readonly summary: PluginThreadSummary;
  readonly directory: string;
  readonly createdAt: number;
  prepared?: Promise<Prepared>;
  settled: boolean;
}

/** The environment a thread leaves. It changes nothing durable until `finish`. */
export class Source extends Transfers<Outgoing> {
  readonly #host: ThreadHost;
  readonly #journal: Journal;
  constructor(host: ThreadHost, dataDirectory: string, journal: Journal) {
    super(dataDirectory, "out");
    this.#host = host;
    this.#journal = journal;
  }
  protected running(state: Outgoing): boolean {
    return state.prepared !== undefined && !state.settled;
  }

  /** Checks the thread can move, before anything is stopped or copied. */
  async offer(threadId: string): Promise<Offer> {
    const summary = await this.#host.describe(threadId).catch((error: unknown) => {
      throw fromHost(error);
    });
    if (summary.archived) throw new MoveError("archived", "This thread is archived.");
    if (summary.repository === null)
      throw new MoveError(
        "not-git",
        "This thread's project is not a Git repository with a remote, so it cannot be matched to a project on another machine.",
      );
    if (summary.subagents > 0)
      throw new MoveError(
        "has-subagents",
        "This thread has subagent threads, which cannot be moved yet.",
      );
    for (const [id, state] of this.transfers) {
      if (state.offer.threadId !== summary.threadId) continue;
      if (this.running(state))
        throw new MoveError("busy", "This thread is already being moved. Try again in a minute.");
      await this.drop(id);
    }
    const id = randomUUID();
    const offer: Offer = {
      id,
      threadId: summary.threadId,
      title: summary.title,
      repository: summary.repository,
      source: {
        environmentId: summary.environmentId,
        environmentLabel: summary.environmentLabel,
      },
    };
    this.transfers.set(id, {
      offer,
      summary,
      directory: await this.allocate(id),
      createdAt: Date.now(),
      settled: false,
    });
    return offer;
  }

  /** Stops the turn, snapshots the work and packs what the destination lacks. Runs once. */
  prepare(id: string, haves: readonly string[], limitBytes: number): Promise<Prepared> {
    const state = this.get(id);
    state.prepared ??= this.#prepare(state, haves, limitBytes).finally(() => {
      state.settled = true;
    });
    // A caller that went away must not turn a failed export into an unhandled rejection.
    state.prepared.catch(() => undefined);
    return state.prepared;
  }
  async #prepare(state: Outgoing, haves: readonly string[], limitBytes: number): Promise<Prepared> {
    const { summary, offer, directory } = state;
    const conversationFile = join(directory, "conversation.json");
    const exported = await this.#host
      .export({ threadId: offer.threadId, file: conversationFile })
      .catch((error: unknown) => {
        throw fromHost(error);
      });
    const { snapshot, snapshotRef, head } = exported;
    if (snapshot === null || snapshotRef === null || head === null)
      throw new MoveError("not-git", "This thread's folder is not a Git repository.");
    try {
      const branch = await currentBranch(summary.directory);
      const bundleFile = join(directory, "work.bundle");
      const bundleBytes = await createBundle({
        directory: summary.directory,
        snapshotRef,
        haves,
        file: bundleFile,
      });
      if (bundleBytes > limitBytes)
        throw new MoveError(
          "too-large",
          `The work to copy is ${megabytes(bundleBytes)} MB, over the ${megabytes(limitBytes)} MB limit for this kind of move. Push the branch first so less has to travel.`,
        );
      const manifest: Manifest = {
        id: offer.id,
        threadId: offer.threadId,
        title: offer.title,
        repository: offer.repository,
        branch,
        upstream: branch === null ? null : await upstreamOf(summary.directory, branch),
        head,
        snapshot,
        snapshotRef,
        source: offer.source,
        messages: exported.messages,
        omittedMessages: exported.omittedMessages,
      };
      const file = join(directory, "parcel");
      const size = await writeParcel({ file, manifest, conversationFile, bundleFile });
      return { manifest, file, size };
    } finally {
      // The bundle holds the objects now; the ref would only keep them alive here.
      await deleteRef(summary.directory, snapshotRef);
    }
  }

  async read(id: string, offset: number, length: number): Promise<Buffer> {
    const prepared = await this.get(id).prepared;
    if (!prepared || offset < 0 || offset > prepared.size)
      throw new MoveError("invalid", "The copy lost its place. Start the move again.");
    const handle = await open(prepared.file, "r");
    try {
      const { buffer, bytesRead } = await handle.read(Buffer.alloc(length), 0, length, offset);
      return buffer.subarray(0, bytesRead);
    } finally {
      await handle.close();
    }
  }

  /** The only step that changes the source: the copy exists, so the original is archived. */
  async finish(
    id: string,
    movedTo: { readonly environmentLabel: string; readonly threadId: string },
  ): Promise<void> {
    const { offer } = this.get(id);
    await this.#host.archive({ threadId: offer.threadId, movedTo }).catch((error: unknown) => {
      throw fromHost(error);
    });
    await this.drop(id);
    await this.#journal.record({
      direction: "out",
      title: offer.title,
      peer: movedTo.environmentLabel,
      outcome: "Moved",
    });
  }

  async abort(id: string): Promise<void> {
    const state = this.transfers.get(id);
    if (!state) return;
    // An export still running owns these files; the sweep frees them once it settles.
    if (!this.running(state)) await this.drop(id);
  }
}

interface Incoming {
  readonly offer: Offer;
  readonly project: PluginThreadProject;
  readonly directory: string;
  readonly createdAt: number;
  received: number;
  importing?: Promise<Imported>;
  settled: boolean;
}

/** The environment a thread arrives on. A failure leaves no thread, branch or ref behind. */
export class Destination extends Transfers<Incoming> {
  readonly #host: ThreadHost;
  readonly #journal: Journal;
  constructor(host: ThreadHost, dataDirectory: string, journal: Journal) {
    super(dataDirectory, "in");
    this.#host = host;
    this.#journal = journal;
  }
  protected running(state: Incoming): boolean {
    return state.importing !== undefined && !state.settled;
  }

  /** Finds the project and reports the history it already has. */
  async accept(offer: Offer): Promise<readonly string[]> {
    const projects = await this.#host.projects(offer.repository).catch((error: unknown) => {
      throw fromHost(error);
    });
    const project = projects[0];
    if (!project)
      throw new MoveError(
        "no-project",
        `This machine has no project for ${offer.repository}. Add the project here first, then move the thread.`,
      );
    await this.drop(offer.id);
    this.transfers.set(offer.id, {
      offer,
      project,
      directory: await this.allocate(offer.id),
      createdAt: Date.now(),
      received: 0,
      settled: false,
    });
    return listHaves(project.directory);
  }

  parcelFile(id: string): string {
    return join(this.get(id).directory, "parcel");
  }

  /** Appends the next chunk. Chunks arrive in order, so a gap means a lost or repeated step. */
  async write(id: string, offset: number, chunk: Buffer): Promise<number> {
    const state = this.get(id);
    if (offset !== state.received || state.importing !== undefined)
      throw new MoveError("invalid", "The copy lost its place. Start the move again.");
    await appendFile(this.parcelFile(id), chunk);
    state.received += chunk.length;
    return state.received;
  }

  /** Creates the branch, worktree and thread. Runs once; later calls await the same result. */
  import(id: string): Promise<Imported> {
    const state = this.get(id);
    state.importing ??= this.#import(state).finally(() => {
      state.settled = true;
    });
    state.importing.catch(() => undefined);
    return state.importing;
  }
  async #import(state: Incoming): Promise<Imported> {
    const { offer, project, directory } = state;
    try {
      const conversationFile = join(directory, "conversation.json");
      const bundleFile = join(directory, "work.bundle");
      const manifest = await openParcel({
        file: join(directory, "parcel"),
        conversationFile,
        bundleFile,
      }).catch(() => {
        throw new MoveError("invalid", "The copied work arrived incomplete. Start the move again.");
      });
      if (
        manifest.id !== offer.id ||
        manifest.repository !== offer.repository ||
        !isSafeRef(manifest.snapshotRef)
      )
        throw new MoveError("invalid", "The copied work does not match this move.");
      const received = await receiveBundle({
        repository: project.directory,
        file: bundleFile,
        snapshotRef: manifest.snapshotRef,
        snapshot: manifest.snapshot,
        head: manifest.head,
        branch: manifest.branch,
        fallbackBranch: `rehome/${offer.id.slice(0, 8)}`,
      });
      const imported = await this.#host
        .import({ file: conversationFile, projectId: project.projectId, branch: received.branch })
        .catch(async (error: unknown) => {
          await restoreBranch(project.directory, received.branch, received.previous);
          throw fromHost(error);
        });
      const warnings: string[] = [];
      await settleWorktree({
        directory: imported.directory,
        head: manifest.head,
        branch: received.branch,
        upstream: manifest.upstream,
      }).catch(() => {
        // The thread exists and holds everything, so this is a warning, not a failed move.
        warnings.push(
          'The uncommitted work arrived as a commit named "t3 checkpoint" at the tip of the branch. Run "git reset HEAD^" in the worktree to turn it back into changes.',
        );
      });
      if (manifest.branch !== null && manifest.branch !== received.branch)
        warnings.push(
          `The branch ${manifest.branch} already holds other work on this machine, so the thread uses ${received.branch}.`,
        );
      if (manifest.omittedMessages > 0)
        warnings.push(
          `${manifest.omittedMessages} older messages were not copied. The archived original keeps them.`,
        );
      await this.#journal.record({
        direction: "in",
        title: offer.title,
        peer: offer.source.environmentLabel,
        outcome: "Moved",
      });
      return {
        thread: {
          environmentId: imported.environmentId,
          environmentLabel: imported.environmentLabel,
          threadId: imported.threadId,
          projectId: project.projectId,
          directory: imported.directory,
          branch: received.branch,
          title: offer.title,
        },
        warnings,
      };
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  async discard(id: string): Promise<void> {
    const state = this.transfers.get(id);
    if (state && !this.running(state)) await this.drop(id);
  }
}
