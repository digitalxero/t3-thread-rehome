import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TestContext } from "node:test";
import { promisify } from "node:util";
import type { ThreadHost } from "../src/moves.ts";

const exec = promisify(execFile);
const environment = {
  ...process.env,
  GIT_AUTHOR_NAME: "Plugin Test",
  GIT_AUTHOR_EMAIL: "test@example.invalid",
  GIT_COMMITTER_NAME: "Plugin Test",
  GIT_COMMITTER_EMAIL: "test@example.invalid",
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_SYSTEM: "/dev/null",
};

export async function git(cwd: string, ...args: string[]): Promise<string> {
  const { stdout } = await exec("git", args, {
    cwd,
    env: environment,
    maxBuffer: 64 * 1024 * 1024,
  });
  // Only the end: `git status` lines begin with a meaningful space.
  return stdout.trimEnd();
}

export const REPOSITORY = "github.com/example/parser";

/** A remote with one pushed commit, and a clone of it for each of two machines. */
export async function repositories(t: TestContext) {
  const temp = await realpath(await mkdtemp(join(tmpdir(), "rehome-")));
  t.after(() => rm(temp, { recursive: true, force: true }));
  const remote = join(temp, "remote.git");
  await exec("git", ["init", "--bare", "-b", "main", remote], { env: environment });
  const clone = async (name: string) => {
    const directory = join(temp, name, "parser");
    await mkdir(join(temp, name), { recursive: true });
    await exec("git", ["clone", "--quiet", remote, directory], { env: environment });
    return directory;
  };
  const laptop = await clone("laptop");
  await writeFile(join(laptop, ".gitignore"), ".env\nnode_modules/\n");
  await writeFile(join(laptop, "parser.ts"), "export const parse = () => 1;\n");
  await writeFile(join(laptop, "legacy.ts"), "export const legacy = true;\n");
  await git(laptop, "add", ".");
  await git(laptop, "commit", "--quiet", "-m", "feat: add the parser");
  await git(laptop, "push", "--quiet", "origin", "main");
  const server = await clone("server");
  return { temp, remote, laptop, server };
}

/** Work a thread leaves behind: an unpushed commit and every kind of uncommitted change. */
export async function dirtyFeatureBranch(directory: string, extraBytes = 0) {
  await git(directory, "switch", "--quiet", "-c", "feature/fast-parser");
  await writeFile(join(directory, "fast.ts"), "export const fast = true;\n");
  await git(directory, "add", "fast.ts");
  await git(directory, "commit", "--quiet", "-m", "feat: add the fast path");
  await writeFile(join(directory, "parser.ts"), "export const parse = () => 2;\n");
  await writeFile(join(directory, "staged.ts"), "export const staged = true;\n");
  await git(directory, "add", "staged.ts");
  await writeFile(join(directory, "notes.md"), "untracked notes\n");
  await rm(join(directory, "legacy.ts"));
  await writeFile(join(directory, ".env"), "TOKEN=secret\n");
  if (extraBytes > 0) {
    // Incompressible, so the bundle really is this large and travels in several chunks.
    const { randomBytes } = await import("node:crypto");
    await writeFile(join(directory, "fixture.bin"), randomBytes(extraBytes));
  }
  return git(directory, "rev-parse", "HEAD");
}

/** What the host's checkpoint does: a commit of the working tree whose parent is HEAD. */
export async function snapshot(directory: string): Promise<{
  snapshot: string;
  snapshotRef: string;
  head: string;
}> {
  const index = join(await mkdtemp(join(tmpdir(), "rehome-index-")), "index");
  const withIndex = (...args: string[]) =>
    exec("git", args, { cwd: directory, env: { ...environment, GIT_INDEX_FILE: index } }).then(
      ({ stdout }) => stdout.trim(),
    );
  const head = await git(directory, "rev-parse", "HEAD");
  await withIndex("read-tree", "HEAD");
  await withIndex("add", "-A", "--", ".");
  const tree = await withIndex("write-tree");
  const commit = await git(directory, "commit-tree", tree, "-p", head, "-m", "t3 checkpoint");
  const snapshotRef = `refs/t3/plugin-exports/${randomUUID()}`;
  await git(directory, "update-ref", snapshotRef, commit);
  return { snapshot: commit, snapshotRef, head };
}

export interface FakeThread {
  title: string;
  directory: string;
  conversation: string;
  running: boolean;
  subagents: number;
  archived: boolean;
  movedTo: { environmentLabel: string; threadId: string } | null;
}

/** One machine's T3 host: its threads, its clone of the repository, and what was called. */
export function fakeHost(options: {
  readonly label: string;
  readonly repository: string;
  /** Null: this machine has no project for the repository. */
  readonly project: string | null;
  readonly worktrees: string;
}) {
  const threads = new Map<string, FakeThread>();
  const calls: string[] = [];
  const failures = {
    import: null as string | null,
    archive: null as string | null,
    /** The repository is not a project on this machine. */
    noProject: false,
  };
  const environmentId = `environment-${options.label}`;
  const require = (threadId: string) => {
    const thread = threads.get(threadId);
    if (!thread) throw new Error("This thread does not exist on this environment.");
    return thread;
  };
  const host: ThreadHost = {
    describe: async (threadId) => {
      const thread = require(threadId);
      return {
        environmentId,
        environmentLabel: options.label,
        threadId,
        title: thread.title,
        projectId: "project",
        repository: options.repository,
        directory: thread.directory,
        branch: null,
        running: thread.running,
        subagents: thread.subagents,
        archived: thread.archived,
      };
    },
    export: async ({ threadId, file }) => {
      calls.push("export");
      const thread = require(threadId);
      thread.running = false;
      await writeFile(file, thread.conversation);
      return { ...(await snapshot(thread.directory)), messages: 2, omittedMessages: 0 };
    },
    projects: async (repository) =>
      options.project !== null && !failures.noProject && repository === options.repository
        ? [{ projectId: "project", title: "Parser", directory: options.project }]
        : [],
    import: async ({ file, branch }) => {
      calls.push("import");
      if (failures.import !== null) throw new Error(failures.import);
      if (options.project === null) throw new Error("no project");
      const directory = join(options.worktrees, branch.replaceAll("/", "-"));
      await mkdir(options.worktrees, { recursive: true });
      await git(options.project, "worktree", "add", "--quiet", directory, branch);
      const threadId = randomUUID();
      threads.set(threadId, {
        title: "Fix the parser",
        directory,
        conversation: await readFile(file, "utf8"),
        running: false,
        subagents: 0,
        archived: false,
        movedTo: null,
      });
      return { environmentId, environmentLabel: options.label, threadId, directory };
    },
    archive: async ({ threadId, movedTo }) => {
      calls.push("archive");
      if (failures.archive !== null) throw new Error(failures.archive);
      Object.assign(require(threadId), { archived: true, movedTo });
    },
  };
  return { host, threads, calls, failures, environmentId, label: options.label };
}

export const CONVERSATION = JSON.stringify({
  messages: [
    { role: "user", text: "Make the parser faster." },
    { role: "assistant", text: "Added a fast path in fast.ts." },
  ],
});

/** A laptop with a thread to move and a server to move it to, sharing one remote. */
export async function twoMachines(t: TestContext, options: { extraBytes?: number } = {}) {
  const repos = await repositories(t);
  const laptop = fakeHost({
    label: "laptop",
    repository: REPOSITORY,
    project: repos.laptop,
    worktrees: join(repos.temp, "laptop", "worktrees"),
  });
  const server = fakeHost({
    label: "server",
    repository: REPOSITORY,
    project: repos.server,
    worktrees: join(repos.temp, "server", "worktrees"),
  });
  const head = await dirtyFeatureBranch(repos.laptop, options.extraBytes);
  laptop.threads.set("thread-1", {
    title: "Fix the parser",
    directory: repos.laptop,
    conversation: CONVERSATION,
    running: true,
    subagents: 0,
    archived: false,
    movedTo: null,
  });
  return { repos, laptop, server, head, data: (name: string) => join(repos.temp, name, "data") };
}

/** Refs a move may create and must remove, on either machine. */
export const temporaryRefs = (directory: string) =>
  git(directory, "for-each-ref", "--format=%(refname)", "refs/t3", "refs/t3-rehome");
