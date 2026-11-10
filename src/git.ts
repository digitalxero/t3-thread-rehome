import { spawn } from "node:child_process";
import { stat } from "node:fs/promises";

/** A Git step failed. The message names the step, never raw command output. */
export class GitError extends Error {
  readonly detail: string;
  constructor(step: string, detail: string) {
    super(`Git could not ${step}.`);
    this.detail = detail;
  }
}

interface GitOptions {
  readonly stdin?: string;
  readonly timeoutMs?: number;
  readonly allowFailure?: boolean;
}
interface GitResult {
  readonly code: number;
  readonly stdout: string;
}

const MAX_OUTPUT_BYTES = 8 * 1024 * 1024;

/**
 * The host's supervised runner keeps 8 KiB of output and has no stdin, which ref lists and
 * `git bundle --stdin` both need, so Git runs here with its own deadline and output bound.
 */
function git(
  step: string,
  cwd: string,
  args: readonly string[],
  options: GitOptions = {},
): Promise<GitResult> {
  return new Promise((resolve, reject) => {
    const child = spawn("git", args, {
      cwd,
      env: { ...process.env, GIT_TERMINAL_PROMPT: "0", LC_ALL: "C" },
      stdio: ["pipe", "pipe", "pipe"],
      signal: AbortSignal.timeout(options.timeoutMs ?? 120_000),
    });
    const stdout: Buffer[] = [];
    let stdoutBytes = 0;
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdoutBytes += chunk.length;
      if (stdoutBytes <= MAX_OUTPUT_BYTES) stdout.push(chunk);
    });
    child.stderr.on("data", (chunk: Buffer) => {
      if (stderr.length < 4096) stderr += chunk.toString("utf8");
    });
    child.on("error", (error) => reject(new GitError(step, error.message)));
    child.on("close", (code) => {
      if (stdoutBytes > MAX_OUTPUT_BYTES) reject(new GitError(step, "output too large"));
      else if (code !== 0 && !options.allowFailure) reject(new GitError(step, stderr.trim()));
      else resolve({ code: code ?? 1, stdout: Buffer.concat(stdout).toString("utf8") });
    });
    child.stdin.on("error", () => {});
    child.stdin.end(options.stdin ?? "");
  });
}

const lines = (text: string) =>
  text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "");
const isSha = (value: string) => /^[0-9a-f]{40,64}$/.test(value);

/** Enough tips to find common history in a busy repository, and small enough to send. */
const MAX_HAVES = 2_000;

/**
 * Commits this repository already has, newest first. The source leaves them out of the
 * bundle, so only work the destination lacks travels: unpushed commits and the snapshot.
 */
export async function listHaves(repository: string): Promise<string[]> {
  // Best effort: a fresh fetch shrinks the bundle, but an offline remote must not block a move.
  await git("fetch the remote", repository, ["fetch", "--quiet", "--all", "--no-tags"], {
    allowFailure: true,
    timeoutMs: 30_000,
  }).catch(() => undefined);
  const refs = await git("list branches", repository, [
    "for-each-ref",
    "--sort=-committerdate",
    `--count=${MAX_HAVES}`,
    "--format=%(objectname)",
    "refs/heads",
    "refs/remotes",
  ]);
  return [...new Set(lines(refs.stdout).filter(isSha))];
}

/** The branch a directory has checked out, or null when HEAD is detached. */
export async function currentBranch(directory: string): Promise<string | null> {
  const result = await git(
    "read the branch",
    directory,
    ["symbolic-ref", "--short", "-q", "HEAD"],
    {
      allowFailure: true,
    },
  );
  return result.code === 0 ? result.stdout.trim() || null : null;
}

/** The remote branch a local branch tracks, such as `origin/feature`. */
export async function upstreamOf(directory: string, branch: string): Promise<string | null> {
  const result = await git(
    "read the upstream",
    directory,
    ["rev-parse", "--abbrev-ref", "--symbolic-full-name", `${branch}@{upstream}`],
    { allowFailure: true },
  );
  return result.code === 0 ? result.stdout.trim() || null : null;
}

/**
 * Writes everything reachable from `snapshotRef` that the destination does not have. The
 * snapshot's parent is the branch tip, so committed and uncommitted work travel together.
 */
export async function createBundle(input: {
  readonly directory: string;
  readonly snapshotRef: string;
  readonly haves: readonly string[];
  readonly file: string;
}): Promise<number> {
  const candidates = input.haves.filter(isSha);
  const known = await git("compare history", input.directory, ["cat-file", "--batch-check"], {
    stdin: candidates.map((sha) => `${sha}\n`).join(""),
  });
  const shared = lines(known.stdout)
    .map((line) => line.split(" "))
    .filter(([, type]) => type === "commit")
    .map(([sha]) => `^${sha}`);
  await git("bundle the work", input.directory, ["bundle", "create", input.file, "--stdin"], {
    stdin: [input.snapshotRef, ...shared].map((line) => `${line}\n`).join(""),
    timeoutMs: 600_000,
  });
  return (await stat(input.file)).size;
}

export async function deleteRef(directory: string, ref: string): Promise<void> {
  await git("remove a temporary ref", directory, ["update-ref", "-d", ref], {
    allowFailure: true,
  });
}

async function branchExists(repository: string, branch: string): Promise<boolean> {
  const result = await git(
    "look up a branch",
    repository,
    ["rev-parse", "--verify", "--quiet", `refs/heads/${branch}^{commit}`],
    { allowFailure: true },
  );
  return result.code === 0;
}

async function checkedOutBranches(repository: string): Promise<Set<string>> {
  const result = await git("list worktrees", repository, ["worktree", "list", "--porcelain"]);
  return new Set(
    lines(result.stdout)
      .filter((line) => line.startsWith("branch refs/heads/"))
      .map((line) => line.slice("branch refs/heads/".length)),
  );
}

/**
 * Picks the destination branch. The source's name is kept when it is free, or when the
 * local branch is behind the moved work and no worktree has it checked out. Anything else
 * would discard or hide someone's commits, so the work lands beside it under a new name.
 */
async function chooseBranch(input: {
  readonly repository: string;
  readonly wanted: string | null;
  readonly head: string;
  readonly fallback: string;
}): Promise<string> {
  const wanted = input.wanted ?? input.fallback;
  const valid = await git("check the branch name", input.repository, [
    "check-ref-format",
    "--branch",
    wanted,
  ]).then(
    () => !wanted.startsWith("-"),
    () => false,
  );
  const base = valid ? wanted : input.fallback;
  if (!(await branchExists(input.repository, base))) return base;
  const behind = await git(
    "compare the branch",
    input.repository,
    ["merge-base", "--is-ancestor", `refs/heads/${base}`, input.head],
    { allowFailure: true },
  );
  if (behind.code === 0 && !(await checkedOutBranches(input.repository)).has(base)) return base;
  for (let suffix = 1; suffix < 100; suffix++) {
    const candidate = suffix === 1 ? `${base}-moved` : `${base}-moved-${suffix}`;
    if (!(await branchExists(input.repository, candidate))) return candidate;
  }
  throw new GitError("find a free branch name", base);
}

/**
 * Fetches a bundle and points a local branch at its snapshot commit, so the worktree the
 * host creates from that branch starts with every file in place. Call `settleWorktree` next.
 */
export async function receiveBundle(input: {
  readonly repository: string;
  readonly file: string;
  readonly snapshotRef: string;
  readonly snapshot: string;
  readonly head: string;
  readonly branch: string | null;
  readonly fallbackBranch: string;
}): Promise<{ readonly branch: string; readonly previous: string | null }> {
  if (!isSha(input.snapshot) || !isSha(input.head)) throw new GitError("read the bundle", "ids");
  const holding = `refs/t3-rehome/${input.snapshot}`;
  await git("verify the bundle", input.repository, ["bundle", "verify", "--quiet", input.file]);
  await git(
    "unpack the bundle",
    input.repository,
    ["fetch", "--quiet", "--no-tags", input.file, `+${input.snapshotRef}:${holding}`],
    { timeoutMs: 600_000 },
  );
  try {
    const fetched = await git("read the bundle", input.repository, [
      "rev-parse",
      "--verify",
      `${holding}^{commit}`,
    ]);
    const parent = await git("read the bundle", input.repository, [
      "rev-parse",
      "--verify",
      `${holding}^^{commit}`,
    ]);
    if (fetched.stdout.trim() !== input.snapshot || parent.stdout.trim() !== input.head)
      throw new GitError("match the bundle to its description", "commit ids differ");
    const branch = await chooseBranch({
      repository: input.repository,
      wanted: input.branch,
      head: input.head,
      fallback: input.fallbackBranch,
    });
    const before = await git(
      "look up a branch",
      input.repository,
      ["rev-parse", "--verify", "--quiet", `refs/heads/${branch}^{commit}`],
      { allowFailure: true },
    );
    await git("create the branch", input.repository, [
      "update-ref",
      `refs/heads/${branch}`,
      input.snapshot,
    ]);
    return { branch, previous: before.code === 0 ? before.stdout.trim() : null };
  } finally {
    await deleteRef(input.repository, holding);
  }
}

/** Puts a branch back the way `receiveBundle` found it, after a failed import. */
export async function restoreBranch(
  repository: string,
  branch: string,
  previous: string | null,
): Promise<void> {
  await git(
    "restore the branch",
    repository,
    previous === null
      ? ["update-ref", "-d", `refs/heads/${branch}`]
      : ["update-ref", `refs/heads/${branch}`, previous],
    { allowFailure: true },
  );
}

/**
 * Turns the checked-out snapshot commit back into uncommitted work: the branch returns to
 * its real tip and the files stay, so changes show as modified and new files as untracked.
 */
export async function settleWorktree(input: {
  readonly directory: string;
  readonly head: string;
  readonly branch: string;
  readonly upstream: string | null;
}): Promise<void> {
  await git("restore the uncommitted work", input.directory, ["reset", "--quiet", input.head]);
  if (input.upstream === null) return;
  const remote = await git(
    "look up the upstream",
    input.directory,
    ["rev-parse", "--verify", "--quiet", `refs/remotes/${input.upstream}^{commit}`],
    { allowFailure: true },
  );
  if (remote.code === 0)
    await git(
      "set the upstream",
      input.directory,
      ["branch", "--quiet", `--set-upstream-to=${input.upstream}`, input.branch],
      { allowFailure: true },
    );
}
