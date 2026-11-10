import assert from "node:assert/strict";
import { mkdir, readdir, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import type { PluginInput } from "@t3tools/plugin-sdk";
import { createCarryCommands } from "../src/carry.ts";
import { Config, TRANSFER_KEY_VARIABLE } from "../src/config.ts";
import { createBundle, listHaves, receiveBundle, settleWorktree } from "../src/git.ts";
import { Destination, Journal, Source } from "../src/moves.ts";
import { Puller, createPullServer, type PullStatus } from "../src/pull.ts";
import {
  CONVERSATION,
  dirtyFeatureBranch,
  git,
  repositories,
  snapshot,
  temporaryRefs,
  twoMachines,
  type fakeHost,
} from "./fixtures.ts";

/** The uncommitted state the laptop had, as the server must show it after a move. */
const MOVED_STATUS = [" D legacy.ts", " M parser.ts", "?? notes.md", "?? staged.ts"];
/** Sorted, with each line's leading status columns intact. */
const status = async (directory: string) =>
  (await git(directory, "status", "--porcelain=v1", "-z")).split("\0").filter(Boolean).sort();

async function machine(t: TestContext, host: ReturnType<typeof fakeHost>, data: string) {
  await mkdir(data, { recursive: true });
  const journal = new Journal(data);
  const source = new Source(host.host, data, journal);
  const destination = new Destination(host.host, data, journal);
  return { ...host, data, source, destination, carry: createCarryCommands(source, destination) };
}

/** What the T3 client does with relay results: carry each step to the machine it names. */
async function carryMove(
  machines: Record<string, Awaited<ReturnType<typeof machine>>>,
  start: { environmentId: string; command: string; input: PluginInput },
) {
  const steps: string[] = [];
  let step = start;
  for (let count = 0; count < 500; count++) {
    const target = Object.values(machines).find((m) => m.environmentId === step.environmentId);
    assert.ok(target, `unknown environment ${step.environmentId}`);
    steps.push(`${target.label}:${step.command}`);
    const result = await target.carry[step.command as keyof typeof target.carry](step.input);
    // The host refuses a result over 256 KiB, so a chunk must leave room for its step.
    assert.ok(Buffer.byteLength(JSON.stringify(result)) < 256 * 1024);
    if (result.$t3 === "notice") return { notice: result, steps };
    step = {
      environmentId: result.environmentId ?? step.environmentId,
      command: result.command,
      input: result.input,
    };
  }
  throw new Error("The move did not finish");
}

test("a bundle carries unpushed commits and every uncommitted change, and nothing Git ignores", async (t) => {
  const repos = await repositories(t);
  const head = await dirtyFeatureBranch(repos.laptop);
  const taken = await snapshot(repos.laptop);
  const bundle = join(repos.temp, "work.bundle");
  await createBundle({
    directory: repos.laptop,
    snapshotRef: taken.snapshotRef,
    haves: await listHaves(repos.server),
    file: bundle,
  });
  // Only the two new commits travel; the shared history is a prerequisite, not a passenger.
  assert.match(await git(repos.server, "bundle", "verify", bundle), /requires this ref/);
  await git(repos.temp, "init", "--quiet", "empty");
  await assert.rejects(git(join(repos.temp, "empty"), "bundle", "verify", bundle));

  const received = await receiveBundle({
    repository: repos.server,
    file: bundle,
    ...taken,
    branch: "feature/fast-parser",
    fallbackBranch: "rehome/fallback",
  });
  assert.deepEqual(received, { branch: "feature/fast-parser", previous: null });
  const worktree = join(repos.temp, "server", "worktree");
  await git(repos.server, "worktree", "add", "--quiet", worktree, received.branch);
  await settleWorktree({
    directory: worktree,
    head,
    branch: received.branch,
    upstream: "origin/main",
  });

  assert.equal(await git(worktree, "rev-parse", "HEAD"), head);
  assert.equal(await git(worktree, "log", "-1", "--format=%s"), "feat: add the fast path");
  assert.deepEqual(await status(worktree), MOVED_STATUS);
  assert.deepEqual((await readdir(worktree)).sort(), [
    ".git",
    ".gitignore",
    "fast.ts",
    "notes.md",
    "parser.ts",
    "staged.ts",
  ]);
  assert.equal(await git(worktree, "rev-parse", "--abbrev-ref", "@{upstream}"), "origin/main");
  assert.equal(await temporaryRefs(repos.server), "");
});

for (const row of [
  { name: "is behind the moved work", prepare: [], expected: "feature/fast-parser", kept: false },
  {
    name: "holds other commits",
    prepare: [["commit", "--allow-empty", "-m", "chore: local work"]],
    expected: "feature/fast-parser-moved",
    kept: true,
  },
  {
    name: "is checked out",
    prepare: [],
    checkout: true,
    expected: "feature/fast-parser-moved",
    kept: true,
  },
] as const)
  test(`a destination branch of the same name that ${row.name} is ${row.kept ? "left alone" : "moved forward"}`, async (t) => {
    const repos = await repositories(t);
    await dirtyFeatureBranch(repos.laptop);
    await git(repos.server, "switch", "--quiet", "-c", "feature/fast-parser");
    for (const args of row.prepare) await git(repos.server, ...args);
    const before = await git(repos.server, "rev-parse", "feature/fast-parser");
    if (!("checkout" in row)) await git(repos.server, "switch", "--quiet", "main");
    const taken = await snapshot(repos.laptop);
    const bundle = join(repos.temp, "work.bundle");
    await createBundle({
      directory: repos.laptop,
      snapshotRef: taken.snapshotRef,
      haves: await listHaves(repos.server),
      file: bundle,
    });
    const received = await receiveBundle({
      repository: repos.server,
      file: bundle,
      ...taken,
      branch: "feature/fast-parser",
      fallbackBranch: "rehome/fallback",
    });
    assert.equal(received.branch, row.expected);
    assert.equal(await git(repos.server, "rev-parse", received.branch), taken.snapshot);
    if (row.kept) assert.equal(await git(repos.server, "rev-parse", "feature/fast-parser"), before);
  });

test("a move carried by the client copies the thread, then archives the original", async (t) => {
  // Larger than one chunk, so the copy crosses the client in several steps.
  const world = await twoMachines(t, { extraBytes: 400 * 1024 });
  const laptop = await machine(t, world.laptop, world.data("laptop"));
  const server = await machine(t, world.server, world.data("server"));

  const { notice, steps } = await carryMove(
    { laptop, server },
    {
      environmentId: laptop.environmentId,
      command: "move",
      input: { threadId: "thread-1", destination: server.environmentId },
    },
  );

  assert.equal(notice.tone, "success");
  assert.match(notice.text, /^Moved to server\. The original was archived here\./);
  const [movedId, moved] = [...server.threads.entries()][0]!;
  assert.deepEqual(notice.thread, { environmentId: server.environmentId, threadId: movedId });
  assert.equal(moved.conversation, CONVERSATION);
  assert.equal(await git(moved.directory, "rev-parse", "HEAD"), world.head);
  assert.equal(await git(moved.directory, "branch", "--show-current"), "feature/fast-parser");
  assert.deepEqual(await status(moved.directory), [...MOVED_STATUS, "?? fixture.bin"].sort());
  assert.equal((await stat(join(moved.directory, "fixture.bin"))).size, 400 * 1024);

  const original = laptop.threads.get("thread-1")!;
  assert.equal(original.running, false);
  assert.deepEqual(original.movedTo, { environmentLabel: "server", threadId: movedId });
  assert.deepEqual(laptop.calls, ["export", "archive"]);
  assert.deepEqual(server.calls, ["import"]);
  // The original is archived only once the copy exists.
  assert.ok(steps.lastIndexOf("server:import") < steps.indexOf("laptop:finish"));
  assert.ok(steps.filter((step) => step === "server:receive").length > 1);

  for (const side of [laptop, server]) {
    assert.equal(
      await temporaryRefs(side === laptop ? world.repos.laptop : world.repos.server),
      "",
    );
    assert.deepEqual(
      await readdir(join(side.data, "transfers", side === laptop ? "out" : "in")),
      [],
    );
  }
});

for (const row of [
  {
    name: "the destination has no project for the repository",
    arrange: async (world: Awaited<ReturnType<typeof twoMachines>>) => {
      world.server.failures.noProject = true;
    },
    message: /no project for github\.com\/example\/parser/,
    stopped: false,
  },
  {
    name: "the destination cannot create the thread",
    arrange: async (world: Awaited<ReturnType<typeof twoMachines>>) => {
      world.server.failures.import = "The worktree could not be created.";
    },
    message: /The worktree could not be created\./,
    stopped: true,
  },
  {
    name: "the thread has subagent threads",
    arrange: async (world: Awaited<ReturnType<typeof twoMachines>>) => {
      world.laptop.threads.get("thread-1")!.subagents = 1;
    },
    message: /subagent threads/,
    stopped: false,
  },
])
  test(`a move that fails because ${row.name} loses nothing`, async (t) => {
    const world = await twoMachines(t);
    await row.arrange(world);
    const laptop = await machine(t, world.laptop, world.data("laptop"));
    const server = await machine(t, world.server, world.data("server"));
    const before = {
      laptopStatus: await status(world.repos.laptop),
      serverBranches: await git(world.repos.server, "for-each-ref", "refs/heads"),
    };

    const { notice } = await carryMove(
      { laptop, server },
      {
        environmentId: laptop.environmentId,
        command: "move",
        input: { threadId: "thread-1", destination: server.environmentId },
      },
    );

    assert.equal(notice.tone, "danger");
    assert.match(notice.text, row.message);
    const original = laptop.threads.get("thread-1")!;
    assert.equal(original.archived, false);
    // A thread that cannot move is refused before its turn is stopped.
    assert.equal(original.running, !row.stopped);
    assert.equal(server.threads.size, 0);
    assert.deepEqual(await status(world.repos.laptop), before.laptopStatus);
    assert.equal(
      await git(world.repos.server, "for-each-ref", "refs/heads"),
      before.serverBranches,
    );
    assert.equal(await temporaryRefs(world.repos.laptop), "");
    assert.equal(await temporaryRefs(world.repos.server), "");
  });

test("a move still succeeds, with a warning, when the original cannot be archived", async (t) => {
  const world = await twoMachines(t);
  world.laptop.failures.archive = "The original thread could not be archived.";
  const laptop = await machine(t, world.laptop, world.data("laptop"));
  const server = await machine(t, world.server, world.data("server"));
  const { notice } = await carryMove(
    { laptop, server },
    {
      environmentId: laptop.environmentId,
      command: "move",
      input: { threadId: "thread-1", destination: server.environmentId },
    },
  );
  assert.equal(notice.tone, "warning");
  assert.match(notice.text, /Moved to server\. The original could not be archived here/);
  assert.equal(server.threads.size, 1);
});

async function pullWorld(t: TestContext, sourceKey: string | null) {
  const world = await twoMachines(t);
  const laptop = await machine(t, world.laptop, world.data("laptop"));
  const server = await machine(t, world.server, world.data("server"));
  const secrets = () => {
    const values = new Map<string, string>();
    return {
      get: async (key: string) => values.get(key) ?? null,
      set: async (key: string, value: string) => void values.set(key, value),
      remove: async (key: string) => void values.delete(key),
    };
  };
  const serve = createPullServer(
    laptop.source,
    new Config(
      laptop.data,
      secrets(),
      sourceKey === null ? {} : { [TRANSFER_KEY_VARIABLE]: sourceKey },
    ),
  );
  const requests: string[] = [];
  const puller = new Puller(server.destination, new Config(server.data, secrets(), {}), {
    // The server reaches the laptop; the laptop never opens a connection of its own.
    fetch: async (url, init) => {
      requests.push(new URL(String(url)).pathname);
      return serve(new Request(String(url), init));
    },
  });
  const pull = async (key: string): Promise<PullStatus> =>
    puller.wait(
      await puller.start({ threadId: "thread-1", source: { url: "http://laptop:3773/", key } }),
      30_000,
    );
  return { world, laptop, server, pull, requests };
}

test("a pull moves a thread with no client in the middle", async (t) => {
  const { world, laptop, server, pull, requests } = await pullWorld(t, "transfer-key");
  const result = await pull("transfer-key");

  assert.equal(result.state, "done");
  assert.deepEqual(result.warnings, []);
  const [movedId, moved] = [...server.threads.entries()][0]!;
  assert.deepEqual(result.thread, {
    environmentId: server.environmentId,
    environmentLabel: "server",
    threadId: movedId,
    projectId: "project",
    directory: moved.directory,
    branch: "feature/fast-parser",
    title: "Fix the parser",
  });
  assert.deepEqual(result.source, {
    environmentId: laptop.environmentId,
    environmentLabel: "laptop",
    threadId: "thread-1",
    archived: true,
  });
  assert.equal(moved.conversation, CONVERSATION);
  assert.equal(await git(moved.directory, "rev-parse", "HEAD"), world.head);
  assert.deepEqual(await status(moved.directory), MOVED_STATUS);
  assert.equal(laptop.threads.get("thread-1")!.archived, true);
  assert.deepEqual(requests, [
    "/api/plugins/rehome/offer",
    "/api/plugins/rehome/export",
    "/api/plugins/rehome/finish",
  ]);
  assert.equal(await temporaryRefs(world.repos.laptop), "");
  assert.equal(await temporaryRefs(world.repos.server), "");
});

for (const row of [
  { name: "the wrong transfer key", sourceKey: "transfer-key", code: "unauthorized" },
  { name: "no transfer key on the source", sourceKey: null, code: "pull-disabled" },
] as const)
  test(`a pull with ${row.name} is refused before the thread is touched`, async (t) => {
    const { laptop, server, pull } = await pullWorld(t, row.sourceKey);
    const result = await pull("guessed-key");
    assert.equal(result.state, "failed");
    assert.equal(result.error?.code, row.code);
    assert.deepEqual(laptop.calls, []);
    assert.equal(laptop.threads.get("thread-1")!.running, true);
    assert.equal(server.threads.size, 0);
  });

test("a pull names its source by a saved name, so no key passes through an agent", async (t) => {
  const { server } = await pullWorld(t, "transfer-key");
  const values = new Map<string, string>();
  const config = new Config(
    server.data,
    {
      get: async (key) => values.get(key) ?? null,
      set: async (key, value) => void values.set(key, value),
      remove: async (key) => void values.delete(key),
    },
    {},
  );
  await config.saveSource({ name: "sandbox", url: "http://sandbox:3773/some/path", key: "k" });
  assert.deepEqual(await config.resolve("sandbox"), { url: "http://sandbox:3773", key: "k" });
  assert.deepEqual(await config.sources(), [{ name: "sandbox", url: "http://sandbox:3773" }]);
  await assert.rejects(config.resolve("unknown"), /No saved source/);
  await assert.rejects(config.resolve({ url: "file:///etc/passwd", key: "k" }), /http/);
  await config.removeSource("sandbox");
  await assert.rejects(config.resolve("sandbox"), /No saved source/);
  await writeFile(join(server.data, "sources.json"), "not json");
  assert.deepEqual(await config.sources(), []);
});
