import assert from "node:assert/strict";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { PluginError, PluginView, type PluginContext } from "@t3tools/plugin-sdk";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import plugin from "../src/plugin.ts";

async function activate(t: TestContext, capabilities: string[]) {
  const dataDirectory = await realpath(await mkdtemp(join(tmpdir(), "rehome-plugin-")));
  const scope = await Effect.runPromise(Scope.make());
  t.after(async () => {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await rm(dataDirectory, { recursive: true, force: true });
  });
  const secrets = new Map<string, string>();
  const unavailable = () => Effect.fail(new PluginError({ operation: "thread", cause: null }));
  const context: PluginContext = {
    id: "rehome",
    dataDirectory,
    capabilities,
    httpBasePath: "/api/plugins/rehome",
    httpBaseUrl: Effect.succeed("http://unused.invalid/api/plugins/rehome"),
    threads: {
      describe: unavailable,
      export: unavailable,
      projects: unavailable,
      import: unavailable,
      archive: unavailable,
    },
    secrets: {
      get: (key) => Effect.succeed(secrets.get(key) ?? null),
      set: (key, value) => Effect.sync(() => void secrets.set(key, value)),
      remove: (key) => Effect.sync(() => void secrets.delete(key)),
    },
  };
  return Effect.runPromise(
    Effect.result(plugin.activate(context).pipe(Effect.provideService(Scope.Scope, scope))),
  );
}
const CAPABILITIES = ["thread-details", "thread-portability"];

test("only the headless commands are open to agents, and every move needs operate access", async (t) => {
  const activated = await activate(t, CAPABILITIES);
  assert.equal(activated._tag, "Success");
  if (activated._tag !== "Success") return;
  const access = Object.fromEntries(
    activated.success.commands.map((command) => [
      command.id,
      `${command.permission}${command.agent ? " agent" : ""}`,
    ]),
  );
  assert.deepEqual(access, {
    pull: "orchestration:operate agent",
    status: "orchestration:read agent",
    // Steps of a carried move run only from the button; an agent cannot start one half-way.
    move: "orchestration:operate",
    accept: "orchestration:operate",
    export: "orchestration:operate",
    send: "orchestration:operate",
    receive: "orchestration:operate",
    import: "orchestration:operate",
    finish: "orchestration:operate",
    abort: "orchestration:operate",
    discard: "orchestration:operate",
  });
  assert.deepEqual(
    activated.success.views.map((view) => [view.id, view.location, view.permission]),
    // The plugin is one button; it has no settings.
    [["thread", "thread-details", "orchestration:operate"]],
  );
});

test("the button asks where to move and says what stays behind, and failures come back as data", async (t) => {
  const activated = await activate(t, CAPABILITIES);
  if (activated._tag !== "Success") return assert.fail("activation failed");
  const { commands, views } = activated.success;
  const run = (id: string, input: Record<string, Schema.Json>) =>
    Effect.runPromise(commands.find((command) => command.id === id)!.run(input));

  for (const view of views)
    Schema.decodeUnknownSync(PluginView)(await Effect.runPromise(view.read()));
  const [block] = (await Effect.runPromise(views[0]!.read())).blocks;
  assert.equal(block?.kind, "status");
  if (block?.kind !== "status") return;
  const [action] = block.actions;
  assert.deepEqual(action?.environment, { name: "destination", label: "Move to" });
  assert.match(action?.confirm ?? "", /files Git ignores/);
  assert.equal(action?.command, "move");

  // A caller gets a reason it can act on, where a thrown error would tell it nothing.
  assert.deepEqual(await run("pull", { threadId: "thread", source: "unknown" }), {
    ok: false,
    error: { code: "invalid", message: 'Pass "source" as { url, key }.' },
  });
  assert.deepEqual(await run("status", { moveId: "missing" }), {
    ok: false,
    error: { code: "not-found", message: "No move has that id on this machine." },
  });
  assert.deepEqual(await run("move", { threadId: "thread", destination: "environment" }), {
    $t3: "notice",
    tone: "danger",
    text: "The host refused.",
  });
});

test("it does not start on a host that cannot move threads", async (t) => {
  const activated = await activate(t, ["thread-details"]);
  assert.equal(activated._tag, "Failure");
});
