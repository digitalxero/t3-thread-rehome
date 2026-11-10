import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { PluginError, PluginView } from "@t3tools/plugin-sdk";
import plugin from "../dist/plugin.mjs";
const manifest = JSON.parse(await readFile(new URL("../t3-plugin.json", import.meta.url), "utf8"));
const pkg = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
assert.equal(plugin.id, manifest.id);
assert.equal(plugin.version, pkg.version);
assert.equal(manifest.version, pkg.version);
const directory = await mkdtemp(join(tmpdir(), "rehome-artifact-")),
  scope = await Effect.runPromise(Scope.make());
const unused = () => Effect.fail(new PluginError({ operation: "unused", cause: null }));
try {
  const contributions = await Effect.runPromise(
    plugin
      .activate({
        id: plugin.id,
        dataDirectory: directory,
        capabilities: ["thread-details", "thread-portability"],
        httpBasePath: "/api/plugins/rehome",
        httpBaseUrl: Effect.succeed("http://unused.invalid"),
        threads: {
          describe: unused,
          export: unused,
          projects: unused,
          import: unused,
          archive: unused,
        },
        secrets: {
          get: () => Effect.succeed(null),
          set: () => Effect.void,
          remove: () => Effect.void,
        },
      })
      .pipe(Effect.provideService(Scope.Scope, scope)),
  );
  assert.deepEqual(
    contributions.views.map((view) => view.location),
    ["thread-details"],
  );
  for (const view of contributions.views)
    Schema.decodeUnknownSync(PluginView)(await Effect.runPromise(view.read()));
  assert.ok(contributions.commands.some((command) => command.id === "pull" && command.agent));
  console.log("Bundled artifact activation and portable view schemas passed");
} finally {
  await Effect.runPromise(Scope.close(scope, Exit.void));
  await rm(directory, { recursive: true, force: true });
}
