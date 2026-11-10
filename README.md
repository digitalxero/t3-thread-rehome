# Rehome for T3 Code

Move a started thread to another machine, with its conversation and the work
that has not left the first machine yet: commits you never pushed and changes
you never committed, including new files.

It implements the proposal in
[pingdotgg/t3code#31077](https://github.com/pingdotgg/t3code/discussions/31077)
as a plugin. A move is a copy followed by an archive. The new thread belongs to
the destination and the original stays where it was, archived, with a note
saying where the work continues. Nothing is deleted, so a move that fails
loses nothing.

Requires the plugin host's `thread-details` and `thread-portability`
capabilities. These are on the `t3/create-plugin-system` branch of
`djgilcrease/t3code`; upstream T3 Code does not ship this host yet. Plugins are
trusted extensions in supervised processes, not sandboxes.

Install the tagged plugin with its repository URL in **Settings → Plugins**, on
**both** machines. Each machine also needs a project for the same repository.

## Move a thread

Open the thread's details. Next to the machine name, choose **Move thread…**,
pick the destination, and confirm. On mobile the button is in the thread's Git
sheet, under the machine's name. When the move finishes, **Open thread** takes
you to the copy.

What happens:

1. A running turn is stopped. Its queue is held, not discarded.
2. The working tree is committed to a snapshot. The snapshot and the branch's
   commits that the destination does not already have are packed into a Git
   bundle. Commits the destination has, from a fetch or an earlier pull, are
   left out, so the copy is usually small.
3. The destination creates the branch and a worktree for it, restores the
   snapshot as uncommitted changes, and creates the thread with the copied
   messages.
4. The original is archived, with a note naming the destination.

The copy starts a **new provider session** on its first message, and that
session is given the conversation. The provider's own session lives on the first
machine's disk and is not moved.

### What does not travel

- Files Git ignores, such as `.env`, `node_modules`, and build output.
- Attachments, terminals, and previews.
- The difference between staged and unstaged changes. Everything arrives
  unstaged, and new files arrive untracked.
- Very long histories: the first message and the most recent 2,000 are copied.
  The archived original keeps the rest.

### What cannot move yet

- A thread whose project has no Git remote. The remote is how the two machines
  agree they hold the same repository.
- A thread whose repository is not a project on the destination. Add the
  project there first.
- A thread with subagent threads.
- More than 64 MB of new work through the button. Push the branch first, or
  [pull](#pull-with-no-app-in-the-middle) the thread instead.

If the destination already has a branch of the same name with other commits, or
has it checked out, the moved work lands on `<branch>-moved` and the result says
so. Your existing branch is never rewritten.

## Pull, with no app in the middle

The button carries the copy through the app you are using, so it works over any
connection, including when the two machines cannot reach each other. A **pull**
needs no app: the destination connects to the source and copies the thread. Use
it from an agent or another plugin, or when the source cannot open connections
to the destination, such as a cloud sandbox behind NAT.

1. On the **source**, open **Settings → Extensions → Rehome** and create a
   transfer key. A tool that provisions the machine can set
   `T3_REHOME_TRANSFER_KEY` in the server's environment instead. Without a key,
   nothing can be pulled from a machine.
2. On the **destination**, save the source there: a name, the address of its T3
   server, and the key. Or pass the address and key in the call.
3. Run `pull` on the destination.

Anyone holding a transfer key can copy any thread from that machine. Treat it
like a password, and remove it when you are done.

### Command contract

Plugin id `rehome`. Both commands run on the **destination** and are open to
agents (`t3_plugin_run`, `t3_plugin_read`) and to other plugins
(`context.plugins.run`).

**`pull`** (permission `orchestration:operate`)

```json
{ "threadId": "<thread id on the source>", "source": "sandbox", "waitMs": 20000 }
```

`source` is a saved source's name, or `{ "url": "http://host:3773", "key": "…" }`.
`waitMs` is optional, at most 20000, and defaults to 20000.

**`status`** (permission `orchestration:read`)

```json
{ "moveId": "<from pull>", "waitMs": 20000 }
```

Both return the same object:

```json
{
  "ok": true,
  "moveId": "0d7c…",
  "state": "done",
  "step": "finish",
  "thread": {
    "environmentId": "…",
    "environmentLabel": "home",
    "threadId": "…",
    "projectId": "…",
    "directory": "/home/me/.t3/worktrees/parser/feature-fast-parser",
    "branch": "feature/fast-parser",
    "title": "Fix the parser"
  },
  "source": {
    "environmentId": "…",
    "environmentLabel": "sandbox",
    "threadId": "…",
    "archived": true
  },
  "warnings": []
}
```

- `state` is `running`, `done`, or `failed`. `step` is `offer`, `export`,
  `import`, or `finish`.
- `thread` and `source` are present when `state` is `done`.
- `error: { code, message }` is present when `state` is `failed`. The message
  is written for a person.
- A request that is refused outright returns
  `{ "ok": false, "error": { "code", "message" } }` and starts nothing.

A move usually finishes inside the first call. If `state` is `running`, call
`status` with the `moveId` until it is not. A command is cut off at 30 seconds,
so a caller should not wait longer than `waitMs` allows.

Error codes: `invalid`, `not-found`, `archived`, `not-git`, `has-subagents`,
`busy`, `no-project`, `too-large`, `git`, `host`, `unauthorized`,
`pull-disabled`, `source`, `internal`.

To use Rehome only when it is installed, check `context.plugins.list()` for a
plugin with id `rehome` and a `pull` command before calling it.

## How the two paths differ

|                      | Button                             | Pull                                 |
| -------------------- | ---------------------------------- | ------------------------------------ |
| Who carries the copy | The app, in small steps            | The destination, over HTTP           |
| Needs                | The app connected to both machines | The destination can reach the source |
| Authentication       | Your session on each machine       | The source's transfer key            |
| Size limit           | 64 MB of new work                  | 2 GB                                 |
| Started from         | The source thread                  | The destination                      |

Neither path needs the source to open a connection to the destination.

## Develop

```sh
pnpm install
pnpm run typecheck
pnpm test
pnpm run build
pnpm run verify
```

`task verify` runs all of them. The tests use real Git repositories and a
stand-in for the T3 host. `dist/plugin.mjs` is committed because T3 installs a
tagged commit as it is and runs no build.

The plugin runs Git itself instead of through the host's supervised process
runner. That runner keeps 8 KiB of output and has no standard input, and the
plugin needs both to compare history and to build a bundle.

`vendor/plugin-sdk` is a snapshot of the host's SDK. `vendor/plugin-sdk/NOTICE`
names the commit it was taken from.

See [RELEASING](.repo-policy/RELEASING.md) for how a version is tagged.
