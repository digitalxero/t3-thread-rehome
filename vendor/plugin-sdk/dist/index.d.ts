import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
export declare const PLUGIN_API_VERSION = 1;
export declare const PluginId: Schema.String;
export declare const PluginLocalId: Schema.String;
export declare const PluginInput: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
export type PluginInput = typeof PluginInput.Type;
export declare const PluginPermission: Schema.Literals<readonly ["orchestration:read", "orchestration:operate", "settings:write", "providers:manage", "environment:maintain", "diagnostics:read", "filesystem:read", "filesystem:write", "terminal:read", "terminal:operate", "source-control:write", "review:write", "access:read", "access:write", "relay:read", "relay:write", "preview:operate"]>;
export type PluginPermission = typeof PluginPermission.Type;
export declare const PluginManifest: Schema.Struct<{
    readonly apiVersion: Schema.Literal<1>;
    readonly id: Schema.String;
    readonly name: Schema.String;
    readonly version: Schema.String;
    readonly description: Schema.optionalKey<Schema.String>;
}>;
export type PluginManifest = typeof PluginManifest.Type;
export declare const PluginAction: Schema.Struct<{
    readonly id: Schema.String;
    readonly label: Schema.String;
    readonly command: Schema.String;
    readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
}>;
export type PluginAction = typeof PluginAction.Type;
export declare const PluginTone: Schema.Literals<readonly ["success", "warning", "danger", "muted"]>;
export type PluginTone = typeof PluginTone.Type;
export declare const PluginStatusAction: Schema.Struct<{
    readonly id: Schema.String;
    readonly label: Schema.String;
    readonly command: Schema.String;
    readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
    /** Shown in a confirmation before the command runs. */
    readonly confirm: Schema.optionalKey<Schema.String>;
    /** Ask for another connected environment running this plugin; its id is sent under `name`. */
    readonly environment: Schema.optionalKey<Schema.Struct<{
        readonly name: Schema.String;
        readonly label: Schema.String;
    }>>;
}>;
export type PluginStatusAction = typeof PluginStatusAction.Type;
export declare const PluginField: Schema.Struct<{
    readonly name: Schema.String;
    readonly label: Schema.String;
    readonly kind: Schema.Literals<readonly ["text", "password", "number", "boolean", "select"]>;
    readonly value: Schema.optionalKey<Schema.Union<readonly [Schema.String, Schema.Number, Schema.Boolean]>>;
    readonly options: Schema.optionalKey<Schema.$Array<Schema.Struct<{
        readonly label: Schema.String;
        readonly value: Schema.String;
    }>>>;
    readonly required: Schema.optionalKey<Schema.Boolean>;
}>;
export declare const PluginBlock: Schema.Union<readonly [Schema.Struct<{
    readonly id: Schema.String;
    readonly kind: Schema.Literal<"text">;
    readonly text: Schema.String;
}>, Schema.Struct<{
    readonly id: Schema.String;
    readonly kind: Schema.Literal<"link">;
    readonly label: Schema.String;
    readonly url: Schema.String;
}>, Schema.Struct<{
    readonly id: Schema.String;
    readonly kind: Schema.Literal<"actions">;
    readonly actions: Schema.$Array<Schema.Struct<{
        readonly id: Schema.String;
        readonly label: Schema.String;
        readonly command: Schema.String;
        readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
    }>>;
}>, Schema.Struct<{
    readonly id: Schema.String;
    readonly kind: Schema.Literal<"form">;
    readonly label: Schema.String;
    readonly command: Schema.String;
    readonly fields: Schema.$Array<Schema.Struct<{
        readonly name: Schema.String;
        readonly label: Schema.String;
        readonly kind: Schema.Literals<readonly ["text", "password", "number", "boolean", "select"]>;
        readonly value: Schema.optionalKey<Schema.Union<readonly [Schema.String, Schema.Number, Schema.Boolean]>>;
        readonly options: Schema.optionalKey<Schema.$Array<Schema.Struct<{
            readonly label: Schema.String;
            readonly value: Schema.String;
        }>>>;
        readonly required: Schema.optionalKey<Schema.Boolean>;
    }>>;
    readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
}>, Schema.Struct<{
    readonly id: Schema.String;
    readonly kind: Schema.Literal<"status">;
    readonly tone: Schema.optionalKey<Schema.Literals<readonly ["success", "warning", "danger", "muted"]>>;
    readonly label: Schema.optionalKey<Schema.String>;
    readonly detail: Schema.optionalKey<Schema.String>;
    readonly actions: Schema.$Array<Schema.Struct<{
        readonly id: Schema.String;
        readonly label: Schema.String;
        readonly command: Schema.String;
        readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
        /** Shown in a confirmation before the command runs. */
        readonly confirm: Schema.optionalKey<Schema.String>;
        /** Ask for another connected environment running this plugin; its id is sent under `name`. */
        readonly environment: Schema.optionalKey<Schema.Struct<{
            readonly name: Schema.String;
            readonly label: Schema.String;
        }>>;
    }>>;
}>, Schema.Struct<{
    readonly id: Schema.String;
    readonly kind: Schema.Literal<"table">;
    readonly columns: Schema.$Array<Schema.String>;
    readonly rows: Schema.$Array<Schema.Struct<{
        readonly id: Schema.String;
        readonly cells: Schema.$Array<Schema.String>;
        readonly actions: Schema.$Array<Schema.Struct<{
            readonly id: Schema.String;
            readonly label: Schema.String;
            readonly command: Schema.String;
            readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
        }>>;
    }>>;
}>]>;
export type PluginBlock = typeof PluginBlock.Type;
export declare const PluginView: Schema.Struct<{
    readonly title: Schema.String;
    readonly blocks: Schema.$Array<Schema.Union<readonly [Schema.Struct<{
        readonly id: Schema.String;
        readonly kind: Schema.Literal<"text">;
        readonly text: Schema.String;
    }>, Schema.Struct<{
        readonly id: Schema.String;
        readonly kind: Schema.Literal<"link">;
        readonly label: Schema.String;
        readonly url: Schema.String;
    }>, Schema.Struct<{
        readonly id: Schema.String;
        readonly kind: Schema.Literal<"actions">;
        readonly actions: Schema.$Array<Schema.Struct<{
            readonly id: Schema.String;
            readonly label: Schema.String;
            readonly command: Schema.String;
            readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
        }>>;
    }>, Schema.Struct<{
        readonly id: Schema.String;
        readonly kind: Schema.Literal<"form">;
        readonly label: Schema.String;
        readonly command: Schema.String;
        readonly fields: Schema.$Array<Schema.Struct<{
            readonly name: Schema.String;
            readonly label: Schema.String;
            readonly kind: Schema.Literals<readonly ["text", "password", "number", "boolean", "select"]>;
            readonly value: Schema.optionalKey<Schema.Union<readonly [Schema.String, Schema.Number, Schema.Boolean]>>;
            readonly options: Schema.optionalKey<Schema.$Array<Schema.Struct<{
                readonly label: Schema.String;
                readonly value: Schema.String;
            }>>>;
            readonly required: Schema.optionalKey<Schema.Boolean>;
        }>>;
        readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
    }>, Schema.Struct<{
        readonly id: Schema.String;
        readonly kind: Schema.Literal<"status">;
        readonly tone: Schema.optionalKey<Schema.Literals<readonly ["success", "warning", "danger", "muted"]>>;
        readonly label: Schema.optionalKey<Schema.String>;
        readonly detail: Schema.optionalKey<Schema.String>;
        readonly actions: Schema.$Array<Schema.Struct<{
            readonly id: Schema.String;
            readonly label: Schema.String;
            readonly command: Schema.String;
            readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
            /** Shown in a confirmation before the command runs. */
            readonly confirm: Schema.optionalKey<Schema.String>;
            /** Ask for another connected environment running this plugin; its id is sent under `name`. */
            readonly environment: Schema.optionalKey<Schema.Struct<{
                readonly name: Schema.String;
                readonly label: Schema.String;
            }>>;
        }>>;
    }>, Schema.Struct<{
        readonly id: Schema.String;
        readonly kind: Schema.Literal<"table">;
        readonly columns: Schema.$Array<Schema.String>;
        readonly rows: Schema.$Array<Schema.Struct<{
            readonly id: Schema.String;
            readonly cells: Schema.$Array<Schema.String>;
            readonly actions: Schema.$Array<Schema.Struct<{
                readonly id: Schema.String;
                readonly label: Schema.String;
                readonly command: Schema.String;
                readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
            }>>;
        }>>;
    }>]>>;
}>;
export type PluginView = typeof PluginView.Type;
export declare const PluginViewDescriptor: Schema.Struct<{
    readonly id: Schema.String;
    readonly title: Schema.String;
    readonly location: Schema.Literals<readonly ["settings", "settings-page", "usage", "thread-details"]>;
    readonly permission: Schema.Literals<readonly ["orchestration:read", "orchestration:operate", "settings:write", "providers:manage", "environment:maintain", "diagnostics:read", "filesystem:read", "filesystem:write", "terminal:read", "terminal:operate", "source-control:write", "review:write", "access:read", "access:write", "relay:read", "relay:write", "preview:operate"]>;
}>;
export type PluginViewDescriptor = typeof PluginViewDescriptor.Type;
export declare const PluginCommandDescriptor: Schema.Struct<{
    readonly id: Schema.String;
    readonly title: Schema.String;
    readonly description: Schema.String;
    readonly permission: Schema.Literals<readonly ["orchestration:read", "orchestration:operate", "settings:write", "providers:manage", "environment:maintain", "diagnostics:read", "filesystem:read", "filesystem:write", "terminal:read", "terminal:operate", "source-control:write", "review:write", "access:read", "access:write", "relay:read", "relay:write", "preview:operate"]>;
    /** Agent access is opt-in; secret-bearing commands should leave this false. */
    readonly agent: Schema.Boolean;
}>;
export type PluginCommandDescriptor = typeof PluginCommandDescriptor.Type;
/** An explicit command may disclose a secret temporarily; never return this from a view. */
export declare const PluginSecretResult: Schema.Struct<{
    readonly $t3: Schema.Literal<"secret">;
    readonly label: Schema.String;
    readonly value: Schema.String;
}>;
export type PluginSecretResult = typeof PluginSecretResult.Type;
/** Ends a `thread-details` action with a message, and optionally a thread to open. */
export declare const PluginNoticeResult: Schema.Struct<{
    readonly $t3: Schema.Literal<"notice">;
    readonly tone: Schema.Literals<readonly ["success", "warning", "danger", "muted"]>;
    readonly text: Schema.String;
    readonly thread: Schema.optionalKey<Schema.Struct<{
        readonly environmentId: Schema.String;
        readonly threadId: Schema.String;
    }>>;
}>;
export type PluginNoticeResult = typeof PluginNoticeResult.Type;
/**
 * Continues a `thread-details` action with another command of the same plugin, here or on
 * another connected environment. The client carries each step, so two environments that
 * cannot reach each other can still exchange data, one bounded command at a time.
 */
export declare const PluginRelayResult: Schema.Struct<{
    readonly $t3: Schema.Literal<"relay">;
    /** Omitted: the environment that returned this result. */
    readonly environmentId: Schema.optionalKey<Schema.String>;
    readonly command: Schema.String;
    readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
    /** Progress shown while the next step runs. */
    readonly status: Schema.optionalKey<Schema.String>;
}>;
export type PluginRelayResult = typeof PluginRelayResult.Type;
declare const PluginError_base: Schema.Class<PluginError, Schema.TaggedStruct<"PluginError", {
    readonly operation: Schema.String;
    readonly cause: Schema.Defect;
}>, import("effect/Cause").YieldableError>;
export declare class PluginError extends PluginError_base {
    get message(): string;
}
export declare const PluginMcpServer: Schema.Union<readonly [Schema.Struct<{
    readonly transport: Schema.Literal<"stdio">;
    readonly command: Schema.String;
    readonly args: Schema.$Array<Schema.String>;
    readonly environment: Schema.$Record<Schema.String, Schema.String>;
}>, Schema.Struct<{
    readonly transport: Schema.Literals<readonly ["http", "sse"]>;
    readonly url: Schema.String;
    readonly headers: Schema.$Record<Schema.String, Schema.String>;
}>]>;
export type PluginMcpServer = typeof PluginMcpServer.Type;
export declare const PluginMcpServers: Schema.$Record<Schema.String, Schema.Union<readonly [Schema.Struct<{
    readonly transport: Schema.Literal<"stdio">;
    readonly command: Schema.String;
    readonly args: Schema.$Array<Schema.String>;
    readonly environment: Schema.$Record<Schema.String, Schema.String>;
}>, Schema.Struct<{
    readonly transport: Schema.Literals<readonly ["http", "sse"]>;
    readonly url: Schema.String;
    readonly headers: Schema.$Record<Schema.String, Schema.String>;
}>]>>;
export type PluginMcpServers = typeof PluginMcpServers.Type;
export declare const PluginMcpContext: Schema.Struct<{
    readonly provider: Schema.String;
    readonly instanceId: Schema.String;
    readonly projectDirectory: Schema.optionalKey<Schema.String>;
    readonly cwd: Schema.optionalKey<Schema.String>;
}>;
export type PluginMcpContext = typeof PluginMcpContext.Type;
/** Only T3-owned launches pass through this hook. Terminal shells are unaffected. */
export interface PluginLaunch {
    readonly provider: string;
    readonly purpose: "agent" | "text-generation" | "managed-server";
    readonly cwd?: string;
    readonly args: readonly string[];
    readonly environment: Readonly<Record<string, string | undefined>>;
}
export declare const PluginProcessInput: Schema.Struct<{
    readonly command: Schema.String;
    readonly args: Schema.$Array<Schema.String>;
    readonly cwd: Schema.String;
    readonly environment: Schema.$Record<Schema.String, Schema.String>;
    readonly timeoutMs: Schema.Int;
}>;
export type PluginProcessInput = typeof PluginProcessInput.Type;
export declare const PluginProcessResult: Schema.Struct<{
    readonly code: Schema.NullOr<Schema.Int>;
    readonly timedOut: Schema.Boolean;
    readonly stdout: Schema.String;
    readonly stderr: Schema.String;
    readonly stdoutTruncated: Schema.Boolean;
    readonly stderrTruncated: Schema.Boolean;
}>;
export type PluginProcessResult = typeof PluginProcessResult.Type;
export declare const PluginThreadSummary: Schema.Struct<{
    readonly environmentId: Schema.String;
    readonly environmentLabel: Schema.String;
    readonly threadId: Schema.String;
    readonly title: Schema.String;
    readonly projectId: Schema.String;
    /** Identity shared by clones of one repository; null without a Git remote. */
    readonly repository: Schema.NullOr<Schema.String>;
    /** Where the thread works: its worktree, or the project directory. */
    readonly directory: Schema.String;
    readonly branch: Schema.NullOr<Schema.String>;
    readonly running: Schema.Boolean;
    readonly subagents: Schema.Int;
    readonly archived: Schema.Boolean;
}>;
export type PluginThreadSummary = typeof PluginThreadSummary.Type;
export declare const PluginThreadExport: Schema.Struct<{
    /**
     * A commit of the working tree as it stood: tracked changes and untracked files Git does
     * not ignore. Its parent is `head`. Both are null outside a Git repository.
     */
    readonly snapshot: Schema.NullOr<Schema.String>;
    /** The ref holding `snapshot`; delete it when done. */
    readonly snapshotRef: Schema.NullOr<Schema.String>;
    readonly head: Schema.NullOr<Schema.String>;
    readonly messages: Schema.Int;
    /** Older messages left out to bound the file; the first message is always kept. */
    readonly omittedMessages: Schema.Int;
}>;
export type PluginThreadExport = typeof PluginThreadExport.Type;
export declare const PluginThreadProject: Schema.Struct<{
    readonly projectId: Schema.String;
    readonly title: Schema.String;
    readonly directory: Schema.String;
}>;
export type PluginThreadProject = typeof PluginThreadProject.Type;
export declare const PluginThreadImport: Schema.Struct<{
    readonly environmentId: Schema.String;
    readonly environmentLabel: Schema.String;
    readonly threadId: Schema.String;
    /** The new worktree. */
    readonly directory: Schema.String;
}>;
export type PluginThreadImport = typeof PluginThreadImport.Type;
/**
 * Copies a thread between environments. The conversation file is the host's own format;
 * carry it as bytes. Paths must lie inside `dataDirectory`. A failure's `cause` is an Error
 * whose message is safe to show the user.
 */
export interface PluginThreads {
    readonly describe: (threadId: string) => Effect.Effect<PluginThreadSummary, PluginError>;
    /** Stops a running turn, holding its queue, then snapshots the work and writes the conversation. */
    readonly export: (input: {
        readonly threadId: string;
        readonly file: string;
    }) => Effect.Effect<PluginThreadExport, PluginError>;
    /** Projects on this environment that are clones of the repository. */
    readonly projects: (repository: string) => Effect.Effect<readonly PluginThreadProject[], PluginError>;
    /**
     * Creates a thread from an exported conversation, in a new worktree of an existing local
     * branch. Its first turn starts a new provider session that is handed the history.
     */
    readonly import: (input: {
        readonly file: string;
        readonly projectId: string;
        readonly branch: string;
    }) => Effect.Effect<PluginThreadImport, PluginError>;
    /** Archives a thread after noting in it where the work continues. */
    readonly archive: (input: {
        readonly threadId: string;
        readonly movedTo: {
            readonly environmentLabel: string;
            readonly threadId: string;
        };
    }) => Effect.Effect<void, PluginError>;
}
export declare const PluginPeer: Schema.Struct<{
    readonly id: Schema.String;
    readonly name: Schema.String;
    readonly version: Schema.String;
    readonly commands: Schema.$Array<Schema.Struct<{
        readonly id: Schema.String;
        readonly title: Schema.String;
        readonly description: Schema.String;
        readonly permission: Schema.Literals<readonly ["orchestration:read", "orchestration:operate", "settings:write", "providers:manage", "environment:maintain", "diagnostics:read", "filesystem:read", "filesystem:write", "terminal:read", "terminal:operate", "source-control:write", "review:write", "access:read", "access:write", "relay:read", "relay:write", "preview:operate"]>;
        /** Agent access is opt-in; secret-bearing commands should leave this false. */
        readonly agent: Schema.Boolean;
    }>>;
}>;
export type PluginPeer = typeof PluginPeer.Type;
export declare const PluginPeerCall: Schema.Struct<{
    readonly pluginId: Schema.String;
    readonly commandId: Schema.String;
    readonly input: Schema.$Record<Schema.String, Schema.Codec<Schema.Json, Schema.Json, never, never>>;
}>;
export type PluginPeerCall = typeof PluginPeerCall.Type;
/**
 * Other plugins on this environment. A plugin reaches only what an agent could: commands
 * declared with `agent: true`, and never a secret result. Check `list` before relying on an
 * optional plugin. A failure's `cause` is an Error naming why the call was refused.
 */
export interface PluginPeers {
    readonly list: () => Effect.Effect<readonly PluginPeer[], PluginError>;
    readonly run: (call: PluginPeerCall) => Effect.Effect<Schema.Json, PluginError>;
}
export interface PluginContext {
    readonly id: string;
    readonly dataDirectory: string;
    readonly threads?: PluginThreads;
    readonly plugins?: PluginPeers;
    readonly runProcess?: (input: PluginProcessInput) => Effect.Effect<PluginProcessResult, PluginError>;
    readonly capabilities?: readonly string[];
    /** Relative URL on the owning environment; never a baked-in localhost origin. */
    readonly httpBasePath: string;
    /** Resolved when the environment is listening; evaluate inside handlers, not activation. */
    readonly httpBaseUrl: Effect.Effect<string, PluginError>;
    readonly secrets: {
        readonly get: (key: string) => Effect.Effect<string | null, PluginError>;
        readonly set: (key: string, value: string) => Effect.Effect<void, PluginError>;
        readonly remove: (key: string) => Effect.Effect<void, PluginError>;
    };
}
export declare const PluginHttpResponseTimeout: Schema.Int;
export declare const PluginLifecycleEvent: Schema.Struct<{
    readonly type: Schema.Literals<readonly ["worktree.created", "worktree.removing", "worktree.removed"]>;
    readonly repositoryDirectory: Schema.String;
    readonly worktreePath: Schema.String;
    readonly refName: Schema.optionalKey<Schema.String>;
}>;
export type PluginLifecycleEvent = typeof PluginLifecycleEvent.Type;
export declare const PluginLifecycleResult: Schema.NullOr<Schema.Struct<{
    readonly error: Schema.String;
}>>;
export type PluginLifecycleResult = typeof PluginLifecycleResult.Type;
declare const PluginLifecycleBlocked_base: Schema.Class<PluginLifecycleBlocked, Schema.TaggedStruct<"PluginLifecycleBlocked", {
    readonly pluginId: Schema.String;
    readonly message: Schema.String;
}>, import("effect/Cause").YieldableError>;
export declare class PluginLifecycleBlocked extends PluginLifecycleBlocked_base {
}
export declare const PluginLifecycleTimeout: Schema.Int;
export interface PluginContributions {
    /** Emit after background state changes; clients refetch mounted views, without polling. */
    readonly changes?: Stream.Stream<void, PluginError>;
    readonly onLifecycle?: (event: PluginLifecycleEvent) => Effect.Effect<PluginLifecycleResult | void, PluginError>;
    readonly lifecycleTimeoutMs?: number;
    readonly commands?: readonly (PluginCommandDescriptor & {
        /** Decode and validate feature-specific input inside the handler. */
        readonly run: (input: PluginInput) => Effect.Effect<Schema.Json, PluginError>;
    })[];
    readonly views?: readonly (PluginViewDescriptor & {
        readonly read: () => Effect.Effect<PluginView, PluginError>;
    })[];
    readonly mcpServers?: (context: PluginMcpContext) => Effect.Effect<PluginMcpServers, PluginError>;
    readonly prepareLaunch?: (launch: PluginLaunch) => Effect.Effect<PluginLaunch, PluginError>;
    /**
     * Routes below httpBasePath. Own authentication explicitly (API key, PKCE/state,
     * webhook signatures); this endpoint deliberately has no T3 browser session.
     * Request and response streams stay streaming and carry cancellation.
     */
    readonly handleHttp?: (request: Request) => Effect.Effect<Response, PluginError>;
    /** Response headers may wait for a long RPC; body streams have no whole-response deadline. */
    readonly httpResponseTimeoutMs?: number;
}
export interface T3Plugin extends PluginManifest {
    /** Acquire resources with Effect scopes; the host closes them on shutdown/failure. */
    readonly activate: (context: PluginContext) => Effect.Effect<PluginContributions, PluginError, Scope.Scope>;
}
export {};
