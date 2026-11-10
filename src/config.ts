import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { MoveError } from "./moves.ts";

export interface Secrets {
  readonly get: (key: string) => Promise<string | null>;
  readonly set: (key: string, value: string) => Promise<void>;
  readonly remove: (key: string) => Promise<void>;
}
export interface SavedSource {
  readonly name: string;
  readonly url: string;
}
export interface ResolvedSource {
  readonly url: string;
  readonly key: string;
}

/** Lets a provisioning tool set the key before the server starts, with no client involved. */
export const TRANSFER_KEY_VARIABLE = "T3_REHOME_TRANSFER_KEY";
const SOURCE_NAME = /^[a-z][a-z0-9-]{0,40}$/;

/** A T3 server origin, without a path: the plugin adds its own. */
export function normalizeSourceUrl(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value.trim());
  } catch {
    throw new MoveError(
      "invalid",
      "The source address must be a full URL, such as http://host:3773.",
    );
  }
  if ((parsed.protocol !== "http:" && parsed.protocol !== "https:") || parsed.username !== "")
    throw new MoveError("invalid", "The source address must start with http:// or https://.");
  return parsed.origin;
}

export class Config {
  readonly #file: string;
  readonly #secrets: Secrets;
  readonly #environment: Readonly<Record<string, string | undefined>>;
  constructor(
    dataDirectory: string,
    secrets: Secrets,
    environment: Readonly<Record<string, string | undefined>> = process.env,
  ) {
    this.#file = join(dataDirectory, "sources.json");
    this.#secrets = secrets;
    this.#environment = environment;
  }

  /** Null means other environments cannot pull threads from this one. */
  async transferKey(): Promise<{ key: string; from: "environment" | "settings" } | null> {
    const provisioned = this.#environment[TRANSFER_KEY_VARIABLE]?.trim();
    if (provisioned) return { key: provisioned, from: "environment" };
    const saved = await this.#secrets.get("transfer-key");
    return saved ? { key: saved, from: "settings" } : null;
  }
  async createKey(): Promise<string> {
    const key = randomBytes(32).toString("base64url");
    await this.#secrets.set("transfer-key", key);
    return key;
  }
  async removeKey(): Promise<void> {
    await this.#secrets.remove("transfer-key");
  }
  /** Compares digests so neither length nor content leaks through timing. */
  async accepts(presented: string | null): Promise<"ok" | "wrong" | "disabled"> {
    const expected = await this.transferKey();
    if (expected === null) return "disabled";
    const digest = (value: string) => createHash("sha256").update(value).digest();
    return presented !== null && timingSafeEqual(digest(presented), digest(expected.key))
      ? "ok"
      : "wrong";
  }

  async sources(): Promise<SavedSource[]> {
    const text = await readFile(this.#file, "utf8").catch(() => "[]");
    try {
      const parsed: unknown = JSON.parse(text);
      return Array.isArray(parsed)
        ? parsed.flatMap((item: unknown) =>
            typeof item === "object" &&
            item !== null &&
            "name" in item &&
            "url" in item &&
            typeof item.name === "string" &&
            typeof item.url === "string"
              ? [{ name: item.name, url: item.url }]
              : [],
          )
        : [];
    } catch {
      return [];
    }
  }
  async saveSource(input: { name: string; url: string; key: string }): Promise<void> {
    const name = input.name.trim();
    if (!SOURCE_NAME.test(name))
      throw new MoveError(
        "invalid",
        "Name the source with lowercase letters, digits and hyphens, starting with a letter.",
      );
    if (input.key.trim() === "") throw new MoveError("invalid", "Enter the source's transfer key.");
    const url = normalizeSourceUrl(input.url);
    await this.#secrets.set(`source-${name}`, input.key.trim());
    const others = (await this.sources()).filter((source) => source.name !== name);
    await writeFile(this.#file, JSON.stringify([...others, { name, url }], null, 2));
  }
  async removeSource(name: string): Promise<void> {
    if (!SOURCE_NAME.test(name)) return;
    await this.#secrets.remove(`source-${name}`);
    const others = (await this.sources()).filter((source) => source.name !== name);
    await writeFile(this.#file, JSON.stringify(others, null, 2));
  }
  /** A saved name keeps keys out of agent conversations; an inline pair suits another plugin. */
  async resolve(source: unknown): Promise<ResolvedSource> {
    if (typeof source === "string") {
      const saved = (await this.sources()).find((candidate) => candidate.name === source);
      const key = saved ? await this.#secrets.get(`source-${saved.name}`) : null;
      if (!saved || key === null)
        throw new MoveError(
          "invalid",
          `No saved source is named "${source}". Add it in Settings, Extensions, Rehome.`,
        );
      return { url: saved.url, key };
    }
    if (
      typeof source === "object" &&
      source !== null &&
      "url" in source &&
      "key" in source &&
      typeof source.url === "string" &&
      typeof source.key === "string" &&
      source.key !== ""
    )
      return { url: normalizeSourceUrl(source.url), key: source.key };
    throw new MoveError("invalid", 'Pass "source" as a saved source name or as { url, key }.');
  }
}
