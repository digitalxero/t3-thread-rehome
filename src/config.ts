import { createHash, timingSafeEqual } from "node:crypto";
import { MoveError } from "./moves.ts";

export interface PullSource {
  readonly url: string;
  readonly key: string;
}

/**
 * The only way to allow pulls from a machine. Whoever starts the server sets it, such as a
 * tool that provisions a sandbox, so the plugin needs no settings of its own.
 */
export const TRANSFER_KEY_VARIABLE = "T3_REHOME_TRANSFER_KEY";

/** A T3 server origin, without a path: the plugin adds its own. */
function normalizeSourceUrl(value: string): string {
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

/** Compares digests so neither length nor content leaks through timing. */
export function checkTransferKey(
  presented: string | null,
  environment: Readonly<Record<string, string | undefined>> = process.env,
): "ok" | "wrong" | "disabled" {
  const expected = environment[TRANSFER_KEY_VARIABLE]?.trim();
  if (!expected) return "disabled";
  const digest = (value: string) => createHash("sha256").update(value).digest();
  return presented !== null && timingSafeEqual(digest(presented), digest(expected))
    ? "ok"
    : "wrong";
}

export function readPullSource(source: unknown): PullSource {
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
  throw new MoveError("invalid", 'Pass "source" as { url, key }.');
}
