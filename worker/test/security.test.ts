import { gzipSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import {
  PayloadTooLargeError, decodePossiblyGzippedText, limitRequestBody, secureResponse,
} from "../src/security";

describe("request body limits", () => {
  it("preserves a request within the limit", async () => {
    const request = new Request("https://example.test/mcp", { method: "POST", body: "hello" });
    const bounded = await limitRequestBody(request, 5);
    expect(await bounded.text()).toBe("hello");
  });

  it("rejects a streamed body over the limit", async () => {
    const request = new Request("https://example.test/mcp", { method: "POST", body: "too large" });
    await expect(limitRequestBody(request, 3)).rejects.toBeInstanceOf(PayloadTooLargeError);
  });

  it("rejects an oversized declared content length before reading", async () => {
    const request = new Request("https://example.test/mcp", {
      method: "POST",
      headers: { "content-length": "1000" },
      body: "x",
    });
    await expect(limitRequestBody(request, 100)).rejects.toBeInstanceOf(PayloadTooLargeError);
  });
});

describe("bounded R2 decoding", () => {
  it("decodes gzip content within the expanded-size limit", async () => {
    const compressed = gzipSync(Buffer.from("granular data"));
    const buffer = compressed.buffer.slice(
      compressed.byteOffset,
      compressed.byteOffset + compressed.byteLength,
    ) as ArrayBuffer;
    await expect(decodePossiblyGzippedText(buffer, 100)).resolves.toBe("granular data");
  });

  it("rejects gzip content that expands past the limit", async () => {
    const compressed = gzipSync(Buffer.from("a".repeat(1000)));
    const buffer = compressed.buffer.slice(
      compressed.byteOffset,
      compressed.byteOffset + compressed.byteLength,
    ) as ArrayBuffer;
    await expect(decodePossiblyGzippedText(buffer, 100)).rejects
      .toBeInstanceOf(PayloadTooLargeError);
  });
});

describe("response hardening", () => {
  it("adds privacy and content-type protections", () => {
    const response = secureResponse(new Response("ok"));
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("x-content-type-options")).toBe("nosniff");
    expect(response.headers.get("referrer-policy")).toBe("no-referrer");
  });
});

