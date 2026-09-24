export class PayloadTooLargeError extends Error {
  constructor(message = "Request body is too large.") {
    super(message);
    this.name = "PayloadTooLargeError";
  }
}

async function readStreamLimited(
  stream: ReadableStream<Uint8Array>,
  maxBytes: number,
): Promise<ArrayBuffer> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel("size limit exceeded");
        throw new PayloadTooLargeError(`Decoded payload exceeds ${maxBytes} bytes.`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output.buffer;
}

export async function limitRequestBody(request: Request, maxBytes: number): Promise<Request> {
  if (!request.body) return request;
  const declaredLength = Number(request.headers.get("content-length"));
  if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
    throw new PayloadTooLargeError(`Request body exceeds ${maxBytes} bytes.`);
  }

  const body = await readStreamLimited(request.body, maxBytes);
  const headers = new Headers(request.headers);
  headers.delete("content-length");
  return new Request(request, { body, headers });
}

export async function decodePossiblyGzippedText(
  buffer: ArrayBuffer,
  maxDecodedBytes: number,
): Promise<string> {
  const bytes = new Uint8Array(buffer);
  if (bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b) {
    const compressedBody = new Response(buffer).body;
    if (!compressedBody) throw new Error("Could not read compressed object.");
    const decoded = await readStreamLimited(
      compressedBody.pipeThrough(new DecompressionStream("gzip")),
      maxDecodedBytes,
    );
    return new TextDecoder().decode(decoded);
  }
  if (buffer.byteLength > maxDecodedBytes) {
    throw new PayloadTooLargeError(`Decoded payload exceeds ${maxDecodedBytes} bytes.`);
  }
  return new TextDecoder().decode(buffer);
}

export function secureResponse(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set("cache-control", "no-store");
  headers.set("x-content-type-options", "nosniff");
  headers.set("referrer-policy", "no-referrer");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

