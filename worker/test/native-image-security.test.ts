import { createRequire } from "node:module";
import { expect, it } from "vitest";

it("loads patched librsvg and decodes a synthetic SVG through Miniflare's sharp", async () => {
  const require = createRequire(import.meta.url);
  const sharp = createRequire(require.resolve("miniflare"))("sharp");
  const rsvg = String(sharp.versions.rsvg ?? "");
  expect(rsvg).toMatch(/^\d+\.\d+\.\d+$/);
  const [major, minor, patch] = rsvg.split(".").map(Number);
  // Check the native library actually loaded, not just the JavaScript package pin.
  // GHSA-wq5f-xc86-pv6w is fixed in librsvg 2.63.2.
  expect(major > 2 || (major === 2 && (minor > 63 || (minor === 63 && patch >= 2))))
    .toBe(true);

  const svg = Buffer.from(
    '<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1">' +
    '<rect width="1" height="1" fill="#102030"/></svg>',
  );
  const { data, info } = await sharp(svg).ensureAlpha().raw().toBuffer({ resolveWithObject: true });
  expect(info).toMatchObject({ width: 1, height: 1, channels: 4 });
  expect([...data]).toEqual([16, 32, 48, 255]);
});
