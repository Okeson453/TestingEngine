/**
 * Transport root-cause audit: primary live path must be native `ws` +
 * Engine.IO v3 binary framing — not socket.io-client as a dependency.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = join(dirname(fileURLToPath(import.meta.url)), "../../..");

describe("transport audit (native WS, not socket.io-client)", () => {
  it("package.json does not depend on socket.io-client or engine.io-client", () => {
    const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
    const deps = { ...pkg.dependencies, ...pkg.devDependencies };
    assert.equal(deps["socket.io-client"], undefined);
    assert.equal(deps["engine.io-client"], undefined);
    assert.ok(deps["ws"], "ws must be present for native transport");
  });

  it("native-socket-client imports ws and implements Engine.IO framing", () => {
    const src = readFileSync(
      join(root, "src/lib/crash/native-socket-client.ts"),
      "utf8",
    );
    assert.match(src, /from ["']ws["']/);
    assert.match(src, /Engine\.IO/);
    // No import of socket.io packages (comments may mention the name)
    assert.doesNotMatch(src, /from ["']socket\.io-client["']/);
    assert.doesNotMatch(src, /from ["']engine\.io-client["']/);
  });

  it("native-protocol implements Engine.IO v3 packet parse without socket.io imports", () => {
    const src = readFileSync(join(root, "src/lib/crash/native-protocol.ts"), "utf8");
    assert.match(src, /parsePacket|encodeConnect|isEngineOpenFrame/);
    assert.doesNotMatch(src, /from ["']socket\.io/);
  });
});
