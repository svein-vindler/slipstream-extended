const path = require("node:path");
const Module = require("node:module");
const root = path.resolve(__dirname, "..");
const deps = process.env.SLIPSTREAM_NODE_MODULES || path.join(root, "worker/node_modules");
const esbuild = require(path.join(deps, "esbuild"));
const built = esbuild.buildSync({
  entryPoints: [path.join(__dirname, "night-native-parity.ts")],
  bundle: true, write: false, platform: "node", format: "cjs", target: "node22", nodePaths: [deps],
});
const modulePath = path.join(__dirname, "night-native-parity.generated.cjs");
const replay = new Module(modulePath, module);
replay.filename = modulePath;
replay.paths = module.paths;
replay._compile(built.outputFiles[0].text, modulePath);
