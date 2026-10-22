function assert(condition, message) {
  if (!condition) throw new Error(message);
}

import pkg from "./test_import_attributes_fixture.json" with { type: "json" };
import pkgAssert from "./test_import_attributes_fixture.json" assert { type: "json" };
import "./test_import_attributes_fixture.json" with { type: "json" };
import { kind as namedKind } from "./test_import_attributes_fixture.json" with { type: "json" };

assert(pkg.kind === "fixture", `default json import failed: ${pkg.kind}`);
assert(pkgAssert.kind === "fixture", `assert-form json import failed: ${pkgAssert.kind}`);
assert(namedKind === "fixture", `named json import failed: ${namedKind}`);

import { x as reexported } from "./test_import_attributes_reexporter.mjs";
assert(reexported === 42, `re-export with attributes failed: ${reexported}`);

import { registerHooks } from "node:module";

const seenAttrs = [];
registerHooks({
  resolve(specifier, context, next) {
    if (specifier.endsWith("test_import_attributes_hooked.json"))
      seenAttrs.push(context.importAttributes);
    return next(specifier, context);
  }
});

await import("./test_import_attributes_child.mjs");
assert(
  seenAttrs.length > 0 && seenAttrs.every(a => a && a.type === "json"),
  `loader hook should see importAttributes on every resolve, got ${JSON.stringify(seenAttrs)}`
);

console.log("test_import_attributes: OK");
