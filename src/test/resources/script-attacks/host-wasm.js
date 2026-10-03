// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { WebAssembly.instantiate(new Uint8Array([0, 97, 115, 109])); return msg; }
