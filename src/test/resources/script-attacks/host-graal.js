// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { msg.meta = { graal: globalThis.Graal.versionGraalVM }; return msg; }
