// expect: SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { return { n: Array.from({ length: 1e9 }).length }; }
