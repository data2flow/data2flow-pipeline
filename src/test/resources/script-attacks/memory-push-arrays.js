// expect: SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { let a = []; while (1) a.push(new Array(1e5).fill(1)); }
