// expect: SCRIPT_OUTPUT_INVALID
function transform(msg, ctx) { let o = {}; const root = o; for (let i = 0; i < 1000; i++) { o.n = {}; o = o.n; } msg.meta = root; return msg; }
