// expect: SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { let s = 'x'; for (let i = 0; i < 64; i++) s += s; return { n: s.length }; }
