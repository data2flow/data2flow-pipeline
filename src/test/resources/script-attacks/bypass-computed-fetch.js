// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { const f = globalThis['fe' + 'tch']; f('http://x'); return msg; }
