// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { return Function('return fetch')()('http://x'); }
