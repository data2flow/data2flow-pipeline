// expect: OK|SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { const big = 'x'.repeat(1000000); for (let i = 0; i < 100; i++) console.log(big); return msg; }
