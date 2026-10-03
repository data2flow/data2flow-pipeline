// expect: SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { return { n: 'x'.repeat(2 ** 28).length }; }
