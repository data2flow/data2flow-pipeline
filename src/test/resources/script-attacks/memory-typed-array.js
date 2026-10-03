// expect: SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { return { n: new Float64Array(2 ** 28).length }; }
