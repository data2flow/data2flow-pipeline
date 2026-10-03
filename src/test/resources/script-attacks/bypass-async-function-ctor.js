// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor; return new AsyncFunction('return 1')(); }
