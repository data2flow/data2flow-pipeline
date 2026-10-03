// expect: SCRIPT_OUTPUT_INVALID
function transform(msg, ctx) { return function () { return 1; }; }
