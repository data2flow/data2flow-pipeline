// expect: SCRIPT_RUNTIME_ERROR
Array.from = function () { return []; };
function transform(msg, ctx) { return msg; }
