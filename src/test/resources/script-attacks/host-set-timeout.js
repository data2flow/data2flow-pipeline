// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { setTimeout(function () { while (true) {} }, 0); return msg; }
