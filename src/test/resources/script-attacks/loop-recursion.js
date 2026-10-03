// expect: SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { return (function f() { return f() + 1; })(); }
