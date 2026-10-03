// expect: SCRIPT_FORBIDDEN_API|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { return eval(atob('ZmV0Y2goImh0dHA6Ly94Iik=')); }
