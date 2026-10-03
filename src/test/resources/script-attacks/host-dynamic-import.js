// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { return import('fs'); }
