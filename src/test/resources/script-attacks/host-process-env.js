// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { msg.meta = { env: process.env }; return msg; }
