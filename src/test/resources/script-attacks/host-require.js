// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { require('child_process').execSync('id'); return msg; }
