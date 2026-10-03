// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { const R = Java.type('java.lang.Runtime'); R.getRuntime().exec('touch /tmp/pwned'); return msg; }
