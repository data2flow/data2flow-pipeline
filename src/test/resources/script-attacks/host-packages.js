// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { new Packages.java.io.File('/etc/passwd').exists(); return msg; }
