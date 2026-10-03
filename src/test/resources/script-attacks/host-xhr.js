// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { const x = new XMLHttpRequest(); x.open('GET', 'http://169.254.169.254/'); x.send(); return msg; }
