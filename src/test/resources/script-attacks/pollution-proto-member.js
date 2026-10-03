// expect: OK
function transform(msg, ctx) { msg.__proto__.x = 1; return msg; }
