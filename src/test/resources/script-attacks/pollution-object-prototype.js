// expect: OK
Object.prototype.polluted = 1;
function transform(msg, ctx) { return msg; }
