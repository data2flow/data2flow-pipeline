// expect: OK
Array.prototype.push = function () { return -1; };
function transform(msg, ctx) { return msg; }
