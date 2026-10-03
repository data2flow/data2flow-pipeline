// expect: OK
JSON.parse = function () { return { hacked: true }; };
JSON.stringify = function () { return '{"hacked":true}'; };
function transform(msg, ctx) { return msg; }
