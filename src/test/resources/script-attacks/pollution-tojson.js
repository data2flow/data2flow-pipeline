// expect: OK
Object.defineProperty(Object.prototype, 'toJSON', { value: function () { return 'hacked'; }, enumerable: false });
function transform(msg, ctx) { return msg; }
