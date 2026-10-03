// expect: OK|SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { msg.meta = { r: /(a+)+$/.test('a'.repeat(30) + '!') }; return msg; }
