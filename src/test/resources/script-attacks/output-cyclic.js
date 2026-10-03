// expect: SCRIPT_OUTPUT_INVALID
function transform(msg, ctx) { const a = { key: 'a', value: 1 }; a.self = a; msg.meta = { a: a }; return msg; }
