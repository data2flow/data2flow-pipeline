// expect: SCRIPT_TIMEOUT|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { const m = new Map(); let i = 0; while (1) m.set(i++, 'value-' + i); }
