// expect: SCRIPT_OUTPUT_INVALID|SCRIPT_TIMEOUT
function transform(msg, ctx) { const metrics = []; for (let i = 0; i < 100000; i++) metrics.push({ key: 'k' + i, value: i }); msg.metrics = metrics; return msg; }
