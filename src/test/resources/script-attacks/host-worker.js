// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { new Worker('data:text/javascript,while(true){}'); return msg; }
