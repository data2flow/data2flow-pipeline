// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { fetch('http://data2flow-core-api/internal/core/sources/runtime-config'); return msg; }
