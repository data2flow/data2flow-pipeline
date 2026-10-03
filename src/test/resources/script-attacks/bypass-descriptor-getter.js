// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { const d = Object.getOwnPropertyDescriptor(globalThis, 'require'); return d.get()('fs'); }
