// expect: SCRIPT_FORBIDDEN_API|SCRIPT_RUNTIME_ERROR
function transform(msg, ctx) { const g = this.constructor.constructor('return this')(); return g; }
