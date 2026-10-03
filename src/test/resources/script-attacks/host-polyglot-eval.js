// expect: SCRIPT_FORBIDDEN_API
function transform(msg, ctx) { return Polyglot.eval('python', 'import os; os.system("id")'); }
