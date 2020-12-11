/**
 * @name Unchecked Error Code
 * @description Unchecked Error Code
 * @kind problem
 * @id cpp/UncheckedErrorCode
 * @problem.severity warning
 */

import cpp

class ErrorReturningFunction extends Function {
    ErrorReturningFunction() {
        // Must return a value.
        not this.getType() instanceof VoidType and
        (
            // Anything declared in the cdb2api.
            this.getADeclarationLocation().getFile().getFileName() = "cdb2api.h"
            or
            // Anything that returns an E* macro.
            exists(ReturnStmt r |
                this = r.getEnclosingFunction() and
                r.getExpr() = any(MacroInvocationExpr m | m.getMacroName().regexpMatch("E[A-Z]+"))
            )
        )
    }

    predicate alwaysReturnsSameValue() {
        forall(ReturnStmt a, ReturnStmt b |
            this = a.getEnclosingFunction() and
            this = b.getEnclosingFunction()
        |
            a.getExpr().getValue() = b.getExpr().getValue()
        )
    }
}

from ErrorReturningFunction f, FunctionCall call
where
    call = f.getACallToThisFunction() and
    call instanceof ExprInVoidContext and
    not f.alwaysReturnsSameValue()
select call, "Call to $@ does not check return code.", f, f.getName()
