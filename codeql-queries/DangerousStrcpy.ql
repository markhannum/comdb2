/**
 * @name Dangerous strcpy
 * @description Calling strcpy with an untrusted string as the source
 *              could cause a buffer overflow.
 * @kind path-problem
 * @id cpp/DangerousStrcpy
 * @problem.severity warning
 */

import cpp
import semmle.code.cpp.controlflow.Guards
private import semmle.code.cpp.rangeanalysis.RangeAnalysisUtils
import DataFlow::PathGraph
import comdb2.NetworkData
import comdb2.Sbuf2Untrusted

// Chase definitions until you find a comparison.
ComparisonOperation chaseDefsToComparison(Expr e) {
    result = e
    or
    exists(SsaDefinition def, Variable v |
        e = def.getAUse(v) and
        result = chaseDefsToComparison(def.getDefiningValue(v))
    )
}

// Chase definitions until you find a call to `strlen`.
StrlenCall chaseDefsToStrlen(Expr e) {
    result = e
    or
    exists(SsaDefinition def, Variable v |
        e = def.getAUse(v) and
        result = chaseDefsToStrlen(def.getDefiningValue(v))
    )
}

predicate guardedLen(GuardCondition guard, VariableAccess e) {
    exists(
        boolean branch, Expr len, BasicBlock block, SsaDefinition strDef, Variable str, StrlenCall call
    |
        relOpWithSwapAndNegate(chaseDefsToComparison(guard), len, _, Lesser(), _, branch) and
        call = chaseDefsToStrlen(len) and
        call.getStringExpr() = strDef.getAUse(str) and
        e = strDef.getAUse(str) and
        guard.controls(block, branch) and
        block.contains(e)
    )
}

class CopyFlow extends NetworkTaintFlow {
    override predicate isSink(DataFlow::Node sink) { copy_sink(sink.asExpr(), _) }
}

predicate copy_sink(Expr sink, string name) {
    exists(FunctionCall call, int argIndex |
        sink = call.getArgument(argIndex) and
        (
            callOrInvocation(call, "%strcpy%", name) and argIndex = 1
            or
            callOrInvocation(call, "%strcat%", name) and argIndex = 1
        ) and
        not guardedLen(_, sink)
    )
}

from CopyFlow c, DataFlow::PathNode source, DataFlow::PathNode sink, string name, Expr se
where
    c.hasFlowPath(source, sink) and
    se = sink.getNode().asExpr() and
    copy_sink(se, name)
select sink, source, sink, name + " with untrusted source from " + source

// // Original query, but using .matches to catch macros.
// from NetworkData sink, FunctionCall call, string name, int argIndex
// where sink = call.getArgument(argIndex)
//   and name = call.getTarget().getName()
//   and ((name.matches("%strcpy%") and argIndex = 1) or
//        (name.matches("%strcat%") and argIndex = 1))
//   and not guardedLen(_, sink)
// select sink, name + " with untrusted source from " + sink.getLinks() + "."
// //select ghe(sink), name + " with user-controlled source from " + sink.getGithubSources() + "."
