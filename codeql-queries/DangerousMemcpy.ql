/**
 * @name Dangerous memcpy
 * @description Calling memcpy with an untrusted value as the size
 *              could cause a buffer overflow.
 * @kind path-problem
 * @id cpp/DangerousMemcpy
 * @problem.severity warning
 */

import cpp
import semmle.code.cpp.controlflow.Guards
import semmle.code.cpp.dataflow.TaintTracking
private import semmle.code.cpp.rangeanalysis.RangeAnalysisUtils
import DataFlow::PathGraph
import comdb2.NetworkData
import comdb2.Utility

predicate guardedVar(GuardCondition guard, VariableAccess e) {
    exists(boolean branch, SsaDefinition def, Variable v, BasicBlock block |
        relOpWithSwapAndNegate(guard, def.getAUse(v), _, Lesser(), _, branch) and
        e = def.getAUse(v) and
        guard.controls(block, branch) and
        block.contains(e)
    )
}

predicate memcopy_sink(Expr sink_exp, string name) {
    exists(FunctionCall call, int argIndex |
        sink_exp = call.getArgument(argIndex) and
        sink_exp.getType().getUnspecifiedType() instanceof IntegralType and
        (
            callOrInvocation(call, "%memcpy%", name) and argIndex = 2
            or
            callOrInvocation(call, "%alloc%", name) and argIndex = 0
        ) and
        not guardedVar(_, sink_exp)
    )
}

class MemcopyFlow extends NetworkTaintFlow {
    override predicate isSink(DataFlow::Node sink) { memcopy_sink(sink.asExpr(), _) }
}

from MemcopyFlow c, DataFlow::PathNode source, DataFlow::PathNode sink, Expr sink_exp, string name
where
    // Flow path setup
    c.hasFlowPath(source, sink) and
    sink_exp = sink.getNode().asExpr() and
    // Main query
    memcopy_sink(sink_exp, name)
select sink, source, sink,
    name + " with untrusted size (type: " + sink_exp.getType().getUnspecifiedType() + ") from $@.", 
    source, source.toString() + " [" + fileLineColumn(source.getNode()) + "]"
//
// Original query
//  from NetworkData sink, FunctionCall call, string name, int argIndex
// where sink = call.getArgument(argIndex)
//   and sink.getType().getUnspecifiedType() instanceof IntegralType
//   and name = call.getTarget().getName()
//   and ((name.matches("%memcpy%") and argIndex = 2) or
//        (name.matches("%alloc%") and argIndex = 0))
//   and not guardedVar(_, sink)
// select sink, name + " with untrusted size (type: "  + sink.getType().getUnspecifiedType() + ") from " + sink.getLinks() + "."
