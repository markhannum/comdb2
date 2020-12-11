/**
 * @name Network Controlled Abort
 * @description Abort controlled by network data
 * @kind path-problem
 * @id cpp/NetworkControlledAbort
 * @problem.severity warning
 */

import comdb2.NetworkData
import DataFlow::PathGraph

Stmt nonIfParent(Stmt s) {
    result = s.getParent() and
    not s instanceof IfStmt
}

predicate abort_sink(DataFlow::Node sink, FunctionCall abort) {
    exists(Expr sink_e, IfStmt i |
        sink_e = sink.asExpr() and
        sink_e.getParent*() = i.getCondition() and
        abort.getTarget().getName().regexpMatch("exit|abort") and
        nonIfParent*(abort.getEnclosingStmt()) = i.getThen() and
        not abort.isInMacroExpansion()
    )
}

class AbortFlow extends NetworkTaintFlow {
    override predicate isSink(DataFlow::Node sink) { abort_sink(sink, _) }
}

from FunctionCall abort, DataFlow::PathNode source, DataFlow::PathNode sink, AbortFlow c
where
    // PathGraph setup
    c.hasFlowPath(source, sink) and
    // query
    abort_sink(sink.getNode(), abort)
select sink, source, sink, "This program abort in " + abort + " is conditional on $@.", source,
    "network data from " + source
// //
// // Original query
// //
// from IfStmt i, FunctionCall abort, NetworkData sink
// where sink.getParent*() = i.getCondition()
//   and abort.getTarget().getName().regexpMatch("exit|abort")
//   and nonIfParent*(abort.getEnclosingStmt()) = i.getThen()
//   and not abort.isInMacroExpansion()
// select abort, "This program abort is conditional on $@.", sink.getASource() as source, "network data from " + source
