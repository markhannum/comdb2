/**
 * @name Dereference of missing data
 * @description Network packets may maliciously omit certain parts,
 *                leading to NULL values that need to be guarded before dereferencing.
 * @kind path-problem
 * @id cpp/DerefOfMissingData
 * @problem.severity warning
 */

import cpp
import semmle.code.cpp.dataflow.DataFlow
import semmle.code.cpp.dataflow.TaintTracking
import semmle.code.cpp.controlflow.Guards
import DataFlow::PathGraph
import semmle.code.cpp.controlflow.Dominance as Dominance


class NetworkHandler extends Function {
    NetworkHandler() {
        exists(FunctionCall call |
            call.getTarget().hasName("net_register_handler") and
            // Function prototype has changed. 
            // TODO: find argument that is of type *function, use that index.
            call.getArgument(3).(FunctionAccess).getTarget() = this
        )
    }

    Parameter getATaintedParam() {
        exists(int i | i = 0 or i in [3 .. 5] | result = this.getParameter(i))
    }
}

class NullCheckConfig extends TaintTracking::Configuration {
    NullCheckConfig() { this = "NullCheckConfig" }

    override predicate isSource(DataFlow::Node source) {
        source.asParameter() = any(NetworkHandler f).getATaintedParam()
    }

    override predicate isSink(DataFlow::Node sink) { dereferenced(sink.asExpr()) }

    override predicate isSanitizer(DataFlow::Node barrier) {
        exists(VariableAccess va, VariableAccess check, GuardCondition guard, boolean sense |
            guard = check and sense = true
            or
            guard = check.getParent() and guard instanceof EQExpr and sense = false
            or
            guard = check.getParent() and guard instanceof NEExpr and sense = true
        |
            va.getTarget() = check.getTarget() and
            guard.controls(va.getBasicBlock(), sense) and
            va = barrier.asExpr()
        )
        or
        exists(VariableAccess prev | strictlyDominates(prev, barrier.asExpr()) |
            dereferenced(prev) and
            prev.getTarget().getAnAccess() = barrier.asExpr()
        )
    }
}

predicate query(DataFlow::Node source, DataFlow::Node sink) {
    // Find flow from source -> sink
    any(NullCheckConfig c).hasFlow(source, sink) and
    // find the last sink not dominated by another _sink_
    not exists(DataFlow::Node previous | any(NullCheckConfig c).hasFlow(source, previous) |
        Dominance::strictlyDominates(previous.asExpr(), sink.asExpr())
    )
}

from DataFlow::PathNode source, DataFlow::PathNode sink, NullCheckConfig c
where
    c.hasFlowPath(source, sink) and
    query(source.getNode(), sink.getNode())
select sink, source, sink,
    source.getNode().getFunction() + "'s " + source +
        " parameter should be checked for NULL, as it is dereferenced $@.", sink,
    " in " + sink.getNode().getFunction()
//
// // Original problem query
// //
// from DataFlow::Node source
// //select ghn(source), "`" + source.getFunction() + "`'s `" + source + "` parameter should be checked for `NULL`, as it is dereferenced at " + strictconcat(DataFlow::Node sink | query(source, sink) | ghn(sink).fullString(), ", ") + "."
// select source,
//     source.getFunction() + "'s " + source +
//         " parameter should be checked for NULL, as it is dereferenced $@.",
//     any(DataFlow::Node sink | query(source, sink)) as sink, " in " + sink.getFunction()
