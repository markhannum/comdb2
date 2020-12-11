/**
 * @name Dangerous arithmetic
 * @description Integer overflow in an arithmetic expression that depends
 *              on an untrusted value could be exploited by an attacker,
 *              for example by bypassing a size check or by triggering
 *              a buffer overflow.
 * @kind path-problem
 * @id cpp/DangerousArithmetic
 * @problem.severity warning
 */

import cpp
import semmle.code.cpp.dataflow.DataFlow
import semmle.code.cpp.controlflow.Guards
import semmle.code.cpp.dataflow.TaintTracking
import semmle.code.cpp.rangeanalysis.SimpleRangeAnalysis
import DataFlow::PathGraph
import comdb2.NetworkData
import comdb2.Utility

class ArithmeticFlow extends NetworkTaintFlow {
    override predicate isSanitizer(DataFlow::Node barrier) {
        super.isSanitizer(barrier) or
        barrier.asParameter() = any(NetCallbackFunction f).getParameter(0) or
        barrier.asExpr() = any(NetCallbackFunction f).getParameter(0).getAnAccess()
    }

    // Don't flow from sink to sink
    override predicate isSanitizerOut(DataFlow::Node sanitizer) {
        arithmetic_sink(sanitizer.asExpr())
    }

    override predicate isSink(DataFlow::Node sink) { arithmetic_sink(sink.asExpr()) }
}

/** Gets the parent of `e`, if the parent is an `AddExpr`. */
AddExpr addExprParent(Expr e) { result.getAnOperand() = e }

class OverflowExpr extends Expr {
    OverflowExpr() {
        not this.getFullyConverted().getType().getUnspecifiedType() instanceof PointerType and
        not exists(ComparisonOperation eq | eq.getAnOperand() = this) and
        not exists(FunctionCall call |
            (
                callOrInvocation(call, "htonl", _) or
                callOrInvocation(call, "ntohl", _)
            ) and
            this = call.getArgument(0)
        ) and
        not exists(MacroInvocation mi, string nm |
            nm = mi.getMacroName() and
            (
                nm = "ntohl"
                or
                nm = "htonl"
                or
                // Pure use of htonl / ntohl
                nm = "ack_info_to_cpu"
                or
                nm = "ack_info_from_cpu"
                or
                // sizes checked
                nm = "ack_info_size"
                or
                // bounds checked
                nm = "ack_info_from_host"
                or
                // casts with size limits
                nm = "P_TO_UINT16"
                or
                nm = "P_TO_UINT32"
                or
                // bit shifting with unsigned types
                nm = "compute_bit2idx"
                or
                // bit masking
                nm = "left_bits"
                or
                nm = "least_bit"
            ) and
            this = mi.getAGeneratedElement()
        ) and
        (
            convertedExprMightOverflow(this) or
            addExprParent(this) instanceof OverflowExpr
        ) and
        (
            not inSystemMacroExpansion(this) and
            // Avoid double-counting: don't include all the conversions
            not this instanceof Conversion
        )
    }
}

predicate arithmetic_sink(Expr se) {
    se instanceof OverflowExpr and
    not se.getFile().getRelativePath().matches("%endian%")
}

//
// Query for PathGraph
from ArithmeticFlow c, DataFlow::PathNode source, DataFlow::PathNode sink, Expr se
where
    c.hasFlowPath(source, sink) and
    se = sink.getNode().asExpr()
select sink, source, sink,
    "Potential overflow (conversion: " + se.getType().getUnspecifiedType().toString() + " -> " +
        se.getFullyConverted().getType().getUnspecifiedType().toString() + ").  Source: $@",
        source, fileLineColumn(source.getNode())

        //
// Original query using sink.getLinks()
//
// from NetworkData sink
// where sink instanceof OverflowExpr
// and not sink.getFile().getRelativePath().matches("%endian%")
// and exists (sink.getASource())
// select sink,
//   "Potential overflow (conversion: " + sink.getType().getUnspecifiedType().toString() +
//   " -> " + sink.getFullyConverted().getType().getUnspecifiedType().toString() +
//   ") with user-controlled source from " +  sink.getLinks() + "."
//
//
//select ghe(sink),
//  "Potential overflow (conversion: `" + sink.getType().getUnspecifiedType().toString() +
//  "` -> `" + sink.getFullyConverted().getType().getUnspecifiedType().toString() +
//  "`) with user-controlled source from " +  sink.getGithubSources() + "."
