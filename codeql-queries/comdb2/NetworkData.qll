import cpp
import semmle.code.cpp.dataflow.DataFlow
import semmle.code.cpp.dataflow.TaintTracking
import semmle.code.cpp.controlflow.SSA
import DataFlow
import comdb2.NetworkStores
import semmle.code.cpp.models.interfaces.DataFlow

abstract class NetworkTaintFlow extends TaintTracking::Configuration {
    NetworkTaintFlow() { this = "NetworkTaintFlow" }

    // The source is common and  implemented here.
    override predicate isSource(DataFlow::Node source) { source instanceof NetworkDataStore }

    // The sinks are specific, implement in subclass
    // override predicate isSink(DataFlow::Node sink) ...
    override predicate isAdditionalTaintStep(DataFlow::Node source, DataFlow::Node sink) {
        // If a struct is network data, any member is also network data.
        source.asExpr() = sink.asExpr().(VariableAccess).getQualifier()
        or
        // Array access
        source.asExpr() = sink.asExpr().(ArrayExpr).getArrayBase()
        or
        // If a pointer is network data, then so is pointer arithmetic involving it.
        source.asExpr() = sink.asExpr().(PointerArithmeticOperation).getAnOperand()
        or
        // When a non-reassigned pointer is tainted, other uses are tainted.
        exists(SsaDefinition def, LocalScopeVariable var |
            var.getType().getUnspecifiedType() instanceof PointerType and
            source.asExpr() = def.getAUse(var) and
            sink.asExpr() = def.getAUse(var) and
            // Ensure source != sink and direction is unique
            source.asExpr().getASuccessor+() = sink.asExpr()
        )
        or
        // Unpacking a tainted data stream produces a tainted protobuf message
        exists(FunctionCall call | call.getTarget().hasName("protobuf_c_message_unpack") |
            source.asExpr() = call.getArgument(3) and
            sink.asExpr() = call
        )
    }

    override predicate isSanitizer(DataFlow::Node barrier) {
        barrier.asExpr() = any(FunctionCall fc | fc.getTarget().hasName("comdb2_free")).getAnArgument()
    }

    cached
    predicate reaches(DataFlow::Node source, DataFlow::Node sink) { this.hasFlow(source, sink) }

    predicate explore(Function f, DataFlow::Node sink, DataFlow::Node source) {
        this.reaches(source, sink) and
        f = sink.getFunction() and
        f.getName().regexpMatch(".*__unpack|get_remote_input")
    }
}

// Find direct and indirect ssa accesses that originate with a Parameter
VariableAccess ssaAccess(Parameter p) {
    // ssa access of a Parameter
    result = p.getAnAccess()
    or
    // access of an ssa initialized from another
    result = any(SsaDefinition def | def.getAnUltimateDefinition(_) = ssaAccess(p)).getAUse(_)
}

// Global effect: Extension point for dataflow library
// XX: in stdlib now, try removing
class MemcopyFunction extends DataFlowFunction {
    MemcopyFunction() {
        exists(FunctionCall c |
            callOrInvocation(c, "%memcpy%", _) and
            this = c.getTarget()
        )
    }

    override predicate hasDataFlow(FunctionInput input, FunctionOutput output) {
        input.isParameter(1) and output.isParameterDeref(0)
    }
}

class DerefCopyFunction extends DataFlowFunction {
    Parameter source;
    Parameter dest;

    // 80 results
    DerefCopyFunction() {
        // XX: potentially, use two SsaDefinitions to narrow this
        // Find `*dest = source`
        this = source.getFunction() and
        this = dest.getFunction() and
        exists(Assignment a | a.getRValue() = source.getAnAccess() |
            a.getLValue().(PointerDereferenceExpr).getOperand() = dest.getAnAccess()
        )
    }

    override predicate hasDataFlow(FunctionInput input, FunctionOutput output) {
        input.isParameter(source.getIndex()) and output.isParameterDeref(dest.getIndex())
    }
}

predicate dff_1(DataFlowFunction f, FunctionInput input, FunctionOutput output) {
    // 1
    f.(Sbuf2ArgCopyFunc).hasDataFlow(input, output)
    // // 80
    // f.(DerefCopyFunction).hasDataFlow(input, output)
    // // 13
    // f.(MemcopyFunction).hasDataFlow(input, output)
    // 206
    // f.hasDataFlow(input, output)
    // 0 for DFF, 80 for Function
    // f instanceof MemcopyFunction
}

predicate callTransfers(DataFlowFunction dff, FunctionCall c, Expr a, Expr b) {
    c.getTarget() = dff and
    exists(FunctionInput input, FunctionOutput output, int source_index, int dest_index |
        dff.hasDataFlow(input, output) and
        input.isParameter(source_index) and
        output.isParameterDeref(dest_index) and
        a = c.getArgument(source_index) and
        (
            b = c.getArgument(dest_index) or
            b = c.getArgument(dest_index).(AddressOfExpr).getOperand()
        )
    )
}

class SsaCopyFunction extends DataFlowFunction {
    Parameter source;
    Parameter dest;

    SsaCopyFunction() {
        this = source.getFunction() and
        this = dest.getFunction() and
        exists(DataFlowFunction df, VariableAccess vd |
            vd.getQualifier*() = ssaAccess(dest) and
            callTransfers(df, _, ssaAccess(source), vd)
        )
    }

    override predicate hasDataFlow(FunctionInput input, FunctionOutput output) {
        input.isParameter(source.getIndex()) and output.isParameterDeref(dest.getIndex())
    }
}

class Sbuf2ArgCopyFunc extends DataFlowFunction {
    Sbuf2ArgCopyFunc() { this.hasName("sbuf2fread_int") }

    override predicate hasDataFlow(FunctionInput input, FunctionOutput output) {
        input.isParameter(3) and output.isParameterDeref(0)
    }
}


bindingset[pattern]
predicate callOrInvocation(FunctionCall c, string pattern, string srcname) {
    (
        // Simple function call
        c.getTarget().getName().matches(pattern)
        or
        // Match A in both `A(...)` and `A(...) ? B : C`
        exists(MacroInvocation mi, Expr e |
            mi.getMacro().getName().matches(pattern) and
            mi.getExpr() = e and
            (
                c = e.(FunctionCall) or
                c = e.(ConditionalExpr).getCondition()
            )
        )
    ) and
    srcname = c.getTarget().getName()
}
