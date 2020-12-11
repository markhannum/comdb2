import cpp
import semmle.code.cpp.dataflow.DataFlow

import comdb2.NetCallbackFunction

/**
 * A dataflow node at which network data is stored to a variable.
 */
abstract class NetworkDataStore extends DataFlow::Node {
	Variable taintedVar;
	Variable getTaintedVar() { result = taintedVar }
}

/**
 * When a function is registered as a network handler, it will be called
 * with network data in some of its arguments.
 */
class UdpHandlerStore extends NetworkDataStore {
	UdpHandlerStore() {
		exists(NetCallbackFunction f | taintedVar = f.getATaintedParameter()) and
		this.asParameter() = taintedVar
	}
}

/**
 * When a pointer variable or the address of a variable is passed to one
 * of the `recv` functions, network data will be written to it.
 */
class RecvStore extends NetworkDataStore {
	RecvStore() {
		exists(FunctionCall recv | recv.getTarget().getName().regexpMatch("recv|recvfrom|recvmsg") |
			exists(VariableAccess va | this.asExpr() = va |
				(
					va = recv.getArgument(1) or
					va = recv.getArgument(1).(AddressOfExpr).getOperand()
				) and
				(
					// The tainted variable is either the one passed to `recv`, or a pointer
					// initialized to point to the same location.
					taintedVar = va.getTarget() or
					taintedVar.getInitializer().getExpr() = va.getTarget().getAnAccess()
				)
			)
		)
	}
}

/** Treat aliases to local variables as taint source.

In particular, identify `buff` in code of the form
```
uint8_t buff[1024] = {0};
ack_info *info = (ack_info *)buff;
```
 */
class AliasedStackBuffer extends NetworkDataStore {
    AliasedStackBuffer() {
        exists(VariableAccess va |
            va = taintedVar.getInitializer().getExpr() and
            va.getTarget() = any(NetworkDataStore n).getTaintedVar() and
            va = this.asExpr() and
            // Restrict to assignments and use the lhs as source
            not exists(Parameter p | va.getTarget() = p)
        )
    }
}
