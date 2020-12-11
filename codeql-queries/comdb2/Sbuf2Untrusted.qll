private import comdb2.NetworkStores

class Sbuf2State extends NetworkDataStore {
  Sbuf2State() {
    exists(FunctionCall recv | recv.getTarget().getName().regexpMatch(".*sbuf2.*read") |
      exists(VariableAccess va | this.asExpr() = va |
        (
          va = recv.getArgument(0) or
          va = recv.getArgument(0).(AddressOfExpr).getOperand()
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
