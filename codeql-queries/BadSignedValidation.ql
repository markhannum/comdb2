  
/**
 * @name No sign validation before access of signed variable
 * @description No sign validation before access of signed variable
 * @kind problem
 * @problem.severity warning
 * @precision low
 * @id cpp/BadSignedValidation
 */

/*
 * Note: This query appears useful for exploratory purposes
 * but also highlights results that are not necessarily problems.
 */

import cpp
import semmle.code.cpp.controlflow.Guards

predicate isSigned(Type t) {
	t.getUnspecifiedType().(IntegralType).isSigned()
}

from RelationalOperation e, Variable v, VariableAccess unsignedUse
where isSigned(e.getLeftOperand().getType())
  and isSigned(e.getRightOperand().getType())
  and e.getGreaterOperand() = v.getAnAccess()
  and unsignedUse = v.getAnAccess()
  and e.(GuardCondition).controls(unsignedUse.getBasicBlock(), false)
  and unsignedUse.getParent*() instanceof Call
  and not isSigned(unsignedUse.getActualType())
  // If there's a guard like `if (x < 0)`, we're fine on the 'else' path.
  and not exists(RelationalOperation lbCheck | lbCheck.getLesserOperand() = v.getAnAccess() |
  	lbCheck.(GuardCondition).controls(unsignedUse.getBasicBlock(), false)
  )
  // If there's a guard like `if (x >= 0)`, we're fine on the 'then' path.
  and not exists(RelationalOperation posCheck | posCheck.getGreaterOperand() = v.getAnAccess() |
  	posCheck.(GuardCondition).controls(unsignedUse.getBasicBlock(), true)
  )
  and not e = any(Loop l).getCondition()
select unsignedUse, "Variable $@ accessed without sign validation", v, v.getName()
