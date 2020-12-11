/**
 * @name Potentially infinite (or short) loop
 * @description Using a loop counter against a bound of a larger type may result in an infinite loop,
 *              or in a loop that completes fewer iterations than intended. 
 * @kind problem
 * @id cpp/PotentiallyInfiniteLoop
 * @problem.severity warning
 */


import cpp

predicate isUnsigned(Type t) {
	t.getUnspecifiedType().(IntegralType).isUnsigned()
}

from Loop l, RelationalOperation op, Type lt, Type gt, string prefix
where op = l.getCondition()
  and lt = op.getLesserOperand().getType()
  and gt = op.getGreaterOperand().getType()
  and lt.getSize() < gt.getSize()
  and not exists(op.getAnOperand().getValue())
  and not op.getAnOperand().getType() instanceof PointerType
  and if (isUnsigned(gt) implies isUnsigned(lt)) then
  	prefix = "Potentially infinite loop: "
  else
  	prefix = "Potentially short-running loop: "
select op, prefix + "its condition compares a " + lt.getSize() * 8 +  "-bit value of type `" + lt + "`"
  + " with a " + gt.getSize() * 8 + "-bit loop bound of type `" + gt + "`."
