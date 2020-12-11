private import comdb2.NetworkStores

private class LuaState extends NetworkDataStore {
	LuaState() {
		this.asParameter() = taintedVar and
		taintedVar.getType().getUnspecifiedType().(PointerType).getBaseType().hasName("lua_State")
	}
}