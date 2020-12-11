import cpp

/**
 * A callback function that is registered with `net_register_handler`.
 * These callback functions are called with arguments containing
 * tainted data.
 *
 * The function signature (here marked with argument indices) is
 *
 *        typedef void NETFP(0 void *ack_handle, 1 void *usr_ptr, 2 char *fromhost,
 *                           3 int usertype, 4 void *dta, 5 int dtalen, uint8_t is_tcp);
 */
class NetCallbackFunction extends Function {
    NetCallbackFunction() {
        // The prototype for this query
        // int net_register_handler(netinfo_type *netinfo_ptr, int usertype,
        //                          char *name, NETFP func)
        exists(FunctionCall call, FunctionAccess access |
            call.getTarget().getName().matches("net_register_handler") and
            access = call.getArgument(3) and
            this = access.getTarget()
        )
    }

    Parameter getATaintedParameter() { result = this.getParameter(this.taintedParamIndex()) }

    /** Gets an index of a parameter that contains tainted data. */
    int taintedParamIndex() {
        // Argument 0 is `ack_state`, which is a struct containing
        // user-controlled fields, such as `seqnum` and `needack`.
        result = 0 or
        result = 3 or
        result = 4 or
        result = 5
    }

    // Argument 1 is `usrptr`. It is used to store persistent
    // data between calls to a callback function. The `usrptr` is stored
    // as a field of `netinfo_ptr` and it is initialized in file.c (search
    // for calls to `net_set_usrptr`.)
    int usrptrParamIndex() { result = 1 }
}
