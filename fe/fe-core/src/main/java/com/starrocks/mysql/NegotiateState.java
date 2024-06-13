package com.starrocks.mysql;

public enum NegotiateState {
    OK("ok"),
    READ_FIRST_AUTH_PKG_FAILED("read first auth package failed"),
    ENABLE_SSL_FAILED("enable ssl failed"),
    READ_SSL_AUTH_PKG_FAILED("read ssl auth package failed"),
    NOT_SUPPORTED_AUTH_MODE("not supported auth mode"),
    KERBEROS_HANDSHAKE_FAILED("kerberos handshake failed"),
    KERBEROS_PLUGIN_NOT_LOADED("kerberos plugin not loaded"),
    AUTHENTICATION_FAILED("authentication failed"),
    SET_DATABASE_FAILED("set database failed");

    private final String msg;
    NegotiateState(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}
