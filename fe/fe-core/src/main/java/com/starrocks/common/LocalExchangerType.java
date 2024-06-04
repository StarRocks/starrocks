package com.starrocks.common;

import com.starrocks.thrift.TLocalExchangerType;

public enum LocalExchangerType {
    PASS_THROUGH("pass_through", TLocalExchangerType.PASSTHROUGH),
    DIRECT("direct", TLocalExchangerType.DIRECT);

    private final String name;

    public TLocalExchangerType getThriftType() {
        return thriftType;
    }

    private final TLocalExchangerType thriftType;

    LocalExchangerType(String name, TLocalExchangerType tLocalExchangerType) {
        this.name = name;
        this.thriftType = tLocalExchangerType;
    }
}
