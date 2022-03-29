package com.starrocks.spi;

public interface Connector {
    Metadata getMetadata();

    ScanRangerProvider getScanRanger();

    RuleProvider getRuleProvider();
}
