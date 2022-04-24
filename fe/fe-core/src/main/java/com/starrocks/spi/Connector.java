package com.starrocks.spi;

public interface Connector {
    Metadata getMetadata();

    ScanRangerProvider getScanRangeProvider();

    RuleProvider getRuleProvider();
}
