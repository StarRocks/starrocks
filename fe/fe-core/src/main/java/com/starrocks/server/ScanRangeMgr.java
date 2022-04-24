package com.starrocks.server;

import com.starrocks.spi.ScanRangerProvider;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ScanRangeMgr {
    private final ConcurrentHashMap<String, ScanRangerProvider> providers = new ConcurrentHashMap<>();

    public void addScanProvider(String catalogName, ScanRangerProvider provider) {
    }

    public void removeMetadata() {
    }

    private Optional<ScanRangerProvider> getOptionalProvider(String catalogName) {
        return Optional.empty();
    }

    List<TScanRangeLocations> getScanRangeLocations(String catalogName, long maxScanRangeLength) {
        Optional<ScanRangerProvider> provider = getOptionalProvider(catalogName);
        if (!provider.isPresent()) {
            // throw exception
            return Collections.emptyList();
        }

        return provider.get().getScanRangeLocations(maxScanRangeLength);
    }
}
