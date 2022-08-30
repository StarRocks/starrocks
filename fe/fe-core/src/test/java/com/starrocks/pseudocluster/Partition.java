package com.starrocks.pseudocluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Partition {
    private static final Logger LOG = LogManager.getLogger(Partition.class);

    public long dataSize;

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public long getDataSize() {
        return dataSize;
    }
}
