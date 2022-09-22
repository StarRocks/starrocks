// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist.metablock;

import com.google.gson.annotations.SerializedName;

public class SRMetaBlockFooter {
    @SerializedName(value = "c")
    private long checksum;

    public SRMetaBlockFooter(long checksum) {
        this.checksum = checksum;
    }

    public long getChecksum() {
        return checksum;
    }
}
