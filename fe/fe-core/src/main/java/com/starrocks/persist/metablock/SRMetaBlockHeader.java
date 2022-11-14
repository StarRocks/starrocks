// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist.metablock;

import com.google.gson.annotations.SerializedName;

public class SRMetaBlockHeader {
    @SerializedName(value = "n")
    private String name;
    @SerializedName(value = "nj")
    private int numJson;

    public SRMetaBlockHeader(String name, int numJson) {
        this.name = name;
        this.numJson = numJson;
    }

    public int getNumJson() {
        return numJson;
    }

    public String getName() {
        return name;
    }
}