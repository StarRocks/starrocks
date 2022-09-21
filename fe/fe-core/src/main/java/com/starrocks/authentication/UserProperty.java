// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;

import com.google.gson.annotations.SerializedName;

public class UserProperty {
    @SerializedName(value = "m")
    private long maxConn = 100;

    public long getMaxConn() {
        return maxConn;
    }
}
