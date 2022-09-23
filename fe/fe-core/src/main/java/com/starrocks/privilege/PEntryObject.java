// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;

public class PEntryObject {
    @SerializedName(value = "i")
    protected long id;
    public PEntryObject(long id) {
        this.id = id;
    }

    boolean isSame(PEntryObject pEntryObject) {
        return (pEntryObject instanceof PEntryObject) && (pEntryObject.id == this.id);
    }
}