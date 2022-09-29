// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;

/**
 * interface is hard to serialized/deserialized using GSON
 * to simplify the implementation, the base class PEntryObject contains one data field called `id`.
 */
public class PEntryObject {
    @SerializedName(value = "i")
    protected long id;

    public PEntryObject(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PEntryObject)) {
            return false;
        }
        PEntryObject other = (PEntryObject) obj;
        return other.id == this.id;
    }
}