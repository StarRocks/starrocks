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

    public boolean keyMatch(PEntryObject pEntryObject) {
        return (pEntryObject instanceof PEntryObject) && (pEntryObject.id == this.id);
    }
}