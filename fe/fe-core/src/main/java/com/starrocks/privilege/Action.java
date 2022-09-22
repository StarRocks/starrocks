// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

public class Action {
    private static final short MAX_ID = 64;
    @SerializedName(value = "i")
    private short id;
    @SerializedName(value = "n")
    private String name;

    public Action(short id, String name) {
        Preconditions.checkState(id < MAX_ID,
                String.format("illegal action %s value %d >= %d", name, id, MAX_ID));
        this.id = id;
        this.name = name;
    }

    public short getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
