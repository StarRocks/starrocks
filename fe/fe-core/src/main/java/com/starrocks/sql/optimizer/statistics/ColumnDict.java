// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;

import java.util.HashMap;

public final class ColumnDict {
    private final HashMap<String, Integer> dict;
    private final long version;

    public ColumnDict(HashMap<String, Integer> dict, long version) {
        Preconditions.checkState(dict.size() > 0 && dict.size() <= 256);
        this.dict = dict;
        this.version = version;
    }

    public HashMap<String, Integer> getDict() {
        return dict;
    }

    public long getVersion() {
        return version;
    }
}
