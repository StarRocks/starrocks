// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Catalog implements Writable {
    private final String name;

    public Catalog(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }
}
