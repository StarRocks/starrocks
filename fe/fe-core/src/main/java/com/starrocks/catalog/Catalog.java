// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Catalog implements Writable {
    private final String name;

    private final String comment;

    public Catalog(String name, String comment) {
        this.name = name;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    /**
     * Fill BaseProcResult with different properties in child resources
     * CatalogMgr.CATALOG_PROC_NODE_TITLE_NAMES format:
     * | Name | Type | Comment |
     */
    public abstract void getProcNodeData(BaseProcResult result);
}
