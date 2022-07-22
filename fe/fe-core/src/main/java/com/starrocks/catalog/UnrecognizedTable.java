// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.common.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;

public class UnrecognizedTable extends Table {
    private static final Logger LOG = LogManager.getLogger(UnrecognizedTable.class);

    public UnrecognizedTable() {
        super(TableType.UNRECOGNIZED);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        String json = Text.readString(in);
        LOG.error("Unrecognized table metadata {}", json);
    }
}
