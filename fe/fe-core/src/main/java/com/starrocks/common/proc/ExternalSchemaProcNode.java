// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;

import java.util.Arrays;
import java.util.List;

public class ExternalSchemaProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Field").add("Type").add("Null").add("Key")
            .add("Default").add("Extra")
            .build();

    private Table table;

    private static final String DEFAULT_STR = FeConstants.null_string;
    private static final String PARTITION_KEY = "partition key";

    public ExternalSchemaProcNode(Table table) {
        this.table = table;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(table);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<Column> schema = table.getFullSchema();
        List<String> partitionColumns = Lists.newArrayList();
        if (table.getType() == TableType.HIVE) {
            partitionColumns = ((HiveTable) table).getPartitionColumnNames();
        }

        for (Column column : schema) {
            String extraStr = partitionColumns.contains(column.getName()) ? PARTITION_KEY : "";
            List<String> rowList = Arrays.asList(column.getName(),
                    column.getType().canonicalName(),
                    column.isAllowNull() ? "Yes" : "No",
                    ((Boolean) column.isKey()).toString(),
                    DEFAULT_STR,
                    extraStr);
            result.addRow(rowList);
        }
        return result;
    }
}
