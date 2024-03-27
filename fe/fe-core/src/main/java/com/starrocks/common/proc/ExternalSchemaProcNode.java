// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;

import java.util.Arrays;
import java.util.List;

public class ExternalSchemaProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Field").add("Type").add("Null").add("Key")
            .add("Default").add("Extra").add("Comment")
            .build();

    private Table table;

    private static final String DEFAULT_STR = FeConstants.NULL_STRING;
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
        List<String> partitionColumns = table.getPartitionColumnNames();

        for (Column column : schema) {
            String extraStr = partitionColumns.contains(column.getName()) ? PARTITION_KEY : "";
            List<String> rowList = Arrays.asList(column.getName(),
                    column.getType().canonicalName(),
                    column.isAllowNull() ? "Yes" : "No",
                    ((Boolean) column.isKey()).toString(),
                    DEFAULT_STR,
                    extraStr,
                    column.getComment());
            result.addRow(rowList);
        }
        return result;
    }
}
