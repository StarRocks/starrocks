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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;


public class ExternalTableProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Nodes")
            .build();

    public static final String SCHEMA = "schema";
    public static final String PARTITIONS = "partitions";

    // TODO implementing show proc external table partitions
    private static final ImmutableList<String> CHILDREN_NODES = new ImmutableList.Builder<String>()
            .add(SCHEMA)
            .build();

    private Table table;

    public ExternalTableProcDir(Table table) {
        this.table = table;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);
        for (String name : CHILDREN_NODES) {
            result.addRow(Lists.newArrayList(name));
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String entryName) throws AnalysisException {
        Preconditions.checkNotNull(table);

        if (Strings.isNullOrEmpty(entryName)) {
            throw new AnalysisException("Entry name is null");
        }

        if (entryName.equals(SCHEMA)) {
            return new ExternalSchemaProcNode(table);
        } else if (entryName.equals(PARTITIONS)) {
            if (table instanceof PaimonTable) {
                PaimonTable paimonTable = (PaimonTable) table;
                PartitionType partitionType = paimonTable.isUnPartitioned() ?
                        PartitionType.UNPARTITIONED : PartitionType.LIST;
                return new PaimonTablePartitionsProcDir(table, partitionType);
            } else {
                throw new AnalysisException("table is not support: " + entryName);
            }
        } else {
            throw new AnalysisException("Not implemented yet: " + entryName);
        }
    }
}
