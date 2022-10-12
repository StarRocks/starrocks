// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;


public class ExternalTableProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Nodes")
            .build();

    public static final String SCHEMA = "schema";

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
        } else {
            throw new AnalysisException("Not implemented yet: " + entryName);
        }
    }
}
