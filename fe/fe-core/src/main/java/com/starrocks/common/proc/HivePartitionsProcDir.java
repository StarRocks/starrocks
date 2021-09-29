// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.external.hive.Utils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions
 * show partitions' detail info within a table
 */
public class HivePartitionsProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(HivePartitionsProcDir.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionName")
            .build();

    private final HiveTable hiveTable;

    public HivePartitionsProcDir(HiveTable hiveTable) {
        this.hiveTable = hiveTable;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(hiveTable);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        // partitionColumns is empty means table is unPartitioned
        if (hiveTable.getPartitionColumnNames().isEmpty()) {
            result.addRow(Lists.newArrayList(hiveTable.getHiveTable()));
        } else {
            try {
                for (PartitionKey partitionKey : hiveTable.getPartitionKeys().keySet()) {
                    result.addRow(Lists.newArrayList(FileUtils.makePartName(hiveTable.getPartitionColumnNames(),
                            Utils.getPartitionValues(partitionKey))));
                }
            } catch (DdlException e) {
                LOG.warn("get hive partitions failed", e);
                throw new AnalysisException("get hive partitions failed: " + e.getMessage());
            }
        }

        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String indexName) throws AnalysisException {
        return null;
    }
}
