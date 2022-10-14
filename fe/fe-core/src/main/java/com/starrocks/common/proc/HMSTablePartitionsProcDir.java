// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions
 * show partitions' detail info within a table
 */
public class HMSTablePartitionsProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(HMSTablePartitionsProcDir.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionName")
            .build();

    private final HiveMetaStoreTable hmsTable;

    public HMSTablePartitionsProcDir(HiveMetaStoreTable hmsTable) {
        this.hmsTable = hmsTable;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(hmsTable);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        if (hmsTable.isUnPartitioned()) {
            result.addRow(Lists.newArrayList(hmsTable.getTableName()));
        } else {
            try {
                List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .listPartitionNames(hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName());
                for (String partitionName : partitionNames) {
                    result.addRow(Lists.newArrayList(partitionName));
                }
            } catch (StarRocksConnectorException e) {
                LOG.warn("Get table partitions failed", e);
                throw new AnalysisException("get table partitions failed: " + e.getMessage());
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
