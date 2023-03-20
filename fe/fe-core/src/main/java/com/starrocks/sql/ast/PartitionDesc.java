// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.thrift.TTabletType;
import org.apache.commons.lang.NotImplementedException;

import java.util.List;
import java.util.Map;

public class PartitionDesc implements ParseNode {

    protected PartitionType type;

    public PartitionType getType() {
        return type;
    }

    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        throw new NotImplementedException();
    }

    public String toSql() {
        throw new NotImplementedException();
    }

    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId,
                                         boolean isTemp, boolean isExprPartition)
            throws DdlException {
        throw new NotImplementedException();
    }

    public String getPartitionName() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public boolean isSetIfNotExists() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public Map<String, String> getProperties() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public short getReplicationNum() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public DataProperty getPartitionDataProperty() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public Long getVersionInfo() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public TTabletType getTabletType() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public boolean isInMemory() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public StorageCacheInfo getStorageCacheInfo() throws NotImplementedException {
        throw new NotImplementedException();
    }
}