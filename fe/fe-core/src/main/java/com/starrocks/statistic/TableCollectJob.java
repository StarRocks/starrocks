// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;

import java.util.ArrayList;
import java.util.List;

public class TableCollectJob {
    private final AnalyzeJob analyzeJob;
    private final Database db;
    private final Table table;
    private final List<String> columns;
    private final List<Long> partitionIdList;

    public TableCollectJob(AnalyzeJob analyzeJob, Database db, Table table, List<String> columns) {
        this.analyzeJob = analyzeJob;
        this.db = db;
        this.table = table;
        this.columns = columns;
        this.partitionIdList =  new ArrayList<>();
    }

    public AnalyzeJob getAnalyzeJob() {
        return analyzeJob;
    }

    public Database getDb() {
        return db;
    }

    public Table getTable() {
        return table;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Long> getPartitionIdList() {
        return partitionIdList;
    }

    public void addPartitionId(Long partitionId) {
        partitionIdList.add(partitionId);
    }
}
