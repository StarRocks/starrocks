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

package com.starrocks.sql.automv.lifecycle;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.automv.qe.ColumnPlus;
import com.starrocks.sql.automv.util.ColumnDescription;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.starrocks.sql.automv.qe.ColumnPlus.BIGINT;
import static com.starrocks.sql.automv.qe.ColumnPlus.DATETIME;
import static com.starrocks.sql.automv.qe.ColumnPlus.VARCHAR;

public class QueryAuditEntry {
    public static final List<ColumnPlus> COLUMNS = collectColumns();
    @ColumnDescription(type = VARCHAR, len = 255)
    String catalog;
    @ColumnDescription(type = VARCHAR, len = 255)
    String db;
    @ColumnDescription(type = VARCHAR, len = 1048576)
    String stmt;
    @ColumnDescription(type = BIGINT)
    Long queryTime;
    @ColumnDescription(type = BIGINT)
    Long scanBytes;
    @ColumnDescription(type = BIGINT)
    Long scanRows;
    @ColumnDescription(type = BIGINT)
    Long returnRows;
    @ColumnDescription(type = BIGINT)
    Long cpuCostNs;
    @ColumnDescription(type = BIGINT)
    Long memCostBytes;
    @ColumnDescription(type = VARCHAR)
    List<String> candidateMVs;
    @ColumnDescription(type = VARCHAR)
    List<String> hitMvs;

    @ColumnDescription(type = DATETIME)
    private Timestamp timestamp;

    public QueryAuditEntry() {
    }

    private static List<ColumnPlus> collectColumns() {
        return Stream.of(QueryAuditEntry.class.getDeclaredFields())
                .filter(ColumnPlus::isAcceptable)
                .map(ColumnPlus::fieldToColumn)
                .collect(ImmutableList.toImmutableList());
    }

    public static List<ColumnPlus> getColumns() {
        return COLUMNS;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getStmt() {
        return stmt;
    }

    public void setStmt(String stmt) {
        this.stmt = stmt;
    }

    public Long getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(Long queryTime) {
        this.queryTime = queryTime;
    }

    public Long getScanBytes() {
        return scanBytes;
    }

    public void setScanBytes(Long scanBytes) {
        this.scanBytes = scanBytes;
    }

    public Long getScanRows() {
        return scanRows;
    }

    public void setScanRows(Long scanRows) {
        this.scanRows = scanRows;
    }

    public Long getReturnRows() {
        return returnRows;
    }

    public void setReturnRows(Long returnRows) {
        this.returnRows = returnRows;
    }

    public Long getCpuCostNs() {
        return cpuCostNs;
    }

    public void setCpuCostNs(Long cpuCostNs) {
        this.cpuCostNs = cpuCostNs;
    }

    public Long getMemCostBytes() {
        return memCostBytes;
    }

    public void setMemCostBytes(Long memCostBytes) {
        this.memCostBytes = memCostBytes;
    }

    public List<String> getCandidateMVs() {
        return candidateMVs;
    }

    public void setCandidateMVs(String candidateMVs) {
        this.candidateMVs = Arrays.asList(candidateMVs.split("\\s*,\\s*"));
    }

    public List<String> getHitMvs() {
        return hitMvs;
    }

    public void setHitMvs(String hitMvs) {
        this.hitMvs = Arrays.asList(hitMvs.split("\\s*,\\s*"));
    }
}