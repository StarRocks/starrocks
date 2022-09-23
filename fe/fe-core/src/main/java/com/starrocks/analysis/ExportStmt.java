// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ExportStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.StatementBase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// EXPORT statement, export data to dirs by broker.
//
// syntax:
//      EXPORT TABLE tablename [PARTITION (name1[, ...])]
//          [(col1, col2[, ...])]
//          TO 'export_target_path'
//          [PROPERTIES("key"="value")]
//          WITH BROKER 'broker_name' [( $broker_attrs)]
public class ExportStmt extends StatementBase {

    private static final String INCLUDE_QUERY_ID_PROP = "include_query_id";

    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_FILE_NAME_PREFIX = "data_";

    private static final Set<String> VALID_SCHEMES = Sets.newHashSet(
            "afs", "bos", "hdfs", "oss", "s3a", "cosn", "viewfs", "ks3");

    private TableName tblName;
    private List<String> partitions;
    private List<String> columnNames;
    // path should include "/"
    private String path;
    private String fileNamePrefix;
    private final BrokerDesc brokerDesc;
    private Map<String, String> properties = Maps.newHashMap();
    private String columnSeparator;
    private String rowDelimiter;
    private boolean includeQueryId = true;

    private TableRef tableRef;
    private long exportStartTime;

    public ExportStmt(TableRef tableRef, List<String> columnNames, String path,
                      Map<String, String> properties, BrokerDesc brokerDesc) {
        this.tableRef = tableRef;
        this.columnNames = columnNames;
        this.path = path.trim();
        if (properties != null) {
            this.properties = properties;
        }
        this.brokerDesc = brokerDesc;
        this.columnSeparator = DEFAULT_COLUMN_SEPARATOR;
        this.rowDelimiter = DEFAULT_LINE_DELIMITER;
        this.includeQueryId = true;
    }

    public long getExportStartTime() {
        return exportStartTime;
    }

    public void setExportStartTime(long exportStartTime) {
        this.exportStartTime = exportStartTime;
    }

    public TableName getTblName() {
        return tblName;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getPath() {
        return path;
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getRowDelimiter() {
        return this.rowDelimiter;
    }

    public boolean isIncludeQueryId() {
        return includeQueryId;
    }

    @Override
    public boolean needAuditEncryption() {
        if (brokerDesc != null) {
            return true;
        }
        return false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        tableRef = analyzer.resolveTableRef(tableRef);
        Preconditions.checkNotNull(tableRef);
        tableRef.analyze(analyzer);

        this.tblName = tableRef.getName();

        PartitionNames partitionNames = tableRef.getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support exporting temporary partitions");
            }
            partitions = partitionNames.getPartitionNames();
        }

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(),
                tblName.getDb(), tblName.getTbl(),
                PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "EXPORT",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tblName.getTbl());
        }

        // check table && partitions && columns whether exist
        checkTable(analyzer.getCatalog());

        // check path is valid
        // generate file name prefix
        checkPath();

        // check broker whether exist
        if (brokerDesc == null) {
            throw new AnalysisException("broker is not provided");
        }
        
        if (brokerDesc.hasBroker()) {
            if (!analyzer.getCatalog().getBrokerMgr().containsBroker(brokerDesc.getName())) {
                throw new AnalysisException("broker " + brokerDesc.getName() + " does not exist");
            }

            FsBroker broker = analyzer.getCatalog().getBrokerMgr().getAnyBroker(brokerDesc.getName());
            if (broker == null) {
                throw new AnalysisException("failed to get alive broker");
            }
        }

        // check properties
        checkProperties(properties);
    }

    private void checkTable(GlobalStateMgr globalStateMgr) throws AnalysisException {
        Database db = globalStateMgr.getDb(tblName.getDb());
        if (db == null) {
            throw new AnalysisException("Db does not exist. name: " + tblName.getDb());
        }

        db.readLock();
        try {
            Table table = db.getTable(tblName.getTbl());
            if (table == null) {
                throw new AnalysisException("Table[" + tblName.getTbl() + "] does not exist");
            }

            Table.TableType tblType = table.getType();
            switch (tblType) {
                case MYSQL:
                case OLAP:
                case LAKE:
                    break;
                case BROKER:
                case SCHEMA:
                case INLINE_VIEW:
                case VIEW:
                default:
                    throw new AnalysisException("Table[" + tblName.getTbl() + "] is " + tblType.toString() +
                            " type, do not support EXPORT.");
            }

            if (partitions != null) {
                for (String partitionName : partitions) {
                    Partition partition = table.getPartition(partitionName);
                    if (partition == null) {
                        throw new AnalysisException("Partition [" + partitionName + "] does not exist.");
                    }
                }
            }

            // check columns
            if (columnNames != null) {
                if (columnNames.isEmpty()) {
                    throw new AnalysisException("Columns is empty.");
                }

                Set<String> tableColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
                for (Column column : table.getBaseSchema()) {
                    tableColumns.add(column.getName());
                }
                Set<String> uniqColumnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
                for (String columnName : columnNames) {
                    if (!uniqColumnNames.add(columnName)) {
                        throw new AnalysisException("Duplicated column [" + columnName + "]");
                    }
                    if (!tableColumns.contains(columnName)) {
                        throw new AnalysisException("Column [" + columnName + "] does not exist in table.");
                    }
                }
            }
        } finally {
            db.readUnlock();
        }
    }

    private void checkPath() throws AnalysisException {
        if (Strings.isNullOrEmpty(path)) {
            throw new AnalysisException("No dest path specified.");
        }

        try {
            URI uri = new URI(path);
            String scheme = uri.getScheme();
            if (scheme == null) {
                throw new AnalysisException(
                        "Invalid export path. please use valid scheme: " + VALID_SCHEMES.toString());
            }
            path = uri.normalize().toString();
        } catch (URISyntaxException e) {
            throw new AnalysisException("Invalid path format. " + e.getMessage());
        }

        if (path.endsWith("/")) {
            fileNamePrefix = DEFAULT_FILE_NAME_PREFIX;
        } else {
            int lastSlashIndex = path.lastIndexOf("/");
            fileNamePrefix = path.substring(lastSlashIndex + 1);
            // path should include "/"
            path = path.substring(0, lastSlashIndex + 1);
        }
    }

    private void checkProperties(Map<String, String> properties) throws AnalysisException {
        this.columnSeparator = PropertyAnalyzer.analyzeColumnSeparator(
                properties, ExportStmt.DEFAULT_COLUMN_SEPARATOR);
        this.columnSeparator = Delimiter.convertDelimiter(this.columnSeparator);
        this.rowDelimiter = PropertyAnalyzer.analyzeRowDelimiter(properties, ExportStmt.DEFAULT_LINE_DELIMITER);
        this.rowDelimiter = Delimiter.convertDelimiter(this.rowDelimiter);
        if (properties.containsKey(LoadStmt.LOAD_MEM_LIMIT)) {
            try {
                Long.parseLong(properties.get(LoadStmt.LOAD_MEM_LIMIT));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid load_mem_limit value: " + e.getMessage());
            }
        } else {
            // use session variables
            properties.put(LoadStmt.LOAD_MEM_LIMIT,
                    String.valueOf(ConnectContext.get().getSessionVariable().getMaxExecMemByte()));
        }
        // timeout
        if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            try {
                Long.parseLong(properties.get(LoadStmt.TIMEOUT_PROPERTY));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid timeout value: " + e.getMessage());
            }
        } else {
            // use session variables
            properties.put(LoadStmt.TIMEOUT_PROPERTY, String.valueOf(Config.export_task_default_timeout_second));
        }

        // include query id
        if (properties.containsKey(INCLUDE_QUERY_ID_PROP)) {
            String includeQueryIdStr = properties.get(INCLUDE_QUERY_ID_PROP);
            if (!includeQueryIdStr.equalsIgnoreCase("true")
                    && !includeQueryIdStr.equalsIgnoreCase("false")) {
                throw new AnalysisException("Invalid include query id value: " + includeQueryIdStr);
            }
            includeQueryId = Boolean.parseBoolean(properties.get(INCLUDE_QUERY_ID_PROP));
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPORT TABLE ");
        if (tblName == null) {
            sb.append("non-exist");
        } else {
            sb.append(tblName.toSql());
        }
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(" PARTITION (");
            Joiner.on(", ").appendTo(sb, partitions);
            sb.append(")");
        }
        sb.append("\n");

        sb.append(" TO ").append("'");
        sb.append(path);
        sb.append("'");

        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }

        if (brokerDesc != null) {
            sb.append("\n WITH BROKER '").append(brokerDesc.getName()).append("' (");
            sb.append(new PrintableMap<String, String>(brokerDesc.getProperties(), "=", true, false, true));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
