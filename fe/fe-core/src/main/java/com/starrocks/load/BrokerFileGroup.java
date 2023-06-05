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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/BrokerFileGroup.java

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

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Delimiter;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.BrokerTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CsvFormat;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.PartitionNames;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A broker file group information, one @DataDescription will
 * produce one BrokerFileGroup. After parsed by broker, detailed
 * broker file information will be saved here.
 */
public class BrokerFileGroup implements Writable {
    private static final Logger LOG = LogManager.getLogger(BrokerFileGroup.class);

    private long tableId;
    private String columnSeparator;
    private String rowDelimiter;
    // fileFormat may be null, which means format will be decided by file's suffix
    private String fileFormat;
    private boolean isNegative;
    private boolean specifyPartition = false;
    private List<Long> partitionIds; // can be null, means no partition specified
    private List<String> filePaths;

    // fileFieldNames should be filled automatically according to schema and mapping when loading from hive table.
    private List<String> fileFieldNames;
    private List<String> columnsFromPath;
    // columnExprList includes all fileFieldNames, columnsFromPath and column mappings
    // this param will be recreated by data desc when the log replay
    private List<ImportColumnDesc> columnExprList;
    // this is only for hadoop function check
    private Map<String, Pair<String, List<String>>> columnToHadoopFunction;
    // filter the data which has been conformed
    private Expr whereExpr;

    // load from table
    private long srcTableId = -1;
    private boolean isLoadFromTable = false;
    
    // for csv
    private CsvFormat csvFormat;

    public static final String ESCAPE = "escape";
    public static final String ENCLOSE = "enclose";
    public static final String TRIMSPACE = "trim_space";
    public static final String SKIPHEADER = "skip_header";

    // for unit test and edit log persistence
    private BrokerFileGroup() {
        this.csvFormat = new CsvFormat((byte) 0, (byte) 0, 0, false);
    }

    // Used for broker table, no need to parse
    public BrokerFileGroup(BrokerTable table) throws AnalysisException {
        this.tableId = table.getId();
        this.columnSeparator = Delimiter.convertDelimiter(table.getColumnSeparator());
        this.rowDelimiter = Delimiter.convertDelimiter(table.getRowDelimiter());
        this.isNegative = false;
        this.filePaths = table.getPaths();
        this.fileFormat = table.getFileFormat();
        this.csvFormat = new CsvFormat((byte) 0, (byte) 0, 0, false);
    }

    public BrokerFileGroup(TableFunctionTable table) throws AnalysisException {
        this.tableId = table.getId();
        this.isNegative = false;

        this.filePaths = new ArrayList<>();
        this.filePaths.add(table.getPath());

        this.fileFormat = table.getFormat();
        this.columnSeparator = "\t";
        this.rowDelimiter = "\n";
        this.csvFormat = new CsvFormat((byte) 0, (byte) 0, 0, false);
        this.fileFieldNames = new ArrayList<>();

        this.columnExprList = table.getColumnExprList();
        this.columnsFromPath = new ArrayList<>();
    }

    public BrokerFileGroup(DataDescription dataDescription) {
        this.fileFieldNames = dataDescription.getFileFieldNames();
        this.columnsFromPath = dataDescription.getColumnsFromPath();
        this.columnExprList = dataDescription.getParsedColumnExprList();
        this.columnToHadoopFunction = dataDescription.getColumnToHadoopFunction();
        this.whereExpr = dataDescription.getWhereExpr();
        this.csvFormat = new CsvFormat((byte) 0, (byte) 0, 0, false);
    }

    public void parseFormatProperties(DataDescription dataDescription) {
        CsvFormat csvFormat = dataDescription.getCsvFormat();
        if (csvFormat != null) {
            this.csvFormat = csvFormat;
        }
    }

    // NOTE: DBLock will be held
    // This will parse the input DataDescription to list for BrokerFileInfo
    public void parse(Database db, DataDescription dataDescription) throws DdlException {
        // tableId
        Table table = db.getTable(dataDescription.getTableName());
        if (table == null) {
            throw new DdlException("Unknown table " + dataDescription.getTableName()
                    + " in database " + db.getOriginName());
        }
        if (!(table instanceof OlapTable)) {
            throw new DdlException("Table " + table.getName() + " is not OlapTable");
        }
        OlapTable olapTable = (OlapTable) table;
        tableId = table.getId();

        // partitionId
        PartitionNames partitionNames = dataDescription.getPartitionNames();
        if (partitionNames != null) {
            specifyPartition = true;
            partitionIds = Lists.newArrayList();
            for (String pName : partitionNames.getPartitionNames()) {
                Partition partition = olapTable.getPartition(pName, partitionNames.isTemp());
                if (partition == null) {
                    throw new DdlException("Unknown partition '" + pName + "' in table '" + table.getName() + "'");
                }
                partitionIds.add(partition.getId());
            }
        }

        if (olapTable.getState() == OlapTableState.RESTORE) {
            throw new DdlException("Table [" + table.getName() + "] is under restore");
        }

        if (olapTable.getKeysType() != KeysType.AGG_KEYS && dataDescription.isNegative()) {
            throw new DdlException("Only aggregate table can specify NEGATIVE");
        }

        // check negative for sum aggregate type
        if (dataDescription.isNegative()) {
            for (Column column : table.getBaseSchema()) {
                if (!column.isKey() && column.getAggregationType() != AggregateType.SUM) {
                    throw new DdlException("Column is not SUM AggreateType. column:" + column.getName());
                }
            }
        }

        // column
        columnSeparator = dataDescription.getColumnSeparator();
        if (columnSeparator == null) {
            columnSeparator = "\t";
        }
        rowDelimiter = dataDescription.getRowDelimiter();
        if (rowDelimiter == null) {
            rowDelimiter = "\n";
        }

        fileFormat = dataDescription.getFileFormat();
        if (fileFormat != null) {
            if (!fileFormat.toLowerCase().equals("parquet") && !fileFormat.toLowerCase().equals("csv") &&
                    !fileFormat.toLowerCase().equals("orc")) {
                throw new DdlException("File Format Type " + fileFormat + " is invalid.");
            }
        }
        isNegative = dataDescription.isNegative();

        parseFormatProperties(dataDescription);

        // FilePath
        filePaths = dataDescription.getFilePaths();

        if (dataDescription.isLoadFromTable()) {
            String srcTableName = dataDescription.getSrcTableName();
            // src table should be hive table
            Table srcTable = db.getTable(srcTableName);
            if (srcTable == null) {
                throw new DdlException("Unknown table " + srcTableName + " in database " + db.getOriginName());
            }
            if (!(srcTable instanceof HiveTable)) {
                throw new DdlException("Source table " + srcTableName + " is not HiveTable");
            }

            // get columns that will be read from hive table
            Set<String> srcColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            Map<String, ImportColumnDesc> nameToColumnDesc = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            Set<String> srcTableColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            Preconditions.checkArgument(columnExprList != null);
            for (ImportColumnDesc desc : columnExprList) {
                nameToColumnDesc.put(desc.getColumnName(), desc);
            }
            for (Column column : srcTable.getBaseSchema()) {
                srcTableColumns.add(column.getName());
            }

            for (Column column : olapTable.getBaseSchema()) {
                String columnName = column.getName();
                if (nameToColumnDesc.containsKey(columnName)) {
                    ImportColumnDesc desc = nameToColumnDesc.get(columnName);
                    if (desc.isColumn()) {
                        srcColumns.add(columnName);
                    } else {
                        Expr expr = desc.getExpr();
                        List<SlotRef> slots = Lists.newArrayList();
                        expr.collect(SlotRef.class, slots);
                        for (SlotRef slot : slots) {
                            String slotColumnName = slot.getColumnName();
                            if (!srcTableColumns.contains(slotColumnName)) {
                                throw new DdlException("Column " + slotColumnName + " is not in source table");
                            }
                            srcColumns.add(slotColumnName);
                        }
                    }
                } else if (srcTableColumns.contains(columnName)) {
                    srcColumns.add(columnName);
                }
            }

            Preconditions.checkArgument(fileFieldNames == null);
            fileFieldNames = Lists.newArrayList(srcColumns);

            srcTableId = srcTable.getId();
            isLoadFromTable = true;
        }
    }

    public boolean isSpecifyPartition() {
        return specifyPartition;
    }

    public long getTableId() {
        return tableId;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getRowDelimiter() {
        return rowDelimiter;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public boolean isNegative() {
        return isNegative;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public List<String> getColumnsFromPath() {
        return columnsFromPath;
    }

    public List<ImportColumnDesc> getColumnExprList() {
        return columnExprList;
    }

    public List<String> getFileFieldNames() {
        return fileFieldNames;
    }

    public Map<String, Pair<String, List<String>>> getColumnToHadoopFunction() {
        return columnToHadoopFunction;
    }

    public long getSrcTableId() {
        return srcTableId;
    }

    public boolean isLoadFromTable() {
        return isLoadFromTable;
    }

    public long getSkipHeader() {
        return csvFormat.getSkipheader();
    }

    public byte getEnclose() {
        return csvFormat.getEnclose();
    }

    public byte getEscape() {
        return csvFormat.getEscape();
    }

    public boolean isTrimspace() {
        return csvFormat.isTrimspace();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BrokerFileGroup{tableId=").append(tableId);
        if (partitionIds != null) {
            sb.append(",partitionIds=[");
            int idx = 0;
            for (long id : partitionIds) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(id);
            }
            sb.append("]");
        }
        if (columnsFromPath != null) {
            sb.append(",columnsFromPath=[");
            int idx = 0;
            for (String name : columnsFromPath) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(name);
            }
            sb.append("]");
        }
        if (fileFieldNames != null) {
            sb.append(",fileFieldNames=[");
            int idx = 0;
            for (String name : fileFieldNames) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(name);
            }
            sb.append("]");
        }
        sb.append(",columnSeparator=").append(columnSeparator)
                .append(",rowDelimiter=").append(rowDelimiter)
                .append(",fileFormat=").append(fileFormat)
                .append(",isNegative=").append(isNegative);
        sb.append(",fileInfos=[");
        int idx = 0;
        for (String path : filePaths) {
            if (idx++ != 0) {
                sb.append(",");
            }
            sb.append(path);
        }
        sb.append("]");
        sb.append(",srcTableId=").append(srcTableId);
        sb.append(",isLoadFromTable=").append(isLoadFromTable);
        sb.append("}");

        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // tableId
        out.writeLong(tableId);
        // columnSeparator
        Text.writeString(out, columnSeparator);
        // rowDelimiter
        Text.writeString(out, rowDelimiter);
        // isNegative
        out.writeBoolean(isNegative);
        // partitionIds
        if (partitionIds == null) {
            out.writeInt(0);
        } else {
            out.writeInt(partitionIds.size());
            for (long id : partitionIds) {
                out.writeLong(id);
            }
        }
        // fileFieldNames
        if (fileFieldNames == null) {
            out.writeInt(0);
        } else {
            out.writeInt(fileFieldNames.size());
            for (String name : fileFieldNames) {
                Text.writeString(out, name);
            }
        }
        // filePaths
        out.writeInt(filePaths.size());
        for (String path : filePaths) {
            Text.writeString(out, path);
        }
        // expr column map will be null after broker load supports function
        out.writeInt(0);

        // fileFormat
        if (fileFormat == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, fileFormat);
        }

        // src table
        out.writeLong(srcTableId);
        out.writeBoolean(isLoadFromTable);
    }

    public void readFields(DataInput in) throws IOException {
        tableId = in.readLong();
        columnSeparator = Text.readString(in);
        rowDelimiter = Text.readString(in);
        isNegative = in.readBoolean();
        // partitionIds
        {
            int partSize = in.readInt();
            if (partSize > 0) {
                partitionIds = Lists.newArrayList();
                for (int i = 0; i < partSize; ++i) {
                    partitionIds.add(in.readLong());
                }
            }
        }
        // fileFieldName
        {
            int fileFieldNameSize = in.readInt();
            if (fileFieldNameSize > 0) {
                fileFieldNames = Lists.newArrayList();
                for (int i = 0; i < fileFieldNameSize; ++i) {
                    fileFieldNames.add(Text.readString(in));
                }
            }
        }
        // fileInfos
        {
            int size = in.readInt();
            filePaths = Lists.newArrayList();
            for (int i = 0; i < size; ++i) {
                filePaths.add(Text.readString(in));
            }
        }
        // expr column map
        Map<String, Expr> exprColumnMap = Maps.newHashMap();
        {
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                final String name = Text.readString(in);
                exprColumnMap.put(name, Expr.readIn(in));
            }
        }
        // file format
        if (in.readBoolean()) {
            fileFormat = Text.readString(in);
        }
        // src table
        srcTableId = in.readLong();
        isLoadFromTable = in.readBoolean();

        // There are no columnExprList in the previous load job which is created before function is supported.
        // The columnExprList could not be analyzed without origin stmt in the previous load job.
        // So, the columnExprList need to be merged in here.
        if (fileFieldNames == null || fileFieldNames.isEmpty()) {
            return;
        }
        // Order of columnExprList: fileFieldNames + columnsFromPath
        columnExprList = Lists.newArrayList();
        for (String columnName : fileFieldNames) {
            columnExprList.add(new ImportColumnDesc(columnName, null));
        }
        if (exprColumnMap == null || exprColumnMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Expr> columnExpr : exprColumnMap.entrySet()) {
            columnExprList.add(new ImportColumnDesc(columnExpr.getKey(), columnExpr.getValue()));
        }
    }

    public static BrokerFileGroup read(DataInput in) throws IOException {
        BrokerFileGroup fileGroup = new BrokerFileGroup();
        fileGroup.readFields(in);
        return fileGroup;
    }
}
