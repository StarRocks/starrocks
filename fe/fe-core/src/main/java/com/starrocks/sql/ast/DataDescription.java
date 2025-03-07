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


package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CsvFormat;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
// used to describe data info which is needed to import.
//
//      data_desc:
//          DATA INFILE ('file_path', ...)
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [COLUMNS TERMINATED BY separator]
//          [ROWS TERMINATED BY separator]
//          [FORMAT AS format]
//          [(tmp_col1, tmp_col2, col3, ...)]
//          [COLUMNS FROM PATH AS (col1, ...)]
//          [SET (k1=f1(xx), k2=f2(xxx))]
//          [where_clause]
//
//          DATA FROM TABLE external_hive_tbl_name
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [SET (k1=f1(xx), k2=f2(xxx))]
//          [where_clause]

/**
 * The transform of columns should be added after the keyword named COLUMNS.
 * The transform after the keyword named SET is the old ways which only supports the hadoop function.
 * It old way of transform will be removed gradually. It
 */
public class DataDescription implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(DataDescription.class);
    // function isn't built-in function, hll_hash is not built-in function in hadoop load.
    private static final List<String> HADOOP_SUPPORT_FUNCTION_NAMES = Arrays.asList(
            FunctionSet.STRFTIME,
            FunctionSet.TIME_FORMAT,
            FunctionSet.ALIGNMENT_TIMESTAMP,
            FunctionSet.DEFAULT_VALUE,
            FunctionSet.MD5_SUM,
            FunctionSet.REPLACE_VALUE,
            FunctionSet.NOW,
            FunctionSet.HLL_HASH,
            FunctionSet.SUBSTITUTE,
            FunctionSet.GET_JSON_INT,
            FunctionSet.GET_JSON_DOUBLE,
            FunctionSet.GET_JSON_STRING);

    private final String tableName;
    private final PartitionNames partitionNames;
    private final List<String> filePaths;
    private final ColumnSeparator columnSeparator;
    private final RowDelimiter rowDelimiter;
    private final String fileFormat;
    private final boolean isNegative;

    private CsvFormat csvFormat;
    private Map<String, String> formatProperties;

    // column names of source files
    private List<String> fileFieldNames;
    // column names in the path
    private final List<String> columnsFromPath;
    // save column mapping in SET(xxx = xxx) clause
    private final List<Expr> columnMappingList;
    private final Expr whereExpr;

    private final String srcTableName;

    // Used for mini load
    private TNetworkAddress beAddr;

    // Merged from fileFieldNames, columnsFromPath and columnMappingList
    // ImportColumnDesc: column name to (expr or null)
    private List<ImportColumnDesc> parsedColumnExprList = Lists.newArrayList();
    /*
     * This param only include the hadoop function which need to be checked in the future.
     * For hadoop load, this param is also used to persistence.
     * The function in this param is copied from 'parsedColumnExprList'
     */
    private Map<String, Pair<String, List<String>>> columnToHadoopFunction =
            Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private boolean isHadoopLoad = false;

    private final NodePosition pos;

    public DataDescription(String tableName,
                           PartitionNames partitionNames,
                           List<String> filePaths,
                           List<String> columns,
                           ColumnSeparator columnSeparator,
                           RowDelimiter rowDelimiter,
                           String fileFormat,
                           boolean isNegative,
                           List<Expr> columnMappingList) {
        this(tableName, partitionNames, filePaths, columns, columnSeparator, rowDelimiter, fileFormat, null, isNegative,
                columnMappingList, null, null);
    }

    public DataDescription(String tableName,
                           PartitionNames partitionNames,
                           List<String> filePaths,
                           List<String> columns,
                           ColumnSeparator columnSeparator,
                           RowDelimiter rowDelimiter,
                           String fileFormat,
                           List<String> columnsFromPath,
                           boolean isNegative,
                           List<Expr> columnMappingList,
                           Expr whereExpr,
                           CsvFormat csvFormat) {
        this(tableName, partitionNames, filePaths, columns, columnSeparator, rowDelimiter, fileFormat, columnsFromPath,
                isNegative, columnMappingList, whereExpr, csvFormat, NodePosition.ZERO);
    }

    public DataDescription(String tableName,
                           PartitionNames partitionNames,
                           List<String> filePaths,
                           List<String> columns,
                           ColumnSeparator columnSeparator,
                           RowDelimiter rowDelimiter,
                           String fileFormat,
                           List<String> columnsFromPath,
                           boolean isNegative,
                           List<Expr> columnMappingList,
                           Expr whereExpr,
                           CsvFormat csvFormat, NodePosition pos) {
        this.pos = pos;
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.filePaths = filePaths;
        this.fileFieldNames = columns;
        this.columnSeparator = columnSeparator;
        this.rowDelimiter = rowDelimiter;
        this.fileFormat = fileFormat;
        this.columnsFromPath = columnsFromPath;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.whereExpr = whereExpr;
        this.srcTableName = null;
        this.csvFormat = csvFormat;
    }

    // data from table external_hive_table
    public DataDescription(String tableName,
                           PartitionNames partitionNames,
                           String srcTableName,
                           boolean isNegative,
                           List<Expr> columnMappingList,
                           Expr whereExpr) {
        this(tableName, partitionNames, srcTableName, isNegative, columnMappingList, whereExpr, NodePosition.ZERO);
    }

    public DataDescription(String tableName,
                           PartitionNames partitionNames,
                           String srcTableName,
                           boolean isNegative,
                           List<Expr> columnMappingList,
                           Expr whereExpr, NodePosition pos) {
        this.pos = pos;
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.filePaths = null;
        this.fileFieldNames = null;
        this.columnSeparator = null;
        this.rowDelimiter = null;
        this.fileFormat = null;
        this.columnsFromPath = null;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.whereExpr = whereExpr;
        this.srcTableName = srcTableName;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public CsvFormat getCsvFormat() {
        return csvFormat;
    }

    public List<String> getFileFieldNames() {
        if (fileFieldNames == null || fileFieldNames.isEmpty()) {
            return null;
        }
        return fileFieldNames;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public List<String> getColumnsFromPath() {
        return columnsFromPath;
    }

    public String getColumnSeparator() {
        if (columnSeparator == null) {
            return null;
        }
        return columnSeparator.getColumnSeparator();
    }

    public String getRowDelimiter() {
        if (rowDelimiter == null) {
            return null;
        }
        return rowDelimiter.getRowDelimiter();
    }

    public boolean isNegative() {
        return isNegative;
    }

    public TNetworkAddress getBeAddr() {
        return beAddr;
    }

    public void setBeAddr(TNetworkAddress addr) {
        beAddr = addr;
    }

    @Deprecated
    public void addColumnMapping(String functionName, Pair<String, List<String>> pair) {
        if (Strings.isNullOrEmpty(functionName) || pair == null) {
            return;
        }
        columnToHadoopFunction.put(functionName, pair);
    }

    public Map<String, Pair<String, List<String>>> getColumnToHadoopFunction() {
        return columnToHadoopFunction;
    }

    public List<ImportColumnDesc> getParsedColumnExprList() {
        return parsedColumnExprList;
    }

    public void setIsHadoopLoad(boolean isHadoopLoad) {
        this.isHadoopLoad = isHadoopLoad;
    }

    public boolean isHadoopLoad() {
        return isHadoopLoad;
    }

    public String getSrcTableName() {
        return srcTableName;
    }

    public boolean isLoadFromTable() {
        return !Strings.isNullOrEmpty(srcTableName);
    }

    /*
     * Analyze parsedExprMap and columnToHadoopFunction from columns, columns from path and columnMappingList
     * Example:
     *      columns (col1, tmp_col2, tmp_col3)
     *      columns from path as (col4, col5)
     *      set (col2=tmp_col2+1, col3=strftime("%Y-%m-%d %H:%M:%S", tmp_col3))
     *
     * Result:
     *      parsedExprMap = {"col1": null, "tmp_col2": null, "tmp_col3": null, "col4": null, "col5": null,
     *                       "col2": "tmp_col2+1", "col3": "strftime("%Y-%m-%d %H:%M:%S", tmp_col3)"}
     *      columnToHadoopFunction = {"col3": "strftime("%Y-%m-%d %H:%M:%S", tmp_col3)"}
     */
    private void analyzeColumns() throws AnalysisException {
        if ((fileFieldNames == null || fileFieldNames.isEmpty()) &&
                (columnsFromPath != null && !columnsFromPath.isEmpty())) {
            throw new AnalysisException("Can not specify columns_from_path without column_list");
        }

        // used to check duplicated column name in COLUMNS and COLUMNS FROM PATH
        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        // merge columns exprs from columns, columns from path and columnMappingList
        // 1. analyze columns
        if (fileFieldNames != null && !fileFieldNames.isEmpty()) {
            for (String columnName : fileFieldNames) {
                if (!columnNames.add(columnName)) {
                    throw new AnalysisException("Duplicate column: " + columnName);
                }
                ImportColumnDesc importColumnDesc = new ImportColumnDesc(columnName, null);
                parsedColumnExprList.add(importColumnDesc);
            }
        }

        // 2. analyze columns from path
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            if (isHadoopLoad) {
                throw new AnalysisException("Hadoop load does not support specifying columns from path");
            }
            for (String columnName : columnsFromPath) {
                if (!columnNames.add(columnName)) {
                    throw new AnalysisException("Duplicate column: " + columnName);
                }
                ImportColumnDesc importColumnDesc = new ImportColumnDesc(columnName, null);
                parsedColumnExprList.add(importColumnDesc);
            }
        }

        // 3: analyze column mapping
        if (columnMappingList == null || columnMappingList.isEmpty()) {
            return;
        }

        // used to check duplicated column name in SET clause
        Set<String> columnMappingNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // Step2: analyze column mapping
        // the column expr only support the SlotRef or eq binary predicate which's child(0) must be a SloRef.
        // the duplicate column name of SloRef is forbidden.
        for (Expr columnExpr : columnMappingList) {
            if (!(columnExpr instanceof BinaryPredicate)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "Expr: " + columnExpr.toSql());
            }
            BinaryPredicate predicate = (BinaryPredicate) columnExpr;
            if (predicate.getOp() != BinaryType.EQ) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "The mapping operator error, op: " + predicate.getOp());
            }
            Expr child0 = predicate.getChild(0);
            if (!(child0 instanceof SlotRef)) {
                throw new AnalysisException("Mapping function expr only support the column or eq binary predicate. "
                        + "The mapping column error. column: " + child0.toSql());
            }
            String column = ((SlotRef) child0).getColumnName();
            if (!columnMappingNames.add(column)) {
                throw new AnalysisException("Duplicate column mapping: " + column);
            }
            // hadoop load only supports the FunctionCallExpr
            Expr child1 = predicate.getChild(1);
            if (isHadoopLoad && !(child1 instanceof FunctionCallExpr)) {
                throw new AnalysisException("Hadoop load only supports the designated function. "
                        + "The error mapping function is:" + child1.toSql());
            }
            ImportColumnDesc importColumnDesc = new ImportColumnDesc(column, child1);
            parsedColumnExprList.add(importColumnDesc);
            if (child1 instanceof FunctionCallExpr) {
                analyzeColumnToHadoopFunction(column, child1);
            }
        }
    }

    private void analyzeColumnToHadoopFunction(String columnName, Expr child1) throws AnalysisException {
        Preconditions.checkState(child1 instanceof FunctionCallExpr);
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) child1;
        String functionName = functionCallExpr.getFnName().getFunction();
        if (!HADOOP_SUPPORT_FUNCTION_NAMES.contains(functionName.toLowerCase())) {
            return;
        }
        List<Expr> paramExprs = functionCallExpr.getParams().exprs();
        List<String> args = Lists.newArrayList();

        for (Expr paramExpr : paramExprs) {
            if (paramExpr instanceof SlotRef) {
                SlotRef slot = (SlotRef) paramExpr;
                args.add(slot.getColumnName());
            } else if (paramExpr instanceof StringLiteral) {
                StringLiteral literal = (StringLiteral) paramExpr;
                args.add(literal.getValue());
            } else if (paramExpr instanceof NullLiteral) {
                args.add(null);
            } else {
                if (isHadoopLoad) {
                    // hadoop function only support slot, string and null parameters
                    throw new AnalysisException("Mapping function args error, arg: " + paramExpr.toSql());
                }
            }
        }

        Pair<String, List<String>> functionPair = new Pair<String, List<String>>(functionName, args);
        columnToHadoopFunction.put(columnName, functionPair);
    }

    public static void validateMappingFunction(String functionName, List<String> args,
                                               Map<String, String> columnNameMap,
                                               Column mappingColumn, boolean isHadoopLoad) throws AnalysisException {
        if (functionName.equalsIgnoreCase(FunctionSet.ALIGNMENT_TIMESTAMP)) {
            validateAlignmentTimestamp(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase(FunctionSet.STRFTIME)) {
            validateStrftime(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase(FunctionSet.TIME_FORMAT)) {
            validateTimeFormat(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase(FunctionSet.DEFAULT_VALUE)) {
            validateDefaultValue(args, mappingColumn);
        } else if (functionName.equalsIgnoreCase(FunctionSet.MD5_SUM)) {
            validateMd5sum(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase(FunctionSet.REPLACE_VALUE)) {
            validateReplaceValue(args, mappingColumn);
        } else if (functionName.equalsIgnoreCase(FunctionSet.HLL_HASH)) {
            validateHllHash(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase(FunctionSet.NOW)) {
            validateNowFunction(mappingColumn);
        } else if (functionName.equalsIgnoreCase(FunctionSet.SUBSTITUTE)) {
            validateSubstituteFunction(args, columnNameMap);
        } else if (functionName.equalsIgnoreCase(FunctionSet.GET_JSON_DOUBLE) ||
                functionName.equalsIgnoreCase(FunctionSet.GET_JSON_INT) ||
                functionName.equalsIgnoreCase(FunctionSet.GET_JSON_STRING)) {
            validateGetJsonFunction(args, columnNameMap);
        } else {
            if (isHadoopLoad) {
                throw new AnalysisException("Unknown function: " + functionName);
            }
        }
    }

    // eg: k2 = substitute(k1)
    // this is used for creating derivative column from existing column
    private static void validateSubstituteFunction(List<String> args, Map<String, String> columnNameMap)
            throws AnalysisException {
        if (args.size() != 1) {
            throw new AnalysisException("Should has only one argument: " + args);
        }

        String argColumn = args.get(0);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(0, columnNameMap.get(argColumn));
    }

    // eg: k2 = get_json_string(k1, "$.id")
    // this is used for creating derivative column from existing column
    private static void validateGetJsonFunction(List<String> args, Map<String, String> columnNameMap)
            throws AnalysisException {
        if (args.size() != 2) {
            throw new AnalysisException("Should has only two arguments: " + args);
        }

        String argColumn = args.get(0);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(0, columnNameMap.get(argColumn));
    }

    private static void validateAlignmentTimestamp(List<String> args, Map<String, String> columnNameMap)
            throws AnalysisException {
        if (args.size() != 2) {
            throw new AnalysisException("Function alignment_timestamp args size is not 2");
        }

        String precision = args.get(0).toLowerCase();
        String regex = "^(?:year|month|day|hour)$";
        if (!precision.matches(regex)) {
            throw new AnalysisException("Alignment precision error. regex: " + regex + ", arg: " + precision);
        }

        String argColumn = args.get(1);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(1, columnNameMap.get(argColumn));
    }

    private static void validateStrftime(List<String> args, Map<String, String> columnNameMap) throws
            AnalysisException {
        if (args.size() != 2) {
            throw new AnalysisException("Function strftime needs 2 args");
        }

        String format = args.get(0);
        String regex = "^(%[YMmdHhiSs][ -:]?){0,5}%[YMmdHhiSs]$";
        if (!format.matches(regex)) {
            throw new AnalysisException("Date format error. regex: " + regex + ", arg: " + format);
        }

        String argColumn = args.get(1);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(1, columnNameMap.get(argColumn));
    }

    private static void validateTimeFormat(List<String> args, Map<String, String> columnNameMap) throws
            AnalysisException {
        if (args.size() != 3) {
            throw new AnalysisException("Function time_format needs 3 args");
        }

        String outputFormat = args.get(0);
        String inputFormat = args.get(1);
        String regex = "^(%[YMmdHhiSs][ -:]?){0,5}%[YMmdHhiSs]$";
        if (!outputFormat.matches(regex)) {
            throw new AnalysisException("Date format error. regex: " + regex + ", arg: " + outputFormat);
        }
        if (!inputFormat.matches(regex)) {
            throw new AnalysisException("Date format error. regex: " + regex + ", arg: " + inputFormat);
        }

        String argColumn = args.get(2);
        if (!columnNameMap.containsKey(argColumn)) {
            throw new AnalysisException("Column is not in sources, column: " + argColumn);
        }

        args.set(2, columnNameMap.get(argColumn));
    }

    private static void validateDefaultValue(List<String> args, Column column) throws AnalysisException {
        if (args.size() != 1) {
            throw new AnalysisException("Function default_value needs 1 arg");
        }

        if (!column.isAllowNull() && args.get(0) == null) {
            throw new AnalysisException("Column is not null, column: " + column.getName());
        }

        if (args.get(0) != null) {
            ColumnDef.validateDefaultValue(column.getType(), new StringLiteral(args.get(0)));
        }
    }

    private static void validateMd5sum(List<String> args, Map<String, String> columnNameMap) throws AnalysisException {
        for (int i = 0; i < args.size(); ++i) {
            String argColumn = args.get(i);
            if (!columnNameMap.containsKey(argColumn)) {
                throw new AnalysisException("Column is not in sources, column: " + argColumn);
            }

            args.set(i, columnNameMap.get(argColumn));
        }
    }

    private static void validateReplaceValue(List<String> args, Column column) throws AnalysisException {
        String replaceValue = null;
        if (args.size() == 1) {
            Column.DefaultValueType defaultValueType = column.getDefaultValueType();
            if (defaultValueType == Column.DefaultValueType.NULL) {
                throw new AnalysisException("Column " + column.getName() + " has no default value");
            } else if (defaultValueType == Column.DefaultValueType.CONST) {
                replaceValue = column.calculatedDefaultValue();
            } else if (defaultValueType == Column.DefaultValueType.VARY) {
                throw new AnalysisException("Column " + column.getName() + " has unsupported default value:" +
                        column.getDefaultExpr().getExpr());
            }

            args.add(replaceValue);
        } else if (args.size() == 2) {
            replaceValue = args.get(1);
        } else {
            throw new AnalysisException("Function replace_value need 1 or 2 args");
        }

        if (!column.isAllowNull() && replaceValue == null) {
            throw new AnalysisException("Column is not null, column: " + column.getName());
        }

        if (replaceValue != null) {
            ColumnDef.validateDefaultValue(column.getType(), new StringLiteral(replaceValue));
        }
    }

    private static void validateHllHash(List<String> args, Map<String, String> columnNameMap) throws AnalysisException {
        for (int i = 0; i < args.size(); ++i) {
            String argColumn = args.get(i);
            if (!columnNameMap.containsKey(argColumn)) {
                throw new AnalysisException("Column is not in sources, column: " + argColumn);
            }
            args.set(i, columnNameMap.get(argColumn));
        }
    }

    private static void validateNowFunction(Column mappingColumn) throws AnalysisException {
        if (!mappingColumn.getType().isDate() && !mappingColumn.getType().isDatetime()) {
            throw new AnalysisException("Now() function is only support for DATE/DATETIME column");
        }
    }

    private void checkLoadPriv(String fullDbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("No table name in load statement.");
        }

        try {
            Authorizer.checkTableAction(ConnectContext.get(), fullDbName, tableName, PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    ConnectContext.get().getCurrentUserIdentity(),
                    ConnectContext.get().getCurrentRoleIds(),
                    PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
        }

        if (isLoadFromTable()) {
            try {
                Authorizer.checkTableAction(ConnectContext.get(), fullDbName, srcTableName, PrivilegeType.SELECT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        ConnectContext.get().getCurrentUserIdentity(),
                        ConnectContext.get().getCurrentRoleIds(),
                        PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), srcTableName);
            }
        }
    }

    public void analyze(String fullDbName) throws AnalysisException {
        checkLoadPriv(fullDbName);
        analyzeTable(fullDbName);
        analyzeWithoutCheckPriv();
    }

    public void analyzeTable(String fullDbName) throws AnalysisException {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(fullDbName, tableName);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName.toString());
        }

        if (table instanceof MaterializedView) {
            throw new AnalysisException(String.format(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tableName, tableName));
        }
    }

    public void analyzeWithoutCheckPriv() throws AnalysisException {
        if (!isLoadFromTable()) {
            if (filePaths == null || filePaths.isEmpty()) {
                throw new AnalysisException("No file path in load statement.");
            }
            for (int i = 0; i < filePaths.size(); ++i) {
                filePaths.set(i, filePaths.get(i).trim());
            }
        }

        if (partitionNames != null) {
            partitionNames.analyze(null);
        }

        analyzeColumns();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (isLoadFromTable()) {
            sb.append("DATA FROM TABLE ").append(srcTableName);
        } else {
            sb.append("DATA INFILE (");
            assert filePaths != null;
            Joiner.on(", ").appendTo(sb, filePaths.stream().map(s -> "'" + s + "'")
                    .collect(Collectors.toList())).append(")");
        }
        if (isNegative) {
            sb.append(" NEGATIVE");
        }
        sb.append(" INTO TABLE ").append(tableName);
        if (partitionNames != null) {
            sb.append(" ");
            sb.append(partitionNames);
        }
        if (columnSeparator != null) {
            sb.append(" COLUMNS TERMINATED BY ").append(columnSeparator);
        }
        if (rowDelimiter != null) {
            sb.append(" ROWS TERMINATED BY ").append(rowDelimiter);
        }
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            sb.append(" COLUMNS FROM PATH AS (");
            Joiner.on(", ").appendTo(sb, columnsFromPath).append(")");
        }
        if (fileFieldNames != null && !fileFieldNames.isEmpty()) {
            sb.append(" (");
            sb.append(Joiner.on(", ").join(
                    fileFieldNames.stream().map(f -> "`" + f + "`").collect(Collectors.toList())));
            sb.append(")");
        }
        if (columnMappingList != null && !columnMappingList.isEmpty()) {
            sb.append(" SET (");
            sb.append(Joiner.on(", ").join(columnMappingList.stream()
                    .map(AstToSQLBuilder::toSQL).collect(Collectors.toList())));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
