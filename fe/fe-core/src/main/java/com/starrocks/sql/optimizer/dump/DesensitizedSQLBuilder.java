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

package com.starrocks.sql.optimizer.dump;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.FileTable;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.credential.CredentialUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static java.util.stream.Collectors.toList;

public class DesensitizedSQLBuilder {

    private static final String COLUMN = "column";

    private static final String COLUMN_ALIAS = "column alias";

    private static final String TABLE_ALIAS = "table alias";



    public static String desensitizeSQL(StatementBase statement, Map<String, String> desensitizedDict) {
        Map<TableName, Table> tables = AnalyzerUtils.collectAllTableAndViewWithAlias(statement);
        boolean sameCatalogDb = tables.keySet().stream().map(TableName::getCatalogAndDb).distinct().count() == 1;
        return new DesensitizedSQLVisitor(sameCatalogDb, false, desensitizedDict).visit(statement);
    }

    public static String desensitizeViewDef(View view, Map<String, String> desensitizedDict, ConnectContext connectContext) {
        QueryStatement stmt = view.getQueryStatement();
        new QueryAnalyzer(connectContext).analyze(stmt);
        Map<TableName, Table> tables = AnalyzerUtils.collectAllTableAndViewWithAlias(stmt);
        boolean sameCatalogDb = tables.keySet().stream().map(TableName::getCatalogAndDb).distinct().count() == 1;
        return new DesensitizedSQLVisitor(sameCatalogDb, false, desensitizedDict).desensitizeViewDef(stmt);
    }

    public static String desensitizeTableDef(Pair<String, Table> pair, Map<String, String> desensitizedDict) {
        Table table = pair.second;
        DesensitizedSQLVisitor visitor = new DesensitizedSQLVisitor(true, true, desensitizedDict);
        String tableDef = "";
        if (table.isMaterializedView()) {
            visitor = new DesensitizedSQLVisitor(true, false, desensitizedDict);
            tableDef = visitor.desensitizeMvDef(table);
        } else if (table.getType() == Table.TableType.MYSQL || table.getType() == Table.TableType.ELASTICSEARCH
                || table.getType() == Table.TableType.BROKER || table.getType() == Table.TableType.HIVE
                || table.getType() == Table.TableType.HUDI || table.getType() == Table.TableType.ICEBERG
                || table.getType() == Table.TableType.JDBC
                || table.getType() == Table.TableType.FILE) {
            tableDef = visitor.desensitizeExternalTableDef(pair.first, table);
        } else if (table instanceof OlapTable) {
            tableDef = visitor.desensitizeOlapTableDef(pair.first, (OlapTable) pair.second);
        } else {
            throw new IllegalArgumentException("unsupported table type " + pair.second.getType());
        }
        return tableDef;
    }

    public static String desensitizeDbName(String dbName, Map<String, String> desensitizedDict) {
        Preconditions.checkState(desensitizedDict.containsKey(dbName),
                "db %s not be desensitized", dbName);
        return "db_" + desensitizedDict.get(dbName);
    }

    public static String desensitizeTblName(String tblName, Map<String, String> desensitizedDict) {
        Preconditions.checkState(desensitizedDict.containsKey(tblName),
                "table %s not be desensitized", tblName);
        return "tbl_" + desensitizedDict.get(tblName);
    }

    public static String desensitizeColName(String colName, Map<String, String> desensitizedDict) {
        colName = StringUtils.lowerCase(colName);
        Preconditions.checkState(desensitizedDict.containsKey(colName),
                "col %s not be desensitized", colName);
        return desensitizedDict.get(colName);
    }

    public static class DesensitizedSQLVisitor extends AstToSQLBuilder.AST2SQLBuilderVisitor {

        private final Map<String, String> desensitizedDict;

        public DesensitizedSQLVisitor(boolean simple, boolean withoutTbl, Map<String, String> desensitizedDict) {
            super(simple, withoutTbl, true);
            this.desensitizedDict = desensitizedDict;
        }

        @Override
        public String visitSelect(SelectRelation stmt, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            SelectList selectList = stmt.getSelectList();
            sqlBuilder.append("SELECT ");
            if (selectList.isDistinct()) {
                sqlBuilder.append("DISTINCT ");
            }

            List<String> selectListString = new ArrayList<>();
            for (int i = 0; i < selectList.getItems().size(); ++i) {

                SelectListItem item = selectList.getItems().get(i);
                Expr expr = item.getExpr();
                String aliasName = item.getAlias() == null ? null : StringUtils.lowerCase(item.getAlias());

                if (item.isStar()) {
                    selectListString.add(
                            item.getTblName() == null ? "*" : desensitizeTableName(item.getTblName()) + ".*");
                } else if (expr instanceof FieldReference) {
                    Field field = stmt.getScope().getRelationFields().getFieldByIndex(i);
                    selectListString.add(
                            desensitizeColumnName(field.getRelationAlias(), field.getName(), aliasName));
                } else if (expr instanceof SlotRef) {
                    SlotRef slot = (SlotRef) expr;
                    if (slot.getOriginType().isStructType()) {
                        selectListString.add(desensitizeStructColumnName(slot.getTblNameWithoutAnalyzed(),
                                slot.getColumnName(), aliasName));
                    } else {
                        selectListString.add(
                                desensitizeColumnName(slot.getTblNameWithoutAnalyzed(), slot.getColumnName(),
                                        aliasName));
                    }
                } else {
                    selectListString.add(StringUtils.isEmpty(aliasName) ?
                            visit(expr) :
                            visit(expr) + " AS " + desensitizeValue(aliasName, COLUMN_ALIAS));
                }
            }

            sqlBuilder.append(Joiner.on(", ").join(selectListString));

            String fromClause = visit(stmt.getRelation());
            if (fromClause != null) {
                sqlBuilder.append("\nFROM ");
                sqlBuilder.append(fromClause);
            }

            if (stmt.hasWhereClause()) {
                sqlBuilder.append("\nWHERE ");
                sqlBuilder.append(visit(stmt.getWhereClause()));
            }

            if (stmt.hasGroupByClause()) {
                sqlBuilder.append("\nGROUP BY ");
                sqlBuilder.append(visit(stmt.getGroupByClause()));
            }

            if (stmt.hasHavingClause()) {
                sqlBuilder.append("\nHAVING ");
                sqlBuilder.append(visit(stmt.getHavingClause()));
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitCTE(CTERelation relation, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("tbl_" + desensitizeValue(relation.getName(), "cte"));

            if (relation.isResolvedInFromClause()) {
                if (relation.getAlias() != null) {
                    sqlBuilder.append(" AS ").append("tbl_")
                            .append(desensitizeValue(relation.getAlias().getTbl(), "cte alias"));
                }
                return sqlBuilder.toString();
            }

            if (relation.getColumnOutputNames() != null) {
                sqlBuilder.append(" (")
                        .append(Joiner.on(", ").join(
                                relation.getColumnOutputNames()
                                        .stream()
                                        .map(c -> desensitizeValue(StringUtils.lowerCase(c), COLUMN))
                                        .collect(toList())))
                        .append(")");
            }
            sqlBuilder.append(" AS (").append(visit(relation.getCteQueryStatement())).append(") ");
            return sqlBuilder.toString();
        }

        @Override
        public String visitView(ViewRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(desensitizeTableName(node.getName()));

            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("tbl_")
                        .append(desensitizeValue(node.getAlias().getTbl(), TABLE_ALIAS));
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTable(TableRelation node, Void outerScope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(desensitizeTableName(node.getName()));

            for (TableRelation.TableHint hint : CollectionUtils.emptyIfNull(node.getTableHints())) {
                sqlBuilder.append(" [");
                sqlBuilder.append(hint.name());
                sqlBuilder.append("] ");
            }

            if (node.getPartitionNames() != null) {
                List<String> partitionNames = node.getPartitionNames().getPartitionNames();
                if (partitionNames != null && !partitionNames.isEmpty()) {
                    sqlBuilder.append(" PARTITION(");
                }
                for (String partitionName : partitionNames) {
                    sqlBuilder.append("'").append(partitionName).append("'").append(",");
                }
                sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
                sqlBuilder.append(")");
            }
            if (node.getAlias() != null) {
                sqlBuilder.append(" AS ");
                sqlBuilder.append("tbl_")
                        .append(desensitizeValue(node.getAlias().getTbl(), TABLE_ALIAS));
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitTableFunction(TableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();

            sqlBuilder.append(node.getFunctionName());
            sqlBuilder.append("(");

            List<String> childSql = node.getChildExpressions().stream().map(this::visit).collect(toList());
            sqlBuilder.append(Joiner.on(",").join(childSql));

            sqlBuilder.append(")");
            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append(node.getAlias().getTbl());

                if (node.getColumnOutputNames() != null) {
                    sqlBuilder.append("(");
                    String names = node.getColumnOutputNames().stream().collect(Collectors.joining(","));
                    sqlBuilder.append(names);
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("TABLE(");

            TableFunctionRelation tableFunction = (TableFunctionRelation) node.getRight();
            sqlBuilder.append(tableFunction.getFunctionName());
            sqlBuilder.append("(");
            sqlBuilder.append(
                    tableFunction.getChildExpressions().stream().map(this::visit).collect(Collectors.joining(",")));
            sqlBuilder.append(")");
            sqlBuilder.append(")"); // TABLE(

            if (tableFunction.getAlias() != null) {
                sqlBuilder.append(" ").append(tableFunction.getAlias().getTbl());
                if (tableFunction.getColumnOutputNames() != null) {
                    sqlBuilder.append("(");
                    String names = tableFunction.getColumnOutputNames().stream().map(c -> c)
                            .collect(Collectors.joining(","));
                    sqlBuilder.append(names);
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitSubqueryRelation(SubqueryRelation node, Void context) {
            StringBuilder sqlBuilder = new StringBuilder("(" + visit(node.getQueryStatement()) + ")");

            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append("tbl_" + desensitizeValue(node.getAlias().getTbl(), TABLE_ALIAS));

                if (node.getExplicitColumnNames() != null) {
                    sqlBuilder.append("(");
                    sqlBuilder.append(Joiner.on(",")
                            .join(desensitizeValues(
                                    node.getExplicitColumnNames().stream().map(String::toLowerCase).collect(toList()),
                                    COLUMN_ALIAS)));
                    sqlBuilder.append(")");
                }
            }
            return sqlBuilder.toString();
        }

        @Override
        public String visitValues(ValuesRelation node, Void scope) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (node.isNullValues()) {
                return null;
            }

            sqlBuilder.append("(VALUES");
            List<String> values = new ArrayList<>();
            for (int i = 0; i < node.getRows().size(); ++i) {
                StringBuilder rowBuilder = new StringBuilder();
                rowBuilder.append("(");
                List<String> rowStrings =
                        node.getRows().get(i).stream().map(this::visit).collect(Collectors.toList());
                rowBuilder.append(Joiner.on(", ").join(rowStrings));
                rowBuilder.append(")");
                values.add(rowBuilder.toString());
            }
            sqlBuilder.append(Joiner.on(", ").join(values));
            sqlBuilder.append(")");
            if (node.getAlias() != null) {
                sqlBuilder.append(" ").append("tbl_").append(
                        desensitizeValue(StringUtils.lowerCase(node.getAlias().getTbl()),
                                COLUMN));

                if (node.getExplicitColumnNames() != null) {
                    sqlBuilder.append("(");
                    sqlBuilder.append(Joiner.on(",").join(
                            desensitizeValues(node.getExplicitColumnNames()
                                    .stream()
                                    .map(StringUtils::lowerCase).collect(toList()),
                                    COLUMN_ALIAS)));
                    sqlBuilder.append(")");
                }
            }

            return sqlBuilder.toString();
        }

        @Override
        public String visitSubfieldExpr(SubfieldExpr node, Void context) {
            StringJoiner joiner = new StringJoiner(".");
            joiner.add(visit(node.getChild(0)));
            for (String fieldName : node.getFieldNames()) {
                joiner.add(desensitizeValue(fieldName, "field"));
            }
            return joiner.toString();
        }

        @Override
        public String visitSlot(SlotRef expr, Void context) {
            if (expr.getOriginType().isStructType()) {
                return desensitizeStructColumnName(expr.getTblNameWithoutAnalyzed(),
                        expr.getColumnName(), expr.getColumnName());
            } else {
                return desensitizeColumnName(expr.getTblNameWithoutAnalyzed(),
                        expr.getColumnName(), expr.getColumnName());
            }
        }

        public String desensitizeViewDef(QueryStatement stmt) {
            StringBuilder sb = new StringBuilder();
            sb.append(visit(stmt)).append(";");
            return sb.toString();
        }

        public String desensitizeExternalTableDef(String dbName, Table table) {
            StringBuilder sb = new StringBuilder();
            String desensitizedDbName = "db_" + desensitizeValue(dbName, "db name");
            String desensitizedTblName = "tbl_" + desensitizeValue(table.getName(), "table name");
            sb.append("CREATE EXTERNAL TABLE ").append(desensitizedDbName).append(".");
            sb.append(desensitizedTblName).append(" (\n");

            List<String> colDefs = Lists.newArrayList();
            for (Column col : table.getBaseSchema()) {
                colDefs.add(desensitizeColumnDef(col, table));
            }

            sb.append(Joiner.on(",\n").join(colDefs));
            sb.append("\n) ENGINE= ").append(table.getType().name()).append(" ");

            if (table.getType() == Table.TableType.MYSQL) {
                // properties
                sb.append("\nPROPERTIES (\n");
                sb.append("\"host\" = \"").append("localhost").append("\",\n");
                sb.append("\"port\" = \"").append("3306").append("\",\n");
                sb.append("\"user\" = \"").append("root").append("\",\n");
                sb.append("\"password\" = \"").append("\",\n");
                sb.append("\"database\" = \"").append(desensitizedDbName).append("\",\n");
                sb.append("\"table\" = \"").append(desensitizedTblName).append("\"\n");
                sb.append(")");
            } else if (table.getType() == Table.TableType.ELASTICSEARCH) {
                EsTable esTable = (EsTable) table;
                // properties
                sb.append("\nPROPERTIES (\n");
                sb.append("\"hosts\" = \"").append(esTable.getHosts()).append("\",\n");
                sb.append("\"user\" = \"").append(esTable.getUserName()).append("\",\n");
                sb.append("\"password\" = \"").append("\",\n");
                sb.append("\"index\" = \"").append(esTable.getIndexName()).append("\",\n");
            } else if (table.getType() == Table.TableType.HIVE) {
                HiveTable hiveTable = (HiveTable) table;
                // properties
                sb.append("\nPROPERTIES (\n");
                sb.append("\"database\" = \"").append(desensitizedDbName).append("\",\n");
                sb.append("\"table\" = \"").append(desensitizedTblName).append("\",\n");
                sb.append("\"resource\" = \"").append(hiveTable.getResourceName()).append("\"");
                if (!table.getProperties().isEmpty()) {
                    sb.append(",\n");
                }
                sb.append(new PrintableMap<>(hiveTable.getProperties(), " = ", true, true, false).toString());
                sb.append("\n)");
            } else if (table.getType() == Table.TableType.FILE) {
                FileTable fileTable = (FileTable) table;
                Map<String, String> clonedFileProperties = new HashMap<>(fileTable.getFileProperties());
                CredentialUtil.maskCredential(clonedFileProperties);

                sb.append("\nPROPERTIES (\n");
                sb.append(new PrintableMap<>(clonedFileProperties, " = ", true, true, false).toString());
                sb.append("\n)");
            } else if (table.getType() == Table.TableType.HUDI) {
                HudiTable hudiTable = (HudiTable) table;

                // properties
                sb.append("\nPROPERTIES (\n");
                sb.append("\"database\" = \"").append(desensitizedDbName).append("\",\n");
                sb.append("\"table\" = \"").append(desensitizedTblName).append("\",\n");
                sb.append("\"resource\" = \"").append(hudiTable.getResourceName()).append("\"");
                sb.append("\n)");
            } else if (table.getType() == Table.TableType.ICEBERG) {
                IcebergTable icebergTable = (IcebergTable) table;

                // properties
                sb.append("\nPROPERTIES (\n");
                sb.append("\"database\" = \"").append(desensitizedDbName).append("\",\n");
                sb.append("\"table\" = \"").append(desensitizedTblName).append("\",\n");
                sb.append("\"resource\" = \"").append(icebergTable.getResourceName()).append("\"");
                sb.append("\n)");
            } else if (table.getType() == Table.TableType.JDBC) {
                JDBCTable jdbcTable = (JDBCTable) table;
                // properties
                sb.append("\nPROPERTIES (\n");
                sb.append("\"resource\" = \"").append(jdbcTable.getResourceName()).append("\",\n");
                sb.append("\"table\" = \"").append(desensitizedTblName).append("\"");
                sb.append("\n)");
            }
            sb.append(";");
            return sb.toString();
        }

        public String desensitizeMvDef(Table table) {
            MaterializedView materializedView = (MaterializedView) table;
            String desensitizedTblName = "tbl_" + desensitizeValue(materializedView.getName(), "table name");
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE MATERIALIZED VIEW `").append(desensitizedTblName).append("` (");
            List<String> colDef = Lists.newArrayList();
            for (Column column : materializedView.getBaseSchema()) {
                StringBuilder colSb = new StringBuilder();
                // Since mv supports complex expressions as the output column, add `` to support to replay it.
                colSb.append("`" + desensitizeValue(StringUtils.lowerCase(column.getName()), "column name") + "`");
                colDef.add(colSb.toString());
            }
            sb.append(Joiner.on(", ").join(colDef));
            sb.append(")");

            // partition
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            if (!(partitionInfo instanceof SinglePartitionInfo)) {
                sb.append("\n").append(desensitizePartitionInfo(materializedView, partitionInfo));
            }

            // distribution
            DistributionInfo distributionInfo = materializedView.getDefaultDistributionInfo();
            sb.append("\n").append(desensitizeDistributionInfo(table.getIdToColumn(), distributionInfo));

            // refresh scheme
            sb.append("\nREFRESH ").append("MANUAL");
            // properties
            sb.append("\nPROPERTIES (\n");

            // replicationNum
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM).append("\" = \"");
            sb.append(1).append("\"");

            Map<String, String> properties = materializedView.getProperties();

            // excluded trigger tables
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)
                        .append("\" = \"");
                sb.append(properties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)).append("\"");
            }

            // force_external_table_query_rewrite
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(
                        PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE).append("\" = \"");
                sb.append(properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)).append("\"");
            }

            // mv_rewrite_staleness
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(
                        PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND).append("\" = \"");
                sb.append(properties.get(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)).append("\"");
            }

            // unique constraints
            // unique constraint
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)
                    && !Strings.isNullOrEmpty(properties.get(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT))) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)
                        .append("\" = \"");
                final List<String> cols = Lists.newArrayList();
                materializedView.getTableProperty().getUniqueConstraints()
                        .forEach(e -> cols.addAll(e.getUniqueColumnNames(materializedView)));
                List<String> desensitizedCols = Lists.newArrayList();
                cols.forEach(e -> desensitizedCols.add(desensitizeValue(e, COLUMN)));
                sb.append(Joiner.on(", ").join(desensitizedCols)).append("\"");
            }

            // TODO: foreign key constraint


            // colocateTable
            String colocateGroup = materializedView.getColocateGroup();
            if (colocateGroup != null) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)
                        .append("\" = \"");
                sb.append(colocateGroup).append("\"");
            }

            // session properties
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX)) {
                    sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(entry.getKey())
                            .append("\" = \"").append(entry.getValue()).append("\"");
                }
            }

            sb.append("\n)");
            String define = materializedView.getSimpleDefineSql();
            if (StringUtils.isEmpty(define) || !simple) {
                define = materializedView.getViewDefineSql();
            }
            StatementBase stmt = SqlParser.parse(define, SessionVariable.DEFAULT_SESSION_VARIABLE).get(0);
            Analyzer.analyze(stmt, ConnectContext.get());
            sb.append("\nAS ").append(visit(stmt));
            sb.append(";");
            return sb.toString();
        }

        public String desensitizeOlapTableDef(String dbName, OlapTable olapTable) {
            StringBuilder sb = new StringBuilder();
            if (olapTable.getType() == Table.TableType.OLAP_EXTERNAL) {
                sb.append("CREATE EXTERNAL TABLE ").append("db_" + desensitizeValue(dbName, "db name")).append(".");
            } else {
                sb.append("CREATE TABLE ").append("db_" + desensitizeValue(dbName, "db name")).append(".");
            }

            sb.append("tbl_")
                    .append(desensitizeValue(olapTable.getName(), "table name"))
                    .append(" (\n");
            List<String> colDefs = Lists.newArrayList();
            for (Column col : olapTable.getBaseSchema()) {
                colDefs.add(desensitizeColumnDef(col, olapTable));
            }

            sb.append(Joiner.on(",\n").join(colDefs));

            if (CollectionUtils.isNotEmpty(olapTable.getIndexes())) {
                for (Index index : olapTable.getIndexes()) {
                    sb.append(",\n");
                    sb.append("  ").append(desensitizeIndexDef(olapTable, index));
                }
            }

            sb.append("\n) ENGINE= ");
            sb.append(olapTable.getType() == Table.TableType.CLOUD_NATIVE ? "OLAP" : olapTable.getType().name()).append(" ");

            // keys
            sb.append("\n").append(olapTable.getKeysType().toSql()).append("(");
            List<String> keysColumnNames = Lists.newArrayList();
            for (Column column : olapTable.getBaseSchema()) {
                if (column.isKey()) {
                    keysColumnNames.add(desensitizeValue(StringUtils.lowerCase(column.getName()), " column"));
                }
            }
            sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");
            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            sb.append(desensitizePartitionInfo(olapTable, partitionInfo));

            // distribution
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            sb.append(desensitizeDistributionInfo(olapTable.getIdToColumn(), distributionInfo));

            // order by
            MaterializedIndexMeta index = olapTable.getIndexMetaByIndexId(olapTable.getBaseIndexId());
            if (index.getSortKeyIdxes() != null) {
                sb.append("\nORDER BY(");
                List<String> sortKeysColumnNames = Lists.newArrayList();
                for (Integer i : index.getSortKeyIdxes()) {
                    sortKeysColumnNames.add(desensitizeValue(
                            StringUtils.lowerCase(olapTable.getBaseSchema().get(i).getName()), COLUMN)
                    );
                }
                sb.append(Joiner.on(", ").join(sortKeysColumnNames)).append(")");
            }

            // properties
            sb.append("\nPROPERTIES (\n");

            // replicationNum
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM).append("\" = \"");
            sb.append(1).append("\"");

            // bloom filter
            Set<String> bfColumnNames = olapTable.getBfColumnNames();
            if (bfColumnNames != null) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                        .append("\" = \"");
                List<String> desensitizedCols = olapTable.getBfColumnNames().stream()
                        .map(e -> desensitizeValue(e, COLUMN)).
                        collect(toList());
                sb.append(Joiner.on(", ").join(desensitizedCols)).append("\"");
            }

            // colocateTable
            String colocateTable = olapTable.getColocateGroup();
            if (colocateTable != null) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)
                        .append("\" = \"");
                sb.append(colocateTable).append("\"");
            }

            // dynamic partition
            if (olapTable.dynamicPartitionExists()) {
                sb.append(olapTable.getTableProperty().getDynamicPartitionProperty().toString());
            }

            String partitionDuration =
                    olapTable.getTableProperty().getProperties()
                            .get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION);
            if (partitionDuration != null) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR)
                        .append(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)
                        .append("\" = \"")
                        .append(partitionDuration).append("\"");
            }


            Map<String, String> properties = olapTable.getTableProperty().getProperties();

            // unique constraint
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)
                    && !Strings.isNullOrEmpty(properties.get(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT))) {
                sb.append(StatsConstants.TABLE_PROPERTY_SEPARATOR).append(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)
                        .append("\" = \"");
                final List<String> cols = Lists.newArrayList();
                olapTable.getTableProperty().getUniqueConstraints()
                        .forEach(e -> cols.addAll(e.getUniqueColumnNames(olapTable)));
                List<String> desensitizedCols = Lists.newArrayList();
                cols.forEach(e -> desensitizedCols.add(desensitizeValue(e, COLUMN)));
                sb.append(Joiner.on(", ").join(desensitizedCols)).append("\"");
            }

            // TODO: foreign key constraint

            sb.append("\n);");
            return sb.toString();
        }

        private String desensitizeColumnDef(Column column, Table table) {
            StringBuilder sb = new StringBuilder();
            sb.append(desensitizeValue(StringUtils.lowerCase(column.getName()), COLUMN)).append(" ");
            String typeStr = column.getType().toSql();
            sb.append(typeStr).append(" ");
            if (table instanceof OlapTable && column.isAggregated() && !column.isAggregationTypeImplicit() &&
                    ((OlapTable) table).getKeysType() != KeysType.PRIMARY_KEYS) {
                sb.append(column.getAggregationType().name()).append(" ");
            }

            if (!column.isAllowNull()) {
                sb.append("NOT NULL ");
            }

            if (column.getDefaultExpr() == null && column.isAutoIncrement()) {
                sb.append("AUTO_INCREMENT ");
            } else if (column.getDefaultExpr() != null) {
                if ("now()".equalsIgnoreCase(column.getDefaultExpr().getExpr())) {
                    // compatible with mysql
                    sb.append("DEFAULT ").append("CURRENT_TIMESTAMP").append(" ");
                } else {
                    sb.append("DEFAULT ").append("(").append(column.getDefaultExpr().getExpr()).append(") ");
                }
            } else if (column.getDefaultValue() != null && column.getPrimitiveType() != PrimitiveType.HLL &&
                    column.getPrimitiveType() != PrimitiveType.BITMAP) {
                sb.append("DEFAULT \"").append(column.getDefaultValue()).append("\" ");
            } else if (column.isGeneratedColumn()) {
                sb.append("AS ").append(visit(column.getGeneratedColumnExpr(table.getIdToColumn())));
            }
            return sb.toString();
        }

        private String desensitizeIndexDef(Table table, Index index) {
            StringBuilder sb = new StringBuilder("INDEX ");
            sb.append(index.getIndexName());
            sb.append(" (");
            List<String> indexCols = Lists.newArrayList();
            for (String col : MetaUtils.getColumnNamesByColumnIds(table, index.getColumns())) {
                indexCols.add(desensitizeValue(StringUtils.lowerCase(col), COLUMN));
            }
            sb.append(Joiner.on(", ").join(indexCols));
            sb.append(")");
            if (index.getIndexType() != null) {
                sb.append(" USING ").append(index.getIndexType().toString());
            }
            return sb.toString();
        }

        private String desensitizePartitionInfo(OlapTable olapTable, PartitionInfo partitionInfo) {
            if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
                String partition = partitionInfo.toSql(olapTable, null);
                int startIdx = partition.indexOf("(");
                int endIdx = partition.indexOf(")");
                String colsString = partition.substring(startIdx + 1, endIdx);
                String[] cols = colsString.split(", ");
                String desensitizeCols = Arrays.stream(cols)
                        .map(e -> desensitizeValue(StringUtils.lowerCase(e.substring(1, e.length() - 1))))
                        .collect(Collectors.joining(", "));

                return "\n" + partition.substring(0, startIdx + 1) + desensitizeCols + partition.substring(endIdx);
            } else {
                return "";
            }
        }

        private String desensitizeDistributionInfo(Map<ColumnId, Column> schema, DistributionInfo distributionInfo) {
            if (distributionInfo instanceof HashDistributionInfo) {
                String distribution = distributionInfo.toSql(schema);
                int startIdx = distribution.indexOf("(");
                int endIdx = distribution.indexOf(")");
                String colsString = distribution.substring(startIdx + 1, endIdx);
                String[] cols = colsString.split(", ");
                String desensitizeCols = Arrays.stream(cols)
                        .map(e -> desensitizeValue(StringUtils.lowerCase(e.substring(1, e.length() - 1))))
                        .collect(Collectors.joining(", "));
                return "\n" + distribution.substring(0, startIdx + 1) + desensitizeCols + distribution.substring(endIdx);
            } else {
                return "\n" + distributionInfo.toSql(schema);
            }
        }

        private String desensitizeColumnName(TableName tableName, String fieldName, String aliasName) {
            String res = "";
            if (tableName != null && !withoutTbl) {
                if (!simple) {
                    res = desensitizeTableName(tableName);
                } else {
                    res = "tbl_" + desensitizeValue(tableName.getTbl(), "table");
                }
                res += ".";
            }

            res += desensitizeValue(StringUtils.lowerCase(fieldName), COLUMN);
            if (StringUtils.isNotEmpty(aliasName) && !StringUtils.equalsIgnoreCase(fieldName, aliasName)) {
                res += " AS " + desensitizeValue(StringUtils.lowerCase(aliasName), COLUMN_ALIAS);
            }
            return res;
        }

        private String desensitizeTableName(@NotNull TableName tableName) {
            StringBuilder stringBuilder = new StringBuilder();
            if (tableName.getCatalog() != null && !CatalogMgr.isInternalCatalog(tableName.getCatalog())) {
                stringBuilder.append("catalog_")
                        .append(desensitizeValue(tableName.getCatalog()))
                        .append(".");
            }
            if (tableName.getDb() != null) {
                stringBuilder.append("db_").append(desensitizeValue(tableName.getDb())).append(".");
            }

            stringBuilder.append("tbl_").append(desensitizeValue(tableName.getTbl()));
            return stringBuilder.toString();
        }

        private String desensitizeStructColumnName(TableName tableName, String fieldName, String aliasName) {
            StringBuilder stringBuilder = new StringBuilder();
            if (tableName != null) {
                stringBuilder.append(desensitizeTableName(tableName)).append(".");
            }

            fieldName = desensitizeStructField(fieldName);
            stringBuilder.append(fieldName);
            if (aliasName == null) {
                return stringBuilder.toString();
            }

            aliasName = desensitizeStructField(aliasName);
            if (!fieldName.equalsIgnoreCase(aliasName)) {
                stringBuilder.append(" AS `").append(aliasName).append("`");
            }
            return stringBuilder.toString();
        }

        private String desensitizeStructField(String name) {
            String[] fields = name.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                sb.append(desensitizeValue(StringUtils.lowerCase(fields[i]), COLUMN));
                if (i < fields.length - 1) {
                    sb.append(".");
                }
            }
            return sb.toString();
        }

        private String desensitizeValue(String key) {
            return desensitizeValue(key, "");
        }

        private String desensitizeValue(String key, String desc) {
            Preconditions.checkState(desensitizedDict.containsKey(key),
                    "%s %s not be desensitized", desc, key);
            return desensitizedDict.get(key);
        }

        private List<String> desensitizeValues(List<String> keys, String desc) {
            return keys.stream().map(e -> desensitizeValue(e, desc)).collect(toList());
        }

    }
}
