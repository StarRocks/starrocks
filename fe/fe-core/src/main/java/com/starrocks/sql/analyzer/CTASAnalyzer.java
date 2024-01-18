// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CTASAnalyzer {
    public static void analyze(CreateTableAsSelectStmt createTableAsSelectStmt, ConnectContext session) {
        List<String> columnNames = createTableAsSelectStmt.getColumnNames();
        QueryStatement queryStatement = createTableAsSelectStmt.getQueryStatement();
        CreateTableStmt createTableStmt = createTableAsSelectStmt.getCreateTableStmt();

        if (createTableStmt.isExternal()) {
            throw new SemanticException("CTAS does not support create external table");
        }

        Analyzer.analyze(queryStatement, session);

        // Pair<TableName, Pair<ColumnName, ColumnAlias>>
        Map<Pair<String, Pair<String, String>>, Table> columnNameToTable = Maps.newHashMap();
        Map<TableName, Table> tables = AnalyzerUtils.collectAllTableWithAlias(queryStatement);
        Map<String, Table> tableRefToTable = new HashMap<>();
        for (Map.Entry<TableName, Table> t : tables.entrySet()) {
            tableRefToTable.put(t.getKey().getTbl(), t.getValue());
        }

        List<Field> allFields = queryStatement.getQueryRelation().getRelationFields().getAllFields();
        List<String> finalColumnNames = Lists.newArrayList();

        if (columnNames != null) {
            if (columnNames.size() != allFields.size()) {
                throw new SemanticException("Number of select columns [%d] don't equal query statement's columns [%d]",
                        columnNames.size(), allFields.size());
            }
            finalColumnNames.addAll(columnNames);
        } else {
            for (Field allField : allFields) {
                finalColumnNames.add(allField.getName());
            }
        }

        boolean isPKTable = false;
        KeysDesc keysDesc = createTableStmt.getKeysDesc();
        if (keysDesc != null) {
            KeysType keysType = keysDesc.getKeysType();
            if (keysType == KeysType.PRIMARY_KEYS) {
                isPKTable = true;
            } else if (keysType != KeysType.DUP_KEYS) {
                throw new SemanticException("CTAS does not support %s table", keysDesc.getKeysType().toString());
            }
        }

        for (int i = 0; i < allFields.size(); i++) {
            Type type = AnalyzerUtils.transformType(allFields.get(i).getType());
            Expr originExpression = allFields.get(i).getOriginExpression();
            ColumnDef columnDef = new ColumnDef(finalColumnNames.get(i), new TypeDef(type), false,
                        null, originExpression.isNullable(), ColumnDef.DefaultValueDef.NOT_SET, "");
            if (isPKTable && keysDesc.containsCol(finalColumnNames.get(i))) {
                columnDef.setAllowNull(false);
            }
            createTableStmt.addColumnDef(columnDef);
            if (originExpression instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) originExpression;
                // lateral json_each(parse_json(c1)) will return null
                if (slotRef.getTblNameWithoutAnalyzed() == null) {
                    continue;
                }
                String tableName = slotRef.getTblNameWithoutAnalyzed().getTbl();
                Table table = tableRefToTable.get(tableName);
                if (!(table instanceof OlapTable)) {
                    continue;
                }
                columnNameToTable.put(new Pair<>(tableName,
                        new Pair<>(slotRef.getColumnName(), allFields.get(i).getName())), table);
            }
        }

        // For replication_num, The behavior is the same as creating a table
        int defaultReplicationNum = FeConstants.default_replication_num;

        Map<String, String> stmtProperties = createTableStmt.getProperties();
        if (null == stmtProperties) {
            Map<String, String> properties = Maps.newHashMap();
            properties.put("replication_num", String.valueOf(defaultReplicationNum));
            createTableStmt.setProperties(properties);
        } else if (!stmtProperties.containsKey("replication_num")) {
            stmtProperties.put("replication_num", String.valueOf(defaultReplicationNum));
        }

        // For HashDistributionDesc key
        // If we have statistics cache, we pick the column with the highest cardinality in the statistics,
        // if we don't, we pick the first column
        if (null == createTableStmt.getDistributionDesc()) {
            String defaultColumnName = finalColumnNames.get(0);
            double candidateDistinctCountCount = 1.0;
            StatisticStorage currentStatisticStorage = GlobalStateMgr.getCurrentStatisticStorage();

            for (Map.Entry<Pair<String, Pair<String, String>>, Table> columnEntry : columnNameToTable.entrySet()) {
                Pair<String, String> columnName = columnEntry.getKey().second;
                ColumnStatistic columnStatistic = currentStatisticStorage.getColumnStatistic(
                        columnEntry.getValue(), columnName.first);
                double curDistinctValuesCount = columnStatistic.getDistinctValuesCount();
                if (curDistinctValuesCount > candidateDistinctCountCount) {
                    defaultColumnName = columnName.second;
                    candidateDistinctCountCount = curDistinctValuesCount;
                }
            }

            DistributionDesc distributionDesc =
                    new HashDistributionDesc(0, Lists.newArrayList(defaultColumnName));
            createTableStmt.setDistributionDesc(distributionDesc);


        }

        Analyzer.analyze(createTableStmt, session);

        InsertStmt insertStmt = createTableAsSelectStmt.getInsertStmt();
        insertStmt.setQueryStatement(queryStatement);
    }

}
