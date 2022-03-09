// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;

import java.util.List;
import java.util.Map;

public class CTASAnalyzer {

    @Deprecated
    public static void transformCTASStmt(CreateTableAsSelectStmt createTableAsSelectStmt, ConnectContext session) {
        List<String> columnNames = createTableAsSelectStmt.getColumnNames();
        QueryStatement queryStatement = createTableAsSelectStmt.getQueryStatement();
        CreateTableStmt createTableStmt = createTableAsSelectStmt.getCreateTableStmt();

        if (createTableStmt.isExternal()) {
            throw new SemanticException("CTAS does not support create external table");
        }

        Analyzer.analyze(queryStatement, session);

        // Pair<TableName, Pair<ColumnName, ColumnAlias>>
        Map<Pair<String, Pair<String, String>>, Table> columnNameToTable = Maps.newHashMap();
        Map<String, Table> tableRefToTable = AnalyzerUtils.collectAllTable(queryStatement);

        // For replication_num, we select the maximum value of all tables replication_num
        int defaultReplicationNum = 1;

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

        for (int i = 0; i < allFields.size(); i++) {
            Type type = transformType(allFields.get(i).getType());
            ColumnDef columnDef = new ColumnDef(finalColumnNames.get(i), new TypeDef(type), false,
                    null, true, ColumnDef.DefaultValueDef.NOT_SET, "");
            createTableStmt.addColumnDef(columnDef);
            Expr originExpression = allFields.get(i).getOriginExpression();
            if (originExpression instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) originExpression;
                String tableName = slotRef.getTblNameWithoutAnalyzed().getTbl();
                Table table = tableRefToTable.get(tableName);
                columnNameToTable.put(new Pair<>(tableName,
                        new Pair<>(slotRef.getColumnName(), allFields.get(i).getName())), table);
            }
        }

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
            StatisticStorage currentStatisticStorage = Catalog.getCurrentStatisticStorage();

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

            int defaultBucket = 10;
            DistributionDesc distributionDesc =
                    new HashDistributionDesc(defaultBucket, Lists.newArrayList(defaultColumnName));
            createTableStmt.setDistributionDesc(distributionDesc);
        }

        CreateTableAnalyzer.transformCreateTableStmt(createTableStmt, session);

        InsertStmt insertStmt = createTableAsSelectStmt.getInsertStmt();
        insertStmt.setQueryStatement(queryStatement);
    }

    // For char and varchar types, use the inferred length if the length can be inferred,
    // otherwise use the longest varchar value.
    // For double and float types, since they may be selected as key columns,
    // the key column must be an exact value, so we unified into a default decimal type.
    private static Type transformType(Type srcType) {
        Type newType;
        if (srcType.isScalarType()) {
            if (PrimitiveType.VARCHAR == srcType.getPrimitiveType() ||
                    PrimitiveType.CHAR == srcType.getPrimitiveType()) {
                int len = ScalarType.MAX_VARCHAR_LENGTH;
                if (srcType instanceof ScalarType) {
                    ScalarType scalarType = (ScalarType) srcType;
                    if (scalarType.getLength() > 0 && scalarType.isAssignedStrLenInColDefinition()) {
                        len = scalarType.getLength();
                    }
                }
                ScalarType stringType = ScalarType.createVarcharType(len);
                stringType.setAssignedStrLenInColDefinition();
                newType = stringType;
            } else if (PrimitiveType.FLOAT == srcType.getPrimitiveType() ||
                    PrimitiveType.DOUBLE == srcType.getPrimitiveType()) {
                newType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
            } else if (PrimitiveType.DECIMAL128 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL64 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL32 == srcType.getPrimitiveType()) {
                newType = ScalarType.createDecimalV3Type(srcType.getPrimitiveType(),
                        srcType.getPrecision(), srcType.getDecimalDigits());
            } else {
                newType = ScalarType.createType(srcType.getPrimitiveType());
            }
        } else if (srcType.isArrayType()) {
            newType = new ArrayType(transformType(((ArrayType) srcType).getItemType()));
        } else {
            throw new SemanticException("Unsupported CTAS transform type: %s", srcType.getPrimitiveType());
        }
        return newType;
    }

}
