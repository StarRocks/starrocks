// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
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
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.SelectStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.relation.QueryRelation;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;

import java.util.List;
import java.util.Map;

public class CTASAnalyzer {
    private final Catalog catalog;
    private final ConnectContext session;

    public CTASAnalyzer(Catalog catalog, ConnectContext session) {
        this.catalog = catalog;
        this.session = session;
    }

    @Deprecated
    public Relation transformCTASStmt(CreateTableAsSelectStmt createTableAsSelectStmt) {
        List<String> columnNames = createTableAsSelectStmt.getColumnNames();
        QueryStmt queryStmt = createTableAsSelectStmt.getQueryStmt();
        CreateTableStmt createTableStmt = createTableAsSelectStmt.getCreateTableStmt();

        if (createTableStmt.isExternal()) {
            throw new SemanticException("CTAS does not support create external table");
        }

        QueryRelation queryRelation = new QueryAnalyzer(catalog, session)
                .transformQueryStmt(queryStmt, new Scope(RelationId.anonymous(), new RelationFields()));

        // Pair<TableName, ColumnName>
        Map<Pair<String, String>, Table> columnNameToTable = Maps.newHashMap();
        Map<String, Table> tableRefToTable = Maps.newHashMap();

        // For replication_num, we select the maximum value of all tables replication_num
        int defaultReplicationNum = 1;
        List<TableRef> tableRefs = ((SelectStmt) queryStmt).getTableRefs();
        for (TableRef tableRef : tableRefs) {
            String[] aliases = tableRef.getAliases();
            Table table = catalog.getDb(tableRef.getName().getDb()).getTable(tableRef.getName().getTbl());
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                Short replicationNum = olapTable.getDefaultReplicationNum();
                if (replicationNum > defaultReplicationNum) {
                    defaultReplicationNum = replicationNum;
                }
            }
            if (aliases != null) {
                for (String alias : aliases) {
                    tableRefToTable.put(alias, table);
                }
            }
        }

        List<Field> allFields = queryRelation.getRelationFields().getAllFields();
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

        ScalarType stringType = Type.STRING;
        stringType.setAssignedStrLenInColDefinition();
        for (int i = 0; i < allFields.size(); i++) {
            Type type = allFields.get(i).getType();
            if (PrimitiveType.VARCHAR == type.getPrimitiveType()) {
                type = stringType;
            } else if (PrimitiveType.DOUBLE == type.getPrimitiveType()) {
                type = Type.DEFAULT_DECIMAL128;
            }
            ColumnDef columnDef = new ColumnDef(finalColumnNames.get(i), new TypeDef(type), false,
                    null, true, ColumnDef.DefaultValueDef.NOT_SET, "");
            createTableStmt.addColumnDef(columnDef);
            Expr originExpression = allFields.get(i).getOriginExpression();
            if (originExpression instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) originExpression;
                String tableName = slotRef.getTblNameWithoutAnalyzed().getTbl();
                Table table = tableRefToTable.get(tableName);
                columnNameToTable.put(new Pair<>(tableName, slotRef.getColumnName()), table);
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

            for (Map.Entry<Pair<String, String>, Table> columnEntry : columnNameToTable.entrySet()) {
                String columnName = columnEntry.getKey().second;
                ColumnStatistic columnStatistic = currentStatisticStorage.getColumnStatistic(
                        columnEntry.getValue(), columnName);
                double curDistinctValuesCount = columnStatistic.getDistinctValuesCount();
                if (curDistinctValuesCount > candidateDistinctCountCount) {
                    defaultColumnName = columnName;
                    candidateDistinctCountCount = curDistinctValuesCount;
                }
            }

            int defaultBucket = 10;
            DistributionDesc distributionDesc = new HashDistributionDesc(defaultBucket, Lists.newArrayList(defaultColumnName));
            createTableStmt.setDistributionDesc(distributionDesc);
        }

        Relation relation = new CreateTableAnalyzer(catalog, session).transformCreateTableStmt(createTableStmt);

        InsertStmt insertStmt = createTableAsSelectStmt.getInsertStmt();
        insertStmt.setQueryStmt(queryStmt);

        return relation;
    }

}
