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
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
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

    public Relation transformCTASStmt(CreateTableAsSelectStmt createTableAsSelectStmt) {

        List<String> columnNames = createTableAsSelectStmt.getColumnNames();
        QueryStmt queryStmt = createTableAsSelectStmt.getQueryStmt();
        CreateTableStmt createTableStmt = createTableAsSelectStmt.getCreateTableStmt();

        QueryRelation queryRelation = new QueryAnalyzer(catalog, session)
                .transformQueryStmt(queryStmt, new Scope(RelationId.anonymous(), new RelationFields()));

        Map<String, Table> columnNameToTable = Maps.newHashMap();
        Map<String, Table> tableRefToTableMap = Maps.newHashMap();

        // For replication_num, we select the maximum value of all tables replication_num
        int defaultReplicationNum = 1;
        List<TableRef> tableRefs = ((SelectStmt) queryStmt).getTableRefs();
        for (TableRef tableRef : tableRefs) {
            String[] aliases = tableRef.getAliases();
            for (String alias : aliases) {
                Table table = catalog.getDb(tableRef.getName().getDb()).getTable(tableRef.getName().getTbl());
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    Short replicationNum = olapTable.getDefaultReplicationNum();
                    if (replicationNum > defaultReplicationNum) {
                        defaultReplicationNum = replicationNum;
                    }
                }
                tableRefToTableMap.put(alias, table);
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
            if ("VARCHAR".equals(type.getPrimitiveType().toString())) {
                type = stringType;
            }
            ColumnDef columnDef = new ColumnDef(finalColumnNames.get(i), new TypeDef(type), false,
                    null, true, ColumnDef.DefaultValue.NOT_SET, "");
            createTableStmt.addColumnDef(columnDef);
            Expr originExpression = allFields.get(i).getOriginExpression();
            if (originExpression instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) originExpression;
                Table table = tableRefToTableMap.get(slotRef.getTblNameWithoutAnalyzed().getTbl());
                columnNameToTable.put(slotRef.getTblNameWithoutAnalyzed().getTbl() + "." + slotRef.getColumnName(), table);
            }
        }


        if (null == createTableStmt.getProperties()) {
            Map<String, String> properties = Maps.newHashMap();
            properties.put("replication_num", String.valueOf(defaultReplicationNum));
            createTableStmt.setProperties(properties);
        }

        // For HashDistributionDesc key
        // If we have statistics cache, we pick the column with the highest cardinality in the statistics,
        // if we don't, we pick the first column
        if (null == createTableStmt.getDistributionDesc()) {
            String defaultColumnName = finalColumnNames.get(0);
            double candidateDistinctCountCount = 1.0;
            StatisticStorage currentStatisticStorage = Catalog.getCurrentStatisticStorage();

            for (Map.Entry<String, Table> columnEntry : columnNameToTable.entrySet()) {
                String columnName = columnEntry.getKey().substring(columnEntry.getKey().indexOf(".") + 1);
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
