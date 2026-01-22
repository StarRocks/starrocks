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

package com.starrocks.planner;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
<<<<<<< HEAD
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
=======
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.connector.ConnectorSinkSortScope;
>>>>>>> 4f2ca125c5 ([Enhancement] Introduce a host-level sorting for iceberg table sink to improve data organization and later reading performance (#68121))
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
<<<<<<< HEAD
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
=======
import com.starrocks.sql.optimizer.base.EmptyDistributionProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
>>>>>>> 4f2ca125c5 ([Enhancement] Introduce a host-level sorting for iceberg table sink to improve data organization and later reading performance (#68121))
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.*;

<<<<<<< HEAD
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
=======
import java.lang.reflect.Method;
import java.util.ArrayList;
>>>>>>> 4f2ca125c5 ([Enhancement] Introduce a host-level sorting for iceberg table sink to improve data organization and later reading performance (#68121))
import java.util.List;
import java.util.Map;

public class InsertPlannerTest {

    @Test
    public void testRefreshAllCollectedExternalTables(@Mocked ConnectContext session,
                                        @Mocked GlobalStateMgr gsm,
                                        @Mocked MetadataMgr metadataMgr,
                                        @Mocked QueryStatement qs,
                                        @Mocked SessionVariable sessVar,
                                        @Mocked AnalyzerUtils unusedStatic,
                                        @Mocked Table t1,
                                        @Mocked Table t2) {
        new Expectations() {{
            session.getGlobalStateMgr(); result = gsm; minTimes = 0;
            gsm.getMetadataMgr(); result = metadataMgr; minTimes = 0;

            session.getSessionVariable(); result = sessVar;
            sessVar.isEnableInsertSelectExternalAutoRefresh(); result = true;

            t1.getCatalogName(); result = "c1"; minTimes = 0;
            t1.getCatalogDBName(); result = "db1"; minTimes = 0;
            t1.isExternalTableWithFileSystem(); result = true; minTimes = 0;

            t2.getCatalogName(); result = "c2"; minTimes = 0;
            t2.getCatalogDBName(); result = "db2"; minTimes = 0;
            t2.isExternalTableWithFileSystem(); result = true; minTimes = 0;

            AnalyzerUtils.collectAllTableWithAlias(qs);
            result = new Delegate<Void>() {
                @SuppressWarnings("unused")
                Map<TableName, Table> delegate(QueryStatement qs) {
                        Map<TableName, Table> out = Maps.newHashMap();
                        out.put(new TableName("c1", "db1", "t1"), t1);
                        out.put(new TableName("c2", "db2", "t2"), t2);
                        return out;
                }
            };
        }};

        InsertPlanner target = new InsertPlanner();
        target.refreshExternalTable(qs, session);

        new Verifications() {{
            metadataMgr.refreshTable("c1", "db1", t1, (List<String>) any, false); times = 1;
            metadataMgr.refreshTable("c2", "db2", t2, (List<String>) any, false); times = 1;
        }};
    }

    @Test
    public void testAutoRefreshDisabled(@Mocked ConnectContext session,
                                        @Mocked SessionVariable sessVar,
                                        @Mocked GlobalStateMgr gsm,
                                        @Mocked MetadataMgr metadataMgr,
                                        @Mocked QueryStatement qs,
                                        @Mocked AnalyzerUtils unusedStatic) {
        new Expectations() {{
            session.getSessionVariable(); result = sessVar;
            sessVar.isEnableInsertSelectExternalAutoRefresh(); result = false;
        }};

        InsertPlanner target = new InsertPlanner();
        target.refreshExternalTable(qs, session);

        new Verifications() {{
            AnalyzerUtils.collectAllTableWithAlias(qs); times = 0;
            metadataMgr.refreshTable(anyString, anyString, (Table) any, (List<String>) any, anyBoolean); times = 0;
            session.getGlobalStateMgr(); times = 0; 
        }};
    }

    @Test
    public void testDoNothingWhenNoTableCollected(@Mocked ConnectContext session,
                                        @Mocked GlobalStateMgr gsm,
                                        @Mocked SessionVariable sessVar,
                                        @Mocked MetadataMgr metadataMgr,
                                        @Mocked QueryStatement qs,
                                        @Mocked AnalyzerUtils unusedStatic) {
        new Expectations() {{
                session.getSessionVariable(); result = sessVar;
                sessVar.isEnableInsertSelectExternalAutoRefresh(); result = true;
                AnalyzerUtils.collectAllTableWithAlias(qs);
                result = new Delegate<Void>() {
                    @SuppressWarnings("unused")
                    Map<TableName, Table> delegate(QueryStatement _qs) {
                        return Maps.newHashMap();
                    }
                };
        }};

        InsertPlanner target = new InsertPlanner();
        target.refreshExternalTable(qs, session);

        new Verifications() {{
            metadataMgr.refreshTable(anyString, anyString, (Table) any, (List<String>) any, anyBoolean); times = 0;
        }};
    }

    @Test
    public void testCreatePhysicalPropertySetWithFileMode(@Mocked InsertStmt insertStmt,
                                                        @Mocked SessionVariable session,
                                                        @Mocked IcebergTable icebergTable,
                                                        @Mocked QueryStatement queryStatement) throws Exception {
        // Create output columns
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        outputColumns.add(new ColumnRefOperator(1, new ScalarType(PrimitiveType.INT), "col1", true));
        outputColumns.add(new ColumnRefOperator(2, new ScalarType(PrimitiveType.BIGINT), "col2", true));

        new Expectations() {{
            insertStmt.getTargetTable(); result = icebergTable;
            insertStmt.getQueryStatement(); result = queryStatement;
            queryStatement.getQueryRelation(); result = null;
            session.getConnectorSinkSortScope(); result = ConnectorSinkSortScope.FILE.scopeName();
            icebergTable.getPartitionColumns().isEmpty(); result = true; minTimes = 0;
        }};

        // Use reflection to call the private method
        Method method = InsertPlanner.class.getDeclaredMethod(
                "createPhysicalPropertySet", InsertStmt.class, List.class, SessionVariable.class);
        method.setAccessible(true);
        PhysicalPropertySet result = (PhysicalPropertySet) method.invoke(new InsertPlanner(), insertStmt, outputColumns, session);

        // FILE mode should not create SortProperty
        // PhysicalPropertySet should be empty (no distribution, no sort)
        assert result != null;
        assert result.getSortProperty().isEmpty();
    }

    @Test
    public void testCreatePhysicalPropertySetWithHostModeNoSortOrder(@Mocked InsertStmt insertStmt,
                                                                     @Mocked SessionVariable session,
                                                                     @Mocked IcebergTable icebergTable,
                                                                     @Mocked QueryStatement queryStatement,
                                                                     @Mocked org.apache.iceberg.Table nativeTable,
                                                                     @Mocked org.apache.iceberg.SortOrder sortOrder) throws Exception {
        // Create output columns
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        outputColumns.add(new ColumnRefOperator(1, new ScalarType(PrimitiveType.INT), "col1", true));
        outputColumns.add(new ColumnRefOperator(2, new ScalarType(PrimitiveType.BIGINT), "col2", true));

        new Expectations() {{
            insertStmt.getTargetTable(); result = icebergTable;
            insertStmt.getQueryStatement(); result = queryStatement;
            queryStatement.getQueryRelation(); result = null;
            session.getConnectorSinkSortScope(); result = ConnectorSinkSortScope.HOST.scopeName();
            icebergTable.getNativeTable(); result = nativeTable;
            nativeTable.sortOrder(); result = sortOrder;
            sortOrder.isSorted(); result = false;
            icebergTable.getPartitionColumns().isEmpty(); result = true; minTimes = 0;
        }};

        // Use reflection to call the private method
        Method method = InsertPlanner.class.getDeclaredMethod(
                "createPhysicalPropertySet", InsertStmt.class, List.class, SessionVariable.class);
        method.setAccessible(true);
        PhysicalPropertySet result = (PhysicalPropertySet) method.invoke(new InsertPlanner(), insertStmt, outputColumns, session);

        // HOST mode with no sort order should not create SortProperty
        assert result != null;
        assert result.getSortProperty().isEmpty();
        assert result.getDistributionProperty() == EmptyDistributionProperty.INSTANCE;
    }

    @Test
    public void testCreatePhysicalPropertySetWithHostModeWithSortOrder(@Mocked InsertStmt insertStmt,
                                                                       @Mocked SessionVariable session,
                                                                       @Mocked IcebergTable icebergTable,
                                                                       @Mocked QueryStatement queryStatement,
                                                                       @Mocked org.apache.iceberg.Table nativeTable,
                                                                       @Mocked org.apache.iceberg.SortOrder sortOrder,
                                                                       @Mocked org.apache.iceberg.SortField sortField1,
                                                                       @Mocked org.apache.iceberg.SortField sortField2,
                                                                       @Mocked org.apache.iceberg.transforms.Transform transform1,
                                                                       @Mocked org.apache.iceberg.transforms.Transform transform2) throws Exception {
        // sortKeyIndexes contains array positions in schema (0, 1, etc.)
        final List<Integer> sortKeyIndexes = new ArrayList<>();
        sortKeyIndexes.add(0);
        sortKeyIndexes.add(1);

        final List<org.apache.iceberg.SortField> sortFields = new ArrayList<>();
        sortFields.add(sortField1);
        sortFields.add(sortField2);

        // Create output columns (matching schema order)
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        outputColumns.add(new ColumnRefOperator(0, new ScalarType(PrimitiveType.INT), "col1", true));
        outputColumns.add(new ColumnRefOperator(1, new ScalarType(PrimitiveType.BIGINT), "col2", true));

        new Expectations() {{
            insertStmt.getTargetTable(); result = icebergTable;
            insertStmt.getQueryStatement(); result = queryStatement;
            queryStatement.getQueryRelation(); result = null;
            session.getConnectorSinkSortScope(); result = ConnectorSinkSortScope.HOST.scopeName();
            icebergTable.getNativeTable(); result = nativeTable;
            nativeTable.sortOrder(); result = sortOrder;
            sortOrder.isSorted(); result = true;
            icebergTable.getSortKeyIndexes(); result = sortKeyIndexes;
            sortOrder.fields(); result = sortFields;
            sortField1.transform(); result = transform1;
            transform1.isIdentity(); result = true;
            sortField1.direction(); result = org.apache.iceberg.SortDirection.ASC;
            sortField1.nullOrder(); result = org.apache.iceberg.NullOrder.NULLS_FIRST;
            sortField2.transform(); result = transform2;
            transform2.isIdentity(); result = true;
            sortField2.direction(); result = org.apache.iceberg.SortDirection.DESC;
            sortField2.nullOrder(); result = org.apache.iceberg.NullOrder.NULLS_LAST;
            icebergTable.getPartitionColumns().isEmpty(); result = true; minTimes = 0;
        }};

        // Use reflection to call the private method
        Method method = InsertPlanner.class.getDeclaredMethod(
                "createPhysicalPropertySet", InsertStmt.class, List.class, SessionVariable.class);
        method.setAccessible(true);
        PhysicalPropertySet result = (PhysicalPropertySet) method.invoke(new InsertPlanner(), insertStmt, outputColumns, session);

        // Verify the result - HOST mode with sort order should create SortProperty
        assert result != null;
        SortProperty sortProperty = result.getSortProperty();
        // The sort property should not be empty when host-level sorting is enabled with sort order
        assert sortProperty != null;
    }

}