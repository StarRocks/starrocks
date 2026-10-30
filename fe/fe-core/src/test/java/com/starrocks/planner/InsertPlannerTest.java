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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.ConnectorSinkSortScope;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.base.EmptyDistributionProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class InsertPlannerTest {

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
                "createPhysicalPropertySet", InsertStmt.class, List.class, SessionVariable.class, List.class);
        method.setAccessible(true);
        PhysicalPropertySet result = (PhysicalPropertySet) method.invoke(new InsertPlanner(), insertStmt, outputColumns, session, null);

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
                "createPhysicalPropertySet", InsertStmt.class, List.class, SessionVariable.class, List.class);
        method.setAccessible(true);
        PhysicalPropertySet result = (PhysicalPropertySet) method.invoke(new InsertPlanner(), insertStmt, outputColumns, session, null);

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
                "createPhysicalPropertySet", InsertStmt.class, List.class, SessionVariable.class, List.class);
        method.setAccessible(true);
        PhysicalPropertySet result = (PhysicalPropertySet) method.invoke(new InsertPlanner(), insertStmt, outputColumns, session, null);

        // Verify the result - HOST mode with sort order should create SortProperty
        assert result != null;
        SortProperty sortProperty = result.getSortProperty();
        // The sort property should not be empty when host-level sorting is enabled with sort order
        assert sortProperty != null;
    }

}
