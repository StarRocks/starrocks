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
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeneratedPartitionNegativeExpressionTest {
    @Injectable
    private OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.mockInitWarehouseEnv();
    }

    @Test
    public void testNegativeExpressionOnlyNegatesSumValue() throws Exception {
        Column key = new Column("k", IntegerType.BIGINT, true, null, false, null, "");
        Column value = new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, false, null, "");
        ColumnDef definition = ColumnDef.newInternalGeneratedPartitionColumn(
                "__generated_partition_column_0", new TypeDef(IntegerType.BIGINT), new SlotRef(null, "k"));
        definition.setAggregateType(AggregateType.REPLACE);
        Column routing = ColumnBuilder.buildGeneratedColumn(null, definition);
        List<Column> columns = List.of(key, value, routing);
        Map<ColumnId, Column> idToColumn = new HashMap<>();
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column column : columns) {
            idToColumn.put(column.getColumnId(), column);
            nameToColumn.put(column.getName(), column);
        }
        new Expectations() {
            {
                table.getBaseSchema();
                result = columns;
                minTimes = 0;
                table.getFullSchema();
                result = columns;
                minTimes = 0;
                table.getIdToColumn();
                result = idToColumn;
                minTimes = 0;
                table.getNameToColumn();
                result = nameToColumn;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.AGG_KEYS;
                minTimes = 0;
                table.getColumn(anyString);
                result = (Column) null;
                minTimes = 0;
                table.getColumn("k");
                result = key;
                minTimes = 0;
                table.getColumn("v");
                result = value;
                minTimes = 0;
                table.getColumn(routing.getName());
                result = routing;
                minTimes = 0;
            }
        };
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor destination = descriptors.createTupleDescriptor("negative_destination");
        for (Column column : columns) {
            SlotDescriptor slot = descriptors.addSlotDescriptor(destination);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            slot.setIsNullable(column.isAllowNull());
        }
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        request.setColumns("k,v");
        request.setNegative(true);
        StreamLoadInfo info = StreamLoadInfo.fromTStreamLoadPutRequest(request, null);
        StreamLoadScanNode scan = new StreamLoadScanNode(info.getId(), new PlanNodeId(1), destination, table, info);
        scan.init(descriptors);
        scan.finalizeStats();
        TBrokerScanRangeParams parameters = scan.getScanRangeLocations(0).get(0).scan_range.broker_scan_range.params;
        TExpr keyExpression = parameters.expr_of_dest_slot.get(destination.getSlots().get(0).getId().asInt());
        TExpr valueExpression = parameters.expr_of_dest_slot.get(destination.getSlots().get(1).getId().asInt());
        TExpr routingExpression = parameters.expr_of_dest_slot.get(destination.getSlots().get(2).getId().asInt());
        Assertions.assertFalse(containsNegativeMultiplier(keyExpression));
        Assertions.assertTrue(containsNegativeMultiplier(valueExpression), valueExpression.toString());
        Assertions.assertFalse(containsNegativeMultiplier(routingExpression));
        Assertions.assertEquals(AggregateType.REPLACE, routing.getAggregationType());
    }

    private boolean containsNegativeMultiplier(TExpr expression) {
        boolean multiplies = expression.nodes.stream().anyMatch(node -> node.node_type == TExprNodeType.ARITHMETIC_EXPR
                && node.opcode == TExprOpcode.MULTIPLY);
        // CSV source slots are VARCHAR. Arithmetic analysis promotes their multiplication to DOUBLE
        // and folds the cast of -1 into a FLOAT_LITERAL before serializing the actual load expression.
        boolean negativeOne = expression.nodes.stream().anyMatch(node ->
                (node.node_type == TExprNodeType.INT_LITERAL && node.int_literal.value == -1)
                        || (node.node_type == TExprNodeType.FLOAT_LITERAL && node.float_literal.value == -1.0));
        return multiplies && negativeOne;
    }
}
