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

package com.starrocks.connector.iceberg;

import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeSerializer;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NativeGeographySupportTest {
    @Test
    public void testNativeGeographyIsExposedWithoutConfiguration() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "geo", Types.GeographyType.crs84()),
                Types.NestedField.required(2, "required_geo", Types.GeographyType.crs84()),
                Types.NestedField.optional(3, "id", Types.IntegerType.get()));
        var columns = IcebergApiConverter.toFullSchemas(schema);
        Assertions.assertEquals(PrimitiveType.GEOGRAPHY, columns.get(0).getType().getPrimitiveType());
        Assertions.assertTrue(columns.get(0).isAllowNull());
        Assertions.assertEquals(PrimitiveType.GEOGRAPHY, columns.get(1).getType().getPrimitiveType());
        Assertions.assertFalse(columns.get(1).isAllowNull());
        Assertions.assertTrue(columns.get(2).getType().isIntegerType());
    }

    @Test
    public void testExecutionRestrictions() throws Exception {
        var type = NativeGeographySupport.geographyType(Types.GeographyType.crs84());
        var slot = new TSlotDescriptor().setIsMaterialized(true).setSlotType(TypeSerializer.toThrift(type));
        var descriptors = new TDescriptorTable().setSlotDescriptors(List.of(slot));
        var fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.UNPARTITIONED);
        fragment.setSink(new ResultSink(new PlanNodeId(0), TResultSinkType.MYSQL_PROTOCAL));
        JobSpec job = new JobSpec.Builder().descTable(descriptors).fragments(List.of(fragment))
                .queryOptions(new TQueryOptions().setQuery_type(TQueryType.SELECT)).build();
        NativeGeographySupport.validateExecution(job, false);
        Assertions.assertThrows(StarRocksException.class,
                () -> NativeGeographySupport.validateExecution(job, true));
        for (TResultSinkType sink : TResultSinkType.values()) {
            if (sink == TResultSinkType.MYSQL_PROTOCAL) {
                continue;
            }
            fragment.setSink(new ResultSink(new PlanNodeId(0), sink));
            Assertions.assertThrows(StarRocksException.class,
                    () -> NativeGeographySupport.validateExecution(job, false));
        }
        // Unmaterialized GEO metadata must not block a query of ordinary columns.
        slot.setIsMaterialized(false);
        NativeGeographySupport.validateExecution(job, true);
    }

    @Test
    public void testOnlyCanonicalGeographyIsExposed() {
        for (EdgeAlgorithm edge : EdgeAlgorithm.values()) {
            for (String crs : new String[] {"OGC:CRS84", "EPSG:4326", "srid:4326", "EPSG:3857"}) {
                var type = NativeGeographySupport.geographyType(Types.GeographyType.of(crs, edge));
                Assertions.assertEquals(!crs.equals("OGC:CRS84") || edge != EdgeAlgorithm.SPHERICAL, type.isUnknown());
            }
        }
    }
}
