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

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.system.BackendHbResponse;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.type.TypeSerializer;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NativeGeographySupportTest {
    @Test
    public void testGatesAndReusedPlan() throws Exception {
        boolean read = Config.enable_native_geography_iceberg_read;
        boolean transport = Config.enable_native_geography_transport;
        boolean output = Config.enable_native_geography_mysql_output;
        try {
            ComputeNode node = new ComputeNode();
            node.setAlive(true);
            BackendHbResponse response = new BackendHbResponse(0, 0, 0, 0, 0, 1, "", 0, 0);
            response.setNativeGeoCapabilities(7);
            node.handleHbResponse(response, false);
            var instances = List.of(new FragmentInstance(node, null));
            var type = NativeGeographySupport.geographyType(Types.GeographyType.crs84());
            var slot = new TSlotDescriptor().setIsMaterialized(true).setSlotType(TypeSerializer.toThrift(type));
            var descriptors = new TDescriptorTable().setSlotDescriptors(List.of(slot));
            var fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.UNPARTITIONED);
            fragment.setSink(new ResultSink(new PlanNodeId(0), TResultSinkType.MYSQL_PROTOCAL));
            JobSpec job = new JobSpec.Builder().descTable(descriptors).fragments(List.of(fragment))
                    .queryOptions(new TQueryOptions().setQuery_type(TQueryType.SELECT)).build();
            for (int mask = 0; mask < 8; ++mask) {
                Config.enable_native_geography_iceberg_read = (mask & 1) != 0;
                Config.enable_native_geography_transport = (mask & 2) != 0;
                Config.enable_native_geography_mysql_output = (mask & 4) != 0;
                Assertions.assertEquals(mask == 7, NativeGeographySupport.gatesEnabled());
                if (mask != 7) {
                    Assertions.assertThrows(StarRocksException.class,
                            () -> NativeGeographySupport.validateExecution(job, instances, false));
                } else {
                    NativeGeographySupport.validateExecution(job, instances, false);
                }
            }
            Assertions.assertThrows(StarRocksException.class,
                    () -> NativeGeographySupport.validateExecution(job, instances, true));
            Assertions.assertThrows(StarRocksException.class,
                    () -> NativeGeographySupport.validateExecution(job, List.of(), false));
            for (TResultSinkType sink : TResultSinkType.values()) {
                if (sink == TResultSinkType.MYSQL_PROTOCAL) {
                    continue;
                }
                fragment.setSink(new ResultSink(new PlanNodeId(0), sink));
                Assertions.assertThrows(StarRocksException.class,
                        () -> NativeGeographySupport.validateExecution(job, instances, false));
            }
            fragment.setSink(new ResultSink(new PlanNodeId(0), TResultSinkType.MYSQL_PROTOCAL));
            ComputeNode oldPeer = new ComputeNode();
            oldPeer.setAlive(true);
            Assertions.assertThrows(StarRocksException.class,
                    () -> NativeGeographySupport.validateExecution(job,
                            List.of(instances.get(0), new FragmentInstance(oldPeer, null)), false));
            response.setNativeGeoCapabilities(3);
            node.handleHbResponse(response, false);
            Assertions.assertFalse(NativeGeographySupport.supportsRead(node));
            Assertions.assertThrows(StarRocksException.class,
                    () -> NativeGeographySupport.validateExecution(job, instances, false));
            Config.enable_native_geography_iceberg_read = false;
            Schema schema = new Schema(Types.NestedField.optional(1, "geo", Types.GeographyType.crs84()),
                    Types.NestedField.optional(2, "id", Types.IntegerType.get()));
            var columns = IcebergApiConverter.toFullSchemas(schema);
            Assertions.assertTrue(columns.get(0).getType().isUnknown());
            Assertions.assertTrue(columns.get(1).getType().isIntegerType());
            // Unmaterialized GEO metadata must not block a query of ordinary columns.
            slot.setIsMaterialized(false);
            NativeGeographySupport.validateExecution(job, instances, false);
        } finally {
            Config.enable_native_geography_iceberg_read = read;
            Config.enable_native_geography_transport = transport;
            Config.enable_native_geography_mysql_output = output;
        }
    }

    @Test
    public void testReadRequiresEveryCapabilityAndAvailableNode() {
        ComputeNode node = new ComputeNode();
        BackendHbResponse response = new BackendHbResponse(0, 0, 0, 0, 0, 1, "", 0, 0);
        for (int mask = 0; mask < 8; ++mask) {
            response.setNativeGeoCapabilities(mask);
            node.handleHbResponse(response, false);
            Assertions.assertEquals(mask == 7, NativeGeographySupport.supportsRead(node));
        }
        node.setAlive(false);
        Assertions.assertFalse(NativeGeographySupport.supportsRead(node));
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
