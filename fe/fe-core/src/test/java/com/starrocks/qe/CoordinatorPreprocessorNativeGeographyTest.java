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

package com.starrocks.qe;

import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.TypeSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CoordinatorPreprocessorNativeGeographyTest {
    @Test
    public void testPrepareExecRechecksGeographySpill() {
        ConnectContext context = new ConnectContext();
        var type = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY,
                new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                        GeoTypeDescriptor.CoordinateSystem.SPHERICAL, GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                        "OGC:CRS84", 4326));
        var slot = new TSlotDescriptor().setIsMaterialized(true).setSlotType(TypeSerializer.toThrift(type));
        CoordinatorPreprocessor preprocessor = newPreprocessor(context,
                new TDescriptorTable().setSlotDescriptors(List.of(slot)));
        Assertions.assertDoesNotThrow(preprocessor::prepareExec);
        context.getSessionVariable().setEnableSpill(true);
        StarRocksException error = Assertions.assertThrows(StarRocksException.class, preprocessor::prepareExec);
        Assertions.assertTrue(error.getMessage().contains("without spill"));
        context.getSessionVariable().setEnableSpill(false);
        Assertions.assertDoesNotThrow(preprocessor::prepareExec);

        // A query that does not materialize the GEO column must still prepare with spill.
        context.getSessionVariable().setEnableSpill(true);
        slot.setIsMaterialized(false);
        Assertions.assertDoesNotThrow(preprocessor::prepareExec);
    }

    @Test
    public void testPrepareExecWithOrdinaryColumn() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setEnableSpill(true);
        var slot = new TSlotDescriptor().setIsMaterialized(true)
                .setSlotType(TypeSerializer.toThrift(TypeFactory.createType(PrimitiveType.INT)));
        CoordinatorPreprocessor preprocessor = newPreprocessor(context,
                new TDescriptorTable().setSlotDescriptors(List.of(slot)));
        Assertions.assertDoesNotThrow(preprocessor::prepareExec);
    }

    private static CoordinatorPreprocessor newPreprocessor(ConnectContext context, TDescriptorTable descriptors) {
        var root = new EmptySetNode(new PlanNodeId(0), new ArrayList<>(List.of(new TupleId(0))));
        var fragment = new PlanFragment(new PlanFragmentId(0), root, DataPartition.UNPARTITIONED);
        fragment.setSink(new ResultSink(new PlanNodeId(0), TResultSinkType.MYSQL_PROTOCAL));
        JobSpec job = new JobSpec.Builder().queryId(new TUniqueId(1, 1)).descTable(descriptors)
                .fragments(List.of(fragment)).queryOptions(new TQueryOptions().setQuery_type(TQueryType.SELECT)).build();
        return new CoordinatorPreprocessor(context, job, false) {
            @Override
            void computeFragmentInstances() {
                // Isolate worker assignment; prepareExec and its GEO validation run unchanged.
            }
        };
    }
}
