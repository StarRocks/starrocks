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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.TupleId;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TDescriptorTable;
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

public class DefaultCoordinatorNativeGeographyTest {
    @Test
    public void testQuerySchedulerValidatesGeographyRestrictions() {
        ConnectContext context = newContext();
        TSlotDescriptor slot = geographySlot();
        TDescriptorTable descriptors = new TDescriptorTable().setSlotDescriptors(List.of(slot));
        Assertions.assertDoesNotThrow(() -> createQueryScheduler(context, descriptors));

        context.getSessionVariable().setEnableSpill(true);
        Assertions.assertThrows(SemanticException.class, () -> createQueryScheduler(context, descriptors));

        slot.setIsMaterialized(false);
        Assertions.assertDoesNotThrow(() -> createQueryScheduler(context, descriptors));
    }

    @Test
    public void testInsertSchedulerRejectsGeography() {
        ConnectContext context = newContext();
        TDescriptorTable descriptors = new TDescriptorTable().setSlotDescriptors(List.of(geographySlot()));
        Assertions.assertThrows(SemanticException.class,
                () -> new DefaultCoordinator.Factory().createInsertScheduler(
                        context, fragments(), List.of(), descriptors, null));
    }

    @Test
    public void testQuerySchedulerAllowsOrdinaryColumns() {
        ConnectContext context = newContext();
        context.getSessionVariable().setEnableSpill(true);
        TSlotDescriptor slot = new TSlotDescriptor().setIsMaterialized(true)
                .setSlotType(TypeSerializer.toThrift(TypeFactory.createType(PrimitiveType.INT)));
        Assertions.assertDoesNotThrow(
                () -> createQueryScheduler(context, new TDescriptorTable().setSlotDescriptors(List.of(slot))));
    }

    private static DefaultCoordinator createQueryScheduler(ConnectContext context, TDescriptorTable descriptors) {
        return new DefaultCoordinator.Factory().createQueryScheduler(
                context, fragments(), List.of(), descriptors, null);
    }

    private static ConnectContext newContext() {
        ConnectContext context = new ConnectContext();
        context.setExecutionId(new TUniqueId(1, 1));
        context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        return context;
    }

    private static TSlotDescriptor geographySlot() {
        ScalarType type = ScalarType.createGeoType(PrimitiveType.GEOGRAPHY,
                new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                        GeoTypeDescriptor.CoordinateSystem.SPHERICAL, GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                        "OGC:CRS84", 4326));
        return new TSlotDescriptor().setIsMaterialized(true).setSlotType(TypeSerializer.toThrift(type));
    }

    private static List<PlanFragment> fragments() {
        EmptySetNode root = new EmptySetNode(new PlanNodeId(0), new ArrayList<>(List.of(new TupleId(0))));
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), root, DataPartition.UNPARTITIONED);
        fragment.setSink(new ResultSink(new PlanNodeId(0), TResultSinkType.MYSQL_PROTOCAL));
        return List.of(fragment);
    }
}
