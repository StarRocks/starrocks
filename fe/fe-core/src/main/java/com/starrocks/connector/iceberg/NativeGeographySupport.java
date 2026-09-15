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
import com.starrocks.planner.ResultSink;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.HeartbeatServiceConstants;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.UnknownType;
import org.apache.iceberg.types.Types;

import java.util.Collection;
import java.util.stream.Stream;

/** Native Iceberg exposure and dispatch use the same policy, including for reused plans. */
public final class NativeGeographySupport {
    private static final long REQUIRED_CAPABILITIES = HeartbeatServiceConstants.NATIVE_GEOGRAPHY_TRANSPORT
            | HeartbeatServiceConstants.NATIVE_GEOGRAPHY_MYSQL_OUTPUT
            | HeartbeatServiceConstants.NATIVE_GEOGRAPHY_ICEBERG_READ;

    private NativeGeographySupport() {
    }

    static boolean gatesEnabled() {
        return Config.enable_native_geography_iceberg_read && Config.enable_native_geography_transport
                && Config.enable_native_geography_mysql_output;
    }

    static boolean supportsRead(ComputeNode node) {
        return node.isAvailable() && (node.getNativeGeoCapabilities() & REQUIRED_CAPABILITIES) == REQUIRED_CAPABILITIES;
    }

    static boolean canExposeNativeType() {
        if (!gatesEnabled()) {
            return false;
        }
        var cluster = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        var backends = cluster.getAvailableBackends();
        var computeNodes = cluster.getAvailableComputeNodes();
        return (!backends.isEmpty() || !computeNodes.isEmpty())
                && Stream.concat(backends.stream(), computeNodes.stream()).allMatch(NativeGeographySupport::supportsRead);
    }

    static Type geographyType(Types.GeographyType source) {
        if ((source.crs() != null && !source.crs().equals("OGC:CRS84"))
                || (source.algorithm() != null && !source.algorithm().name().equals("SPHERICAL"))) {
            return UnknownType.UNKNOWN_TYPE;
        }
        return ScalarType.createGeoType(PrimitiveType.GEOGRAPHY,
                new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                        GeoTypeDescriptor.CoordinateSystem.SPHERICAL, GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                        "OGC:CRS84", 4326));
    }

    public static void validateExecution(JobSpec job, Collection<FragmentInstance> instances, boolean spillEnabled)
            throws StarRocksException {
        var descriptors = job.getDescTable();
        if (descriptors == null || !descriptors.isSetSlotDescriptors()
                || descriptors.getSlotDescriptors().stream().filter(slot -> slot.isIsMaterialized())
                .flatMap(slot -> slot.getSlotType().getTypes().stream())
                .noneMatch(type -> type.isSetScalar_type()
                        && type.getScalar_type().getType() == TPrimitiveType.GEOGRAPHY)) {
            return;
        }
        if (!gatesEnabled() || instances.isEmpty()
                || instances.stream().anyMatch(instance -> !supportsRead(instance.getWorker()))) {
            throw new StarRocksException("Native Iceberg GEOGRAPHY requires enabled read, transport and output gates "
                    + "and compatible execution nodes; replan the query after changing capabilities");
        }
        if (spillEnabled || !job.isQueryType() || job.getFragments().stream().noneMatch(fragment ->
                fragment.getSink() instanceof ResultSink sink && sink.getSinkType() == TResultSinkType.MYSQL_PROTOCAL)) {
            throw new StarRocksException("Native Iceberg GEOGRAPHY currently supports MySQL query output without spill");
        }
    }
}
