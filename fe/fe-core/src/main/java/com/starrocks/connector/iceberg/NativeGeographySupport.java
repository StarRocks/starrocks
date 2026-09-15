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
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.UnknownType;
import org.apache.iceberg.types.Types;

/** Native Iceberg exposure and dispatch use the same policy, including for reused plans. */
public final class NativeGeographySupport {
    private NativeGeographySupport() {
    }

    static boolean gatesEnabled() {
        return Config.enable_native_geography_iceberg_read && Config.enable_native_geography_transport
                && Config.enable_native_geography_mysql_output;
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

    public static void validateExecution(JobSpec job, boolean spillEnabled)
            throws StarRocksException {
        var descriptors = job.getDescTable();
        if (descriptors == null || !descriptors.isSetSlotDescriptors()
                || descriptors.getSlotDescriptors().stream().filter(slot -> slot.isIsMaterialized())
                .flatMap(slot -> slot.getSlotType().getTypes().stream())
                .noneMatch(type -> type.isSetScalar_type()
                        && type.getScalar_type().getType() == TPrimitiveType.GEOGRAPHY)) {
            return;
        }
        if (!gatesEnabled()) {
            throw new StarRocksException("Native Iceberg GEOGRAPHY requires enabled read, transport and output gates");
        }
        if (spillEnabled || !job.isQueryType() || job.getFragments().stream().noneMatch(fragment ->
                fragment.getSink() instanceof ResultSink sink && sink.getSinkType() == TResultSinkType.MYSQL_PROTOCAL)) {
            throw new StarRocksException("Native Iceberg GEOGRAPHY currently supports MySQL query output without spill");
        }
    }
}
