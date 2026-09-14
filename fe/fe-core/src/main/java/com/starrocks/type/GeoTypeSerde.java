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

package com.starrocks.type;

import com.starrocks.proto.GeoCoordinateSystemPB;
import com.starrocks.proto.GeoEdgeAlgorithmPB;
import com.starrocks.proto.GeoLogicalTypePB;
import com.starrocks.proto.GeoTypeDescPB;
import com.starrocks.thrift.TGeoCoordinateSystem;
import com.starrocks.thrift.TGeoEdgeAlgorithm;
import com.starrocks.thrift.TGeoLogicalType;
import com.starrocks.thrift.TGeoTypeDesc;

/** Wire conversion only; native primitive consistency belongs to ScalarType construction. */
final class GeoTypeSerde {
    private GeoTypeSerde() {
    }

    static TGeoTypeDesc toThrift(GeoTypeDescriptor descriptor) {
        TGeoTypeDesc semantic = new TGeoTypeDesc();
        semantic.setLogical_type(TGeoLogicalType.valueOf(descriptor.logicalType().name()));
        semantic.setCoordinate_system(TGeoCoordinateSystem.valueOf(descriptor.coordinateSystem().name()));
        semantic.setEdge_algorithm(TGeoEdgeAlgorithm.valueOf(descriptor.edgeAlgorithm().name()));
        semantic.setCrs(descriptor.crs());
        if (descriptor.srid() != null) {
            semantic.setSrid(descriptor.srid());
        }
        return semantic;
    }

    static GeoTypeDescPB toProtobuf(GeoTypeDescriptor descriptor) {
        GeoTypeDescPB semantic = new GeoTypeDescPB();
        semantic.logicalType = GeoLogicalTypePB.valueOf("GEO_LOGICAL_TYPE_" + descriptor.logicalType().name());
        semantic.coordinateSystem =
                GeoCoordinateSystemPB.valueOf("GEO_COORDINATE_SYSTEM_" + descriptor.coordinateSystem().name());
        semantic.edgeAlgorithm = GeoEdgeAlgorithmPB.valueOf("GEO_EDGE_ALGORITHM_" + descriptor.edgeAlgorithm().name());
        semantic.crs = descriptor.crs();
        semantic.srid = descriptor.srid();
        return semantic;
    }

    static GeoTypeDescriptor fromThrift(TGeoTypeDesc semantic) {
        return new GeoTypeDescriptor(
                fromWire(semantic.logical_type, GeoTypeDescriptor.LogicalType.class, ""),
                fromWire(semantic.coordinate_system, GeoTypeDescriptor.CoordinateSystem.class, ""),
                fromWire(semantic.edge_algorithm, GeoTypeDescriptor.EdgeAlgorithm.class, ""),
                semantic.crs == null ? "" : semantic.crs, semantic.isSetSrid() ? semantic.srid : null);
    }

    static GeoTypeDescriptor fromProtobuf(GeoTypeDescPB semantic) {
        return new GeoTypeDescriptor(
                fromWire(semantic.logicalType, GeoTypeDescriptor.LogicalType.class, "GEO_LOGICAL_TYPE_"),
                fromWire(semantic.coordinateSystem, GeoTypeDescriptor.CoordinateSystem.class, "GEO_COORDINATE_SYSTEM_"),
                fromWire(semantic.edgeAlgorithm, GeoTypeDescriptor.EdgeAlgorithm.class, "GEO_EDGE_ALGORITHM_"),
                semantic.crs == null ? "" : semantic.crs, semantic.srid);
    }

    private static <T extends Enum<T>> T fromWire(Enum<?> value, Class<T> type, String prefix) {
        return Enum.valueOf(type, value == null ? "UNKNOWN" : value.name().substring(prefix.length()));
    }
}
