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

import com.starrocks.proto.GeoColumnDescPB;
import com.starrocks.proto.GeoCoordinateSystemPB;
import com.starrocks.proto.GeoDimensionPB;
import com.starrocks.proto.GeoEdgeAlgorithmPB;
import com.starrocks.proto.GeoEncodingPB;
import com.starrocks.proto.GeoLogicalTypePB;
import com.starrocks.proto.GeoStorageDescPB;
import com.starrocks.proto.GeoTypeDescPB;
import com.starrocks.proto.GeoValidationStatePB;
import com.starrocks.thrift.TGeoColumnDesc;
import com.starrocks.thrift.TGeoCoordinateSystem;
import com.starrocks.thrift.TGeoDimension;
import com.starrocks.thrift.TGeoEdgeAlgorithm;
import com.starrocks.thrift.TGeoEncoding;
import com.starrocks.thrift.TGeoLogicalType;
import com.starrocks.thrift.TGeoStorageDesc;
import com.starrocks.thrift.TGeoTypeDesc;
import com.starrocks.thrift.TGeoValidationState;

/** Wire conversion only; native primitive consistency belongs to ScalarType construction. */
final class GeoTypeSerde {
    private GeoTypeSerde() {
    }

    static TGeoColumnDesc toThrift(GeoColumnDescriptor descriptor) {
        TGeoTypeDesc semantic = new TGeoTypeDesc();
        TGeoStorageDesc storage = new TGeoStorageDesc();
        semantic.setLogical_type(TGeoLogicalType.valueOf(descriptor.logicalType().name()));
        semantic.setCoordinate_system(TGeoCoordinateSystem.valueOf(descriptor.coordinateSystem().name()));
        semantic.setEdge_algorithm(TGeoEdgeAlgorithm.valueOf(descriptor.edgeAlgorithm().name()));
        storage.setEncoding(TGeoEncoding.valueOf(descriptor.encoding().name()));
        storage.setDimension(TGeoDimension.valueOf(descriptor.dimension().name()));
        storage.setValidation_state(TGeoValidationState.valueOf(descriptor.validationState().name()));
        semantic.setCrs(descriptor.crs());
        if (descriptor.srid() != null) {
            semantic.setSrid(descriptor.srid());
        }
        return new TGeoColumnDesc().setType(semantic).setStorage(storage);
    }

    static GeoColumnDescPB toProtobuf(GeoColumnDescriptor descriptor) {
        GeoTypeDescPB semantic = new GeoTypeDescPB();
        GeoStorageDescPB storage = new GeoStorageDescPB();
        semantic.logicalType = GeoLogicalTypePB.valueOf("GEO_LOGICAL_TYPE_" + descriptor.logicalType().name());
        semantic.coordinateSystem =
                GeoCoordinateSystemPB.valueOf("GEO_COORDINATE_SYSTEM_" + descriptor.coordinateSystem().name());
        semantic.edgeAlgorithm = GeoEdgeAlgorithmPB.valueOf("GEO_EDGE_ALGORITHM_" + descriptor.edgeAlgorithm().name());
        storage.encoding = GeoEncodingPB.valueOf("GEO_ENCODING_" + descriptor.encoding().name());
        storage.dimension = GeoDimensionPB.valueOf("GEO_DIMENSION_" + descriptor.dimension().name());
        storage.validationState = GeoValidationStatePB.valueOf("GEO_VALIDATION_STATE_" + descriptor.validationState().name());
        semantic.crs = descriptor.crs();
        semantic.srid = descriptor.srid();
        GeoColumnDescPB result = new GeoColumnDescPB();
        result.type = semantic;
        result.storage = storage;
        return result;
    }

    static GeoColumnDescriptor fromThrift(TGeoColumnDesc descriptor) {
        TGeoTypeDesc semantic = descriptor.isSetType() ? descriptor.type : new TGeoTypeDesc();
        TGeoStorageDesc storage = descriptor.isSetStorage() ? descriptor.storage : new TGeoStorageDesc();
        return new GeoColumnDescriptor(
                fromWire(semantic.logical_type, GeoColumnDescriptor.LogicalType.class, ""),
                fromWire(semantic.coordinate_system, GeoColumnDescriptor.CoordinateSystem.class, ""),
                fromWire(semantic.edge_algorithm, GeoColumnDescriptor.EdgeAlgorithm.class, ""),
                semantic.crs == null ? "" : semantic.crs, semantic.isSetSrid() ? semantic.srid : null,
                fromWire(storage.encoding, GeoColumnDescriptor.Encoding.class, ""),
                fromWire(storage.dimension, GeoColumnDescriptor.Dimension.class, ""),
                fromWire(storage.validation_state, GeoColumnDescriptor.ValidationState.class, ""));
    }

    static GeoColumnDescriptor fromProtobuf(GeoColumnDescPB descriptor) {
        GeoTypeDescPB semantic = descriptor.type == null ? new GeoTypeDescPB() : descriptor.type;
        GeoStorageDescPB storage = descriptor.storage == null ? new GeoStorageDescPB() : descriptor.storage;
        return new GeoColumnDescriptor(
                fromWire(semantic.logicalType, GeoColumnDescriptor.LogicalType.class, "GEO_LOGICAL_TYPE_"),
                fromWire(semantic.coordinateSystem, GeoColumnDescriptor.CoordinateSystem.class, "GEO_COORDINATE_SYSTEM_"),
                fromWire(semantic.edgeAlgorithm, GeoColumnDescriptor.EdgeAlgorithm.class, "GEO_EDGE_ALGORITHM_"),
                semantic.crs == null ? "" : semantic.crs, semantic.srid,
                fromWire(storage.encoding, GeoColumnDescriptor.Encoding.class, "GEO_ENCODING_"),
                fromWire(storage.dimension, GeoColumnDescriptor.Dimension.class, "GEO_DIMENSION_"),
                fromWire(storage.validationState, GeoColumnDescriptor.ValidationState.class, "GEO_VALIDATION_STATE_"));
    }

    private static <T extends Enum<T>> T fromWire(Enum<?> value, Class<T> type, String prefix) {
        return Enum.valueOf(type, value == null ? "UNKNOWN" : value.name().substring(prefix.length()));
    }
}
