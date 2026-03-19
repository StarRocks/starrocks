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

#include "runtime/runtime_filter_serde.h"

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "runtime/runtime_filter_factory.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks {

static uint8_t get_rf_version(const RuntimeState* state) {
    if (state->func_version() >= TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3) {
        return RF_VERSION_V3;
    } else if (state->func_version() >= TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_2) {
        return RF_VERSION_V2;
    }
    return RF_VERSION;
}

size_t RuntimeFilterSerde::max_size(int rf_version, const RuntimeFilter* rf) {
    size_t size = RF_VERSION_SZ;
    if (rf_version >= RF_VERSION_V3) {
        size += sizeof(RuntimeFilterSerializeType);
    }
    size += rf->max_serialized_size();
    return size;
}

size_t RuntimeFilterSerde::max_size(const RuntimeState* state, const RuntimeFilter* rf) {
    const uint8_t rf_version = get_rf_version(state);
    return max_size(rf_version, rf);
}

size_t RuntimeFilterSerde::serialize(int rf_version, const RuntimeFilter* rf, uint8_t* data) {
    size_t offset = 0;

    memcpy(data + offset, &rf_version, RF_VERSION_SZ);
    offset += RF_VERSION_SZ;

    if (rf_version >= RF_VERSION_V3) {
        const RuntimeFilterSerializeType type = rf->type();
        memcpy(data + offset, &type, sizeof(type));
        offset += sizeof(type);
    }

    offset += rf->serialize(rf_version, data + offset);

    return offset;
}

size_t RuntimeFilterSerde::serialize(const RuntimeState* state, const RuntimeFilter* rf, uint8_t* data) {
    const uint8_t rf_version = get_rf_version(state);
    return serialize(rf_version, rf, data);
}

int RuntimeFilterSerde::deserialize(ObjectPool* pool, RuntimeFilter** rf, const uint8_t* data, size_t size) {
    *rf = nullptr;

    size_t offset = 0;

    uint8_t version = 0;
    memcpy(&version, data, sizeof(version));
    offset += sizeof(version);
    if (version < RF_VERSION_V2) {
        LOG(WARNING) << "unrecognized version:" << version;
        return 0;
    }

    RuntimeFilterSerializeType rf_type = RuntimeFilterSerializeType::BLOOM_FILTER;
    if (version >= RF_VERSION_V3) {
        memcpy(&rf_type, data + offset, sizeof(rf_type));
        offset += sizeof(rf_type);
    }
    if (rf_type > RuntimeFilterSerializeType::UNKNOWN_FILTER) {
        LOG(WARNING) << "unrecognized runtime filter type:" << static_cast<int>(rf_type);
        return 0;
    }

    TPrimitiveType::type tltype;
    memcpy(&tltype, data + offset, sizeof(tltype));
    const LogicalType ltype = thrift_to_type(tltype);

    RuntimeFilter* filter = RuntimeFilterFactory::create_filter(pool, rf_type, ltype, TJoinDistributionMode::NONE);
    DCHECK(filter != nullptr);
    if (filter != nullptr) {
        offset += filter->deserialize(version, data + offset);
        DCHECK(offset == size);
        *rf = filter;
    }

    return version;
}

size_t RuntimeFilterSerde::skew_max_size(const ColumnPtr& column) {
    size_t size = RF_VERSION_SZ;
    size += (sizeof(bool) + sizeof(size_t) + sizeof(bool) + sizeof(bool));
    size += serde::ColumnArraySerde::max_serialized_size(*column);
    return size;
}

StatusOr<size_t> RuntimeFilterSerde::skew_serialize(const ColumnPtr& column, bool eq_null, uint8_t* data) {
    size_t offset = 0;
#define JRF_COPY_FIELD_TO(field)                  \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);
    JRF_COPY_FIELD_TO(RF_VERSION_V2);
    JRF_COPY_FIELD_TO(eq_null);
    size_t num_rows = column->size();
    JRF_COPY_FIELD_TO(num_rows);
    bool is_nullable = column->is_nullable();
    JRF_COPY_FIELD_TO(is_nullable);
    bool is_const = column->is_constant();
    JRF_COPY_FIELD_TO(is_const);

    uint8_t* cur = data + offset;
    ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::serialize(*column, cur));
    offset += (cur - (data + offset));

#undef JRF_COPY_FIELD_TO
    return offset;
}

StatusOr<int> RuntimeFilterSerde::skew_deserialize(ObjectPool* pool, RuntimeFilterSkewMaterial** material,
                                                   const uint8_t* data, size_t size, const PTypeDesc& ptype) {
    *material = nullptr;
    auto* rf_material = pool->add(new RuntimeFilterSkewMaterial());
    size_t offset = 0;

#define JRF_COPY_FIELD_FROM(field)                \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    uint8_t version = 0;
    JRF_COPY_FIELD_FROM(version);
    if (version != RF_VERSION_V2) {
        LOG(WARNING) << "unrecognized version:" << version;
        return 0;
    }

    bool eq_null;
    JRF_COPY_FIELD_FROM(eq_null);

    size_t num_rows = 0;
    JRF_COPY_FIELD_FROM(num_rows);

    bool is_null;
    JRF_COPY_FIELD_FROM(is_null);

    bool is_const;
    JRF_COPY_FIELD_FROM(is_const);

    const TypeDescriptor type_descriptor = TypeDescriptor::from_protobuf(ptype);
    auto column = ColumnHelper::create_column(type_descriptor, is_null, is_const, num_rows);

    const uint8_t* cur = data + offset;
    const uint8_t* end = data + size;
    ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::deserialize(cur, end, column.get()));
    offset += (cur - (data + offset));
    DCHECK(offset == size);

    rf_material->build_type = type_descriptor.type;
    rf_material->eq_null = eq_null;
    rf_material->key_column = std::move(column);
    *material = rf_material;
#undef JRF_COPY_FIELD_FROM
    return version;
}

} // namespace starrocks
