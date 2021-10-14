// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/column_vector.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "column_vector.h"

#include <memory>

#include "storage/field.h"

namespace starrocks {

ColumnVectorBatch::~ColumnVectorBatch() = default;

Status ColumnVectorBatch::resize(size_t new_cap) {
    if (_nullable) {
        _null_signs.resize(new_cap);
    }
    _capacity = new_cap;
    return Status::OK();
}

namespace {
template <FieldType field_type>
using batch_t = ScalarColumnVectorBatch<typename CppTypeTraits<field_type>::CppType>;
} // namespace

Status ColumnVectorBatch::create(size_t capacity, bool nullable, const TypeInfoPtr& type_info, Field* field,
                                 std::unique_ptr<ColumnVectorBatch>* batch) {
    switch (type_info->type()) {
    case OLAP_FIELD_TYPE_BOOL:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_TINYINT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_TINYINT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_TINYINT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_SMALLINT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_SMALLINT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_INT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_INT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_UNSIGNED_INT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_BIGINT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_LARGEINT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_LARGEINT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_FLOAT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_FLOAT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DOUBLE:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DOUBLE>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DECIMAL:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DECIMAL>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DECIMAL_V2>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DECIMAL32:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DECIMAL32>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DECIMAL64:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DECIMAL64>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DECIMAL128:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DECIMAL128>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DATE:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DATE>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DATE_V2:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DATE_V2>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_DATETIME:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_DATETIME>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_TIMESTAMP:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_TIMESTAMP>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_CHAR:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_CHAR>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_VARCHAR:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_VARCHAR>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_HLL:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_HLL>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_OBJECT:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_OBJECT>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_PERCENTILE:
        *batch = std::make_unique<batch_t<OLAP_FIELD_TYPE_PERCENTILE>>(type_info, nullable);
        break;
    case OLAP_FIELD_TYPE_ARRAY: {
        if (field == nullptr) {
            return Status::InvalidArgument("`Field` cannot be NULL when create ArrayColumnVectorBatch");
        }
        *batch = std::make_unique<ArrayColumnVectorBatch>(type_info, nullable, capacity, field);
        break;
    }
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
        return Status::NotSupported("Unsupported type " + std::to_string(type_info->type()));
    }
    (*batch)->resize(capacity);
    return Status::OK();
}

template <class ScalarType>
ScalarColumnVectorBatch<ScalarType>::ScalarColumnVectorBatch(const TypeInfoPtr& type_info, bool is_nullable)
        : ColumnVectorBatch(type_info, is_nullable), _data(0) {}

ArrayColumnVectorBatch::ArrayColumnVectorBatch(const TypeInfoPtr& type_info, bool is_nullable, size_t init_capacity,
                                               Field* field)
        : ColumnVectorBatch(type_info, is_nullable), _item_offsets(1) {
    auto array_type_info = down_cast<const ArrayTypeInfo*>(type_info.get());
    _item_offsets[0] = 0;
    ColumnVectorBatch::create(init_capacity, field->get_sub_field(0)->is_nullable(), array_type_info->item_type_info(),
                              field->get_sub_field(0), &_elements);
}

ArrayColumnVectorBatch::~ArrayColumnVectorBatch() = default;

Status ArrayColumnVectorBatch::resize(size_t new_cap) {
    if (capacity() < new_cap) {
        RETURN_IF_ERROR(ColumnVectorBatch::resize(new_cap));
        _data.resize(new_cap);
        _item_offsets.resize(new_cap + 1);
    }
    return Status::OK();
}

void ArrayColumnVectorBatch::prepare_for_read(size_t start_idx, size_t end_idx) {
    for (size_t idx = start_idx; idx < end_idx; ++idx) {
        if (!is_null_at(idx)) {
            _data[idx].has_null = _elements->is_nullable();
            _data[idx].length = _item_offsets[idx + 1] - _item_offsets[idx];
            if (_elements->is_nullable()) {
                _data[idx].null_signs = &_elements->null_signs()[_item_offsets[idx]];
            } else {
                _data[idx].null_signs = nullptr;
            }
            _data[idx].data = _elements->mutable_cell_ptr(_item_offsets[idx]);
        } else {
            _data[idx].data = nullptr;
            _data[idx].length = 0;
            _data[idx].has_null = false;
            _data[idx].null_signs = nullptr;
        }
    }
}

} // namespace starrocks
