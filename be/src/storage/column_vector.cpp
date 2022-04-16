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

#include "storage/column_vector.h"

#include <memory>

#include "common/statusor.h"
#include "storage/field.h"
#include "storage/olap_common.h"
#include "storage/olap_type_infra.h"

namespace starrocks {

ColumnVectorBatch::~ColumnVectorBatch() = default;

Status ColumnVectorBatch::resize(size_t new_cap) {
    if (_nullable) {
        _null_signs.resize(new_cap);
    }
    _capacity = new_cap;
    return Status::OK();
}

template <FieldType ftype>
struct BatchT {
    using value = ScalarColumnVectorBatch<typename CppTypeTraits<ftype>::CppType>;
};
template <>
struct BatchT<OLAP_FIELD_TYPE_BOOL> {
    using value = ScalarColumnVectorBatch<typename CppTypeTraits<OLAP_FIELD_TYPE_TINYINT>::CppType>;
};

struct BatchBuilder {
    template <FieldType ftype>
    StatusOr<std::unique_ptr<ColumnVectorBatch>> operator()(const TypeInfoPtr& type_info, bool nullable, Field* field,
                                                            size_t capacity) {
        switch (ftype) {
        case OLAP_FIELD_TYPE_ARRAY: {
            if (field == nullptr) {
                return Status::InvalidArgument("`Field` cannot be NULL when create ArrayColumnVectorBatch");
            }
            return std::make_unique<ArrayColumnVectorBatch>(type_info, nullable, capacity, field);
        }
        default: {
            using batch_t = typename BatchT<ftype>::value;
            return std::make_unique<batch_t>(type_info, nullable);
        }
        }
    }
};

Status ColumnVectorBatch::create(size_t capacity, bool nullable, const TypeInfoPtr& type_info, Field* field,
                                 std::unique_ptr<ColumnVectorBatch>* batch) {
    auto res = field_type_dispatch_column(type_info->type(), BatchBuilder(), type_info, nullable, field, capacity);
    if (!res.ok()) {
        return res.status();
    }
    *batch = std::move(res.value());
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
