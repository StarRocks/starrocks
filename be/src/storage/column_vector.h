// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/column_vector.h

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

#pragma once

#include <utility>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_v2/common.h" // for ordinal_t
#include "storage/types.h"
#include "util/raw_container.h"

namespace starrocks {

// struct that contains column data(null bitmap), data array in sub class.
class ColumnVectorBatch {
public:
    explicit ColumnVectorBatch(TypeInfoPtr type_info, bool is_nullable)
            : _type_info(std::move(type_info)),
              _capacity(0),
              _delete_state(DEL_NOT_SATISFIED),

              _nullable(is_nullable) {}

    virtual ~ColumnVectorBatch();

    const TypeInfoPtr& type_info() const { return _type_info; }

    size_t capacity() const { return _capacity; }

    bool is_nullable() const { return _nullable; }

    bool is_null_at(size_t row_idx) { return _nullable && _null_signs[row_idx]; }

    void set_is_null(size_t idx, bool is_null) {
        if (_nullable) {
            _null_signs[idx] = is_null;
        }
    }

    void set_null_bits(size_t offset, size_t num_rows, bool val) {
        if (_nullable) {
            memset(&_null_signs[offset], val, num_rows);
        }
    }

    uint8_t* null_signs() { return _null_signs.data(); }

    const uint8_t* null_signs() const { return _null_signs.data(); }

    void set_delete_state(DelCondSatisfied delete_state) { _delete_state = delete_state; }

    DelCondSatisfied delete_state() const { return _delete_state; }

    /**
     * Change the number of slots to at least the given capacity.
     * This function is not recursive into subtypes.
     * Tips: This function will change `_capacity` attribute.
     */
    virtual Status resize(size_t new_cap);

    // Get the start of the data.
    virtual uint8_t* data() const = 0;

    // Get the idx's cell_ptr
    virtual const uint8_t* cell_ptr(size_t idx) const = 0;

    // Get thr idx's cell_ptr for write
    virtual uint8_t* mutable_cell_ptr(size_t idx) = 0;

    virtual void swap(ColumnVectorBatch* rhs) {
        _type_info.swap(rhs->_type_info);
        std::swap(_capacity, rhs->_capacity);
        std::swap(_delete_state, rhs->_delete_state);
        std::swap(_nullable, rhs->_nullable);
        std::swap(_null_signs, rhs->_null_signs);
    }

    static Status create(size_t capacity, bool nullable, const TypeInfoPtr& type_info, Field* field,
                         std::unique_ptr<ColumnVectorBatch>* column_vector_batch);

    virtual void prepare_for_read(size_t start_idx, size_t end_idx) {}

private:
    TypeInfoPtr _type_info;
    size_t _capacity;
    DelCondSatisfied _delete_state;
    std::vector<uint8_t> _null_signs;
    bool _nullable;
};

template <class ScalarCppType>
class ScalarColumnVectorBatch : public ColumnVectorBatch {
public:
    explicit ScalarColumnVectorBatch(const TypeInfoPtr& type_info, bool is_nullable);

    ~ScalarColumnVectorBatch() override = default;

    Status resize(size_t new_cap) override {
        if (capacity() < new_cap) { // before first init, _capacity is 0.
            RETURN_IF_ERROR(ColumnVectorBatch::resize(new_cap));
            _data.resize(new_cap);
        }
        return Status::OK();
    }

    // Get the start of the data.
    uint8_t* data() const override { return const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(_data.data())); }

    // Get the idx's cell_ptr
    const uint8_t* cell_ptr(size_t idx) const override { return reinterpret_cast<const uint8_t*>(&_data[idx]); }

    // Get thr idx's cell_ptr for write
    uint8_t* mutable_cell_ptr(size_t idx) override { return reinterpret_cast<uint8_t*>(&_data[idx]); }

    void swap(ColumnVectorBatch* rhs) override {
        ColumnVectorBatch::swap(rhs);
        std::swap(_data, down_cast<ScalarColumnVectorBatch*>(rhs)->_data);
    }

private:
    std::vector<ScalarCppType> _data;
};

class ArrayColumnVectorBatch : public ColumnVectorBatch {
public:
    explicit ArrayColumnVectorBatch(const TypeInfoPtr& type_info, bool is_nullable, size_t init_capacity, Field* field);
    ~ArrayColumnVectorBatch() override;
    Status resize(size_t new_cap) override;

    ColumnVectorBatch* elements() const { return _elements.get(); }

    // Get the start of the data.
    uint8_t* data() const override { return reinterpret_cast<uint8*>(const_cast<Collection*>(_data.data())); }

    // Get the idx's cell_ptr
    const uint8_t* cell_ptr(size_t idx) const override { return reinterpret_cast<const uint8*>(&_data[idx]); }

    // Get thr idx's cell_ptr for write
    uint8_t* mutable_cell_ptr(size_t idx) override { return reinterpret_cast<uint8*>(&_data[idx]); }

    size_t item_offset(size_t idx) const { return _item_offsets[idx]; }

    std::vector<uint32_t>* offsets() { return &_item_offsets; }

    // Generate collection slots.
    void prepare_for_read(size_t start_idx, size_t end_idx) override;

    void swap(ColumnVectorBatch* rhs) override {
        ColumnVectorBatch::swap(rhs);
        std::swap(_data, down_cast<ArrayColumnVectorBatch*>(rhs)->_data);
        std::swap(_elements, down_cast<ArrayColumnVectorBatch*>(rhs)->_elements);
        std::swap(_item_offsets, down_cast<ArrayColumnVectorBatch*>(rhs)->_item_offsets);
    }

private:
    std::vector<Collection> _data;

    std::unique_ptr<ColumnVectorBatch> _elements;

    // Stores each collection's start offsets in _elements.
    std::vector<uint32_t> _item_offsets;
};

template class ScalarColumnVectorBatch<int8_t>;
template class ScalarColumnVectorBatch<int16_t>;
template class ScalarColumnVectorBatch<int32_t>;
template class ScalarColumnVectorBatch<uint32_t>;
template class ScalarColumnVectorBatch<int64_t>;
template class ScalarColumnVectorBatch<uint64_t>;
template class ScalarColumnVectorBatch<int128_t>;
template class ScalarColumnVectorBatch<float>;
template class ScalarColumnVectorBatch<double>;
template class ScalarColumnVectorBatch<decimal12_t>;
template class ScalarColumnVectorBatch<uint24_t>;
template class ScalarColumnVectorBatch<Slice>;

} // namespace starrocks
