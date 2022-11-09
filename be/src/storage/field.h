// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/field.h

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

#include <sstream>
#include <string>

#include "runtime/mem_pool.h"
#include "storage/decimal_type_info.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "storage/utils.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace starrocks {

// A Field is used to represent a column in memory format.
// User can use this class to access or deal with column data in memory.
class Field {
public:
    explicit Field() = default;
    explicit Field(const TabletColumn& column)
            : _name(column.name()),
              _type_info(get_type_info(column)),
              _key_coder(get_key_coder(column.type())),
              _index_size(column.index_length()),
              _length(column.length()),
              _is_nullable(column.is_nullable()) {
        DCHECK(column.type() != OLAP_FIELD_TYPE_DECIMAL32 && column.type() != OLAP_FIELD_TYPE_DECIMAL64 &&
               column.type() != OLAP_FIELD_TYPE_DECIMAL128);
    }

    Field(const TabletColumn& column, std::shared_ptr<TypeInfo>&& type_info)
            : _name(column.name()),
              _type_info(type_info),
              _key_coder(get_key_coder(column.type())),
              _index_size(column.index_length()),
              _length(column.length()),
              _is_nullable(column.is_nullable()) {}

    virtual ~Field() = default;

    // Disable copy ctor and assignment.
    Field(const Field&) = delete;
    void operator=(const Field&) = delete;

    // Enable move ctor and move assignment.
    Field(Field&&) = default;
    Field& operator=(Field&&) = default;

    size_t size() const { return _type_info->size(); }
    int32_t length() const { return _length; }
    size_t field_size() const { return size() + 1; }
    size_t index_size() const { return _index_size; }
    const std::string& name() const { return _name; }

    virtual void set_to_max(char* buf) const { return _type_info->set_to_max(buf); }
    void set_to_min(char* buf) const { return _type_info->set_to_min(buf); }

    // This function allocate memory from pool, other than allocate_memory
    // reserve memory from continuous memory.
    virtual char* allocate_value(MemPool* pool) const { return (char*)pool->allocate(_type_info->size()); }

    virtual char* allocate_memory(char* cell_ptr, char* variable_ptr) const { return variable_ptr; }

    virtual size_t get_variable_len() const { return 0; }

    virtual Field* clone() const {
        std::unique_ptr<Field> local = std::make_unique<Field>();
        this->clone(local.get());
        return local.release();
    }

    // Test if these two cell is equal with each other
    template <typename LhsCellType, typename RhsCellType>
    bool equal(const LhsCellType& lhs, const RhsCellType& rhs) const {
        bool l_null = lhs.is_null();
        bool r_null = rhs.is_null();

        if (l_null != r_null) {
            return false;
        } else if (l_null) {
            return true;
        } else {
            return _type_info->equal(lhs.cell_ptr(), rhs.cell_ptr());
        }
    }

    // Only compare column content, without considering NULL condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than rigth,
    //      1 means left bigger than right
    int compare(const void* left, const void* right) const { return _type_info->cmp(left, right); }

    // Compare two types of cell.
    // This function differs compare in that this function compare cell which
    // will consider the condition which cell may be NULL. While compare only
    // compare column content without considering NULL condition.
    // Only compare column content, without considering NULL condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than rigth,
    //      1 means left bigger than right
    template <typename LhsCellType, typename RhsCellType>
    int compare_cell(const LhsCellType& lhs, const RhsCellType& rhs) const {
        bool l_null = lhs.is_null();
        bool r_null = rhs.is_null();
        if (l_null != r_null) {
            return l_null ? -1 : 1;
        }
        return l_null ? 0 : _type_info->cmp(lhs.cell_ptr(), rhs.cell_ptr());
    }

    // Copy source cell's content to destination cell directly.
    // For string type, this function assume that destination has
    // enough space and copy source content into destination without
    // memory allocation.
    template <typename DstCellType, typename SrcCellType>
    void direct_copy(DstCellType* dst, const SrcCellType& src, MemPool* pool = nullptr) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        return _type_info->direct_copy(dst->mutable_cell_ptr(), src.cell_ptr(), pool);
    }

    // deep copy source cell' content to destination cell.
    // For string type, this will allocate data form pool,
    // and copy srouce's conetent.
    template <typename DstCellType, typename SrcCellType>
    void copy_object(DstCellType* dst, const SrcCellType& src, MemPool* pool) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        _type_info->copy_object(dst->mutable_cell_ptr(), src.cell_ptr(), pool);
    }

    // deep copy source cell' content to destination cell.
    // For string type, this will allocate data form pool,
    // and copy srouce's conetent.
    template <typename DstCellType, typename SrcCellType>
    void deep_copy(DstCellType* dst, const SrcCellType& src, MemPool* pool) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        _type_info->deep_copy(dst->mutable_cell_ptr(), src.cell_ptr(), pool);
    }

    // deep copy field content from `src` to `dst` without null-byte
    void deep_copy_content(char* dst, const char* src, MemPool* mem_pool) const {
        _type_info->deep_copy(dst, src, mem_pool);
    }

    // shallow copy field content from `src` to `dst` without null-byte.
    // for string like type, shallow copy only copies Slice, not the actual data pointed by slice.
    void shallow_copy_content(char* dst, const char* src) const { _type_info->shallow_copy(dst, src); }

    //convert and copy field from src to desc
    Status convert_from(char* dest, const char* src, const TypeInfoPtr& src_type, MemPool* mem_pool) const {
        return _type_info->convert_from(dest, src, src_type, mem_pool);
    }

    // used by init scan key stored in string format
    // value_string should end with '\0'
    Status from_string(char* buf, const std::string& value_string) const {
        return _type_info->from_string(buf, value_string);
    }

    // It's a critical function, used by ZoneMapIndexWriter to serialize max and min value
    std::string to_string(const char* src) const { return _type_info->to_string(src); }

    template <typename CellType>
    std::string debug_string(const CellType& cell) const {
        std::stringstream ss;
        if (cell.is_null()) {
            ss << "(null)";
        } else {
            ss << _type_info->to_string(cell.cell_ptr());
        }
        return ss.str();
    }

    template <typename CellType>
    uint32_t hash_code(const CellType& cell, uint32_t seed) const;

    FieldType type() const { return _type_info->type(); }
    const TypeInfoPtr& type_info() const { return _type_info; }
    bool is_nullable() const { return _is_nullable; }

    // similar to `full_encode_ascending`, but only encode part (the first `index_size` bytes) of the value.
    // only applicable to string type
    void encode_ascending(const void* value, std::string* buf) const {
        _key_coder->encode_ascending(value, _index_size, buf);
    }

    // encode the provided `value` into `buf`.
    void full_encode_ascending(const void* value, std::string* buf) const {
        _key_coder->full_encode_ascending(value, buf);
    }

    Status decode_ascending(Slice* encoded_key, uint8_t* cell_ptr, MemPool* pool) const {
        return _key_coder->decode_ascending(encoded_key, _index_size, cell_ptr, pool);
    }

    std::string to_zone_map_string(const char* value) const {
        switch (type()) {
        case OLAP_FIELD_TYPE_DECIMAL32:
        case OLAP_FIELD_TYPE_DECIMAL64:
        case OLAP_FIELD_TYPE_DECIMAL128:
            return get_decimal_zone_map_string(type_info().get(), value);
        default:
            return type_info()->to_string(value);
        }
    }

    void add_sub_field(std::unique_ptr<Field> sub_field) { _sub_fields.emplace_back(std::move(sub_field)); }

    Field* get_sub_field(int i) { return _sub_fields[i].get(); }

    Status convert_to(FieldType type, std::unique_ptr<Field>* output) const {
        std::unique_ptr<Field> new_field(clone());
        new_field->_type_info = get_type_info(type);
        new_field->_key_coder = get_key_coder(type);

        // TODO(zc): we only support fixed length type now.
        new_field->_index_size = static_cast<uint16_t>(new_field->_type_info->size());

        *output = std::move(new_field);
        return Status::OK();
    }

    virtual std::string debug_string() const {
        std::stringstream ss;
        ss << "(type=" << _type_info->type() << ",index_size=" << _index_size << ",is_nullable=" << _is_nullable
           << ",length=" << _length << ")";
        return ss.str();
    }

protected:
    char* allocate_string_value(MemPool* pool) const {
        char* type_value = (char*)pool->allocate(sizeof(Slice));
        assert(type_value != nullptr);
        auto slice = reinterpret_cast<Slice*>(type_value);
        slice->size = _length;
        slice->data = (char*)pool->allocate(slice->size);
        assert(slice->data != nullptr);
        return type_value;
    }

    void clone(Field* other) const {
        other->_type_info = this->_type_info;
        other->_key_coder = this->_key_coder;
        other->_name = this->_name;
        other->_index_size = this->_index_size;
        other->_is_nullable = this->_is_nullable;
        other->_sub_fields.clear();
        for (const auto& f : _sub_fields) {
            Field* item = f->clone();
            other->add_sub_field(std::unique_ptr<Field>(item));
        }
    }

    std::string _name;
    TypeInfoPtr _type_info;
    const KeyCoder* _key_coder;
    uint16_t _index_size;
    uint32_t _length;
    bool _is_nullable;
    std::vector<std::unique_ptr<Field>> _sub_fields;
};

template <typename CellType>
uint32_t Field::hash_code(const CellType& cell, uint32_t seed) const {
    bool is_null = cell.is_null();
    if (is_null) {
        return HashUtil::hash(&is_null, sizeof(is_null), seed);
    }
    return _type_info->hash_code(cell.cell_ptr(), seed);
}

class CharField : public Field {
public:
    explicit CharField() {}
    explicit CharField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override { return _length; }

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = variable_ptr;
        slice->size = _length;
        variable_ptr += slice->size;
        return variable_ptr;
    }

    CharField* clone() const override {
        std::unique_ptr<CharField> local = std::make_unique<CharField>();
        Field::clone(local.get());
        return local.release();
    }

    char* allocate_value(MemPool* pool) const override { return Field::allocate_string_value(pool); }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length;
        memset(slice->data, 0xFF, slice->size);
    }
};

class VarcharField : public Field {
public:
    explicit VarcharField() {}
    explicit VarcharField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override { return _length - OLAP_STRING_MAX_BYTES; }

    // minus OLAP_STRING_MAX_BYTES here just for being compatible with old storage format
    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = variable_ptr;
        slice->size = _length - OLAP_STRING_MAX_BYTES;
        variable_ptr += slice->size;
        return variable_ptr;
    }

    VarcharField* clone() const override {
        std::unique_ptr<VarcharField> local = std::make_unique<VarcharField>();
        Field::clone(local.get());
        return local.release();
    }

    char* allocate_value(MemPool* pool) const override { return Field::allocate_string_value(pool); }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length - OLAP_STRING_MAX_BYTES;
        memset(slice->data, 0xFF, slice->size);
    }
};

class BitmapAggField : public Field {
public:
    explicit BitmapAggField() {}
    explicit BitmapAggField(const TabletColumn& column) : Field(column) {}

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = nullptr;
        return variable_ptr;
    }

    BitmapAggField* clone() const override {
        std::unique_ptr<BitmapAggField> local = std::make_unique<BitmapAggField>();
        Field::clone(local.get());
        return local.release();
    }
};

class HllAggField : public Field {
public:
    explicit HllAggField() {}
    explicit HllAggField(const TabletColumn& column) : Field(column) {}

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = nullptr;
        return variable_ptr;
    }

    HllAggField* clone() const override {
        std::unique_ptr<HllAggField> local = std::make_unique<HllAggField>();
        Field::clone(local.get());
        return local.release();
    }
};

class PercentileAggField : public Field {
public:
    PercentileAggField() {}
    explicit PercentileAggField(const TabletColumn& column) : Field(column) {}

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = nullptr;
        return variable_ptr;
    }

    PercentileAggField* clone() const override {
        std::unique_ptr<PercentileAggField> local = std::make_unique<PercentileAggField>();
        Field::clone(local.get());
        return local.release();
    }
};

class FieldFactory {
public:
    static Field* create(const TabletColumn& column) {
        // for key column
        if (column.is_key()) {
            switch (column.type()) {
            case OLAP_FIELD_TYPE_CHAR:
                return new CharField(column);
            case OLAP_FIELD_TYPE_VARCHAR:
                return new VarcharField(column);
            case OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.subcolumn(0)));
                auto* local = new Field(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            case OLAP_FIELD_TYPE_DECIMAL32:
            case OLAP_FIELD_TYPE_DECIMAL64:
            case OLAP_FIELD_TYPE_DECIMAL128:
                return new Field(column, get_decimal_type_info(column.type(), column.precision(), column.scale()));
            default:
                return new Field(column);
            }
        }

        // for value column
        switch (column.aggregation()) {
        case OLAP_FIELD_AGGREGATION_NONE:
        case OLAP_FIELD_AGGREGATION_SUM:
        case OLAP_FIELD_AGGREGATION_MIN:
        case OLAP_FIELD_AGGREGATION_MAX:
        case OLAP_FIELD_AGGREGATION_REPLACE:
        case OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
            switch (column.type()) {
            case OLAP_FIELD_TYPE_CHAR:
                return new CharField(column);
            case OLAP_FIELD_TYPE_VARCHAR:
                return new VarcharField(column);
            case OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.subcolumn(0)));
                std::unique_ptr<Field> local = std::make_unique<Field>(column);
                local->add_sub_field(std::move(item_field));
                return local.release();
            }
            case OLAP_FIELD_TYPE_DECIMAL32:
            case OLAP_FIELD_TYPE_DECIMAL64:
            case OLAP_FIELD_TYPE_DECIMAL128:
                return new Field(column, get_decimal_type_info(column.type(), column.precision(), column.scale()));
            default:
                return new Field(column);
            }
        case OLAP_FIELD_AGGREGATION_HLL_UNION:
            return new HllAggField(column);
        case OLAP_FIELD_AGGREGATION_BITMAP_UNION:
            return new BitmapAggField(column);
        case OLAP_FIELD_AGGREGATION_PERCENTILE_UNION:
            return new PercentileAggField(column);
        case OLAP_FIELD_AGGREGATION_UNKNOWN:
            LOG(WARNING) << "WOW! value column agg type is unknown";
            return nullptr;
        }
        LOG(WARNING) << "WOW! value column no agg type";
        return nullptr;
    }

    static Field* create_by_type(const FieldType& type) {
        TabletColumn column(OLAP_FIELD_AGGREGATION_NONE, type);
        return create(column);
    }
};

} // namespace starrocks
