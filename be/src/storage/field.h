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
#include "types/logical_type.h"
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
              _index_size(column.index_length()),
              _length(column.length()),
              _is_nullable(column.is_nullable()) {
        DCHECK(column.type() != TYPE_DECIMAL32 && column.type() != TYPE_DECIMAL64 && column.type() != TYPE_DECIMAL128);
    }

    Field(const TabletColumn& column, std::shared_ptr<TypeInfo>&& type_info)
            : _name(column.name()),
              _type_info(type_info),
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
    size_t index_size() const { return _index_size; }
    const std::string& name() const { return _name; }

    virtual void set_to_max(char* buf) const { return _type_info->set_to_max(buf); }
    void set_to_min(char* buf) const { return _type_info->set_to_min(buf); }

    // Only compare column content, without considering NULL condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than rigth,
    //      1 means left bigger than right
    int compare(const void* left, const void* right) const { return _type_info->cmp(left, right); }

    // It's a critical function, used by ZoneMapIndexWriter to serialize max and min value
    std::string to_string(const char* src) const { return _type_info->to_string(src); }

    LogicalType type() const { return _type_info->type(); }
    const TypeInfoPtr& type_info() const { return _type_info; }
    bool is_nullable() const { return _is_nullable; }

    void add_sub_field(std::unique_ptr<Field> sub_field) { _sub_fields.emplace_back(std::move(sub_field)); }

    Field* get_sub_field(int i) { return _sub_fields[i].get(); }

    virtual std::string debug_string() const {
        std::stringstream ss;
        ss << "(type=" << _type_info->type() << ",index_size=" << _index_size << ",is_nullable=" << _is_nullable
           << ",length=" << _length << ")";
        return ss.str();
    }

protected:
    std::string _name;
    TypeInfoPtr _type_info;
    uint16_t _index_size;
    uint32_t _length;
    bool _is_nullable;
    std::vector<std::unique_ptr<Field>> _sub_fields;
};

class CharField : public Field {
public:
    explicit CharField() = default;
    explicit CharField(const TabletColumn& column) : Field(column) {}

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length;
        memset(slice->data, 0xFF, slice->size);
    }
};

class VarcharField : public Field {
public:
    explicit VarcharField() = default;
    explicit VarcharField(const TabletColumn& column) : Field(column) {}

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length - OLAP_STRING_MAX_BYTES;
        memset(slice->data, 0xFF, slice->size);
    }
};

class BitmapAggField : public Field {
public:
    explicit BitmapAggField() = default;
    explicit BitmapAggField(const TabletColumn& column) : Field(column) {}
};

class HllAggField : public Field {
public:
    explicit HllAggField() = default;
    explicit HllAggField(const TabletColumn& column) : Field(column) {}
};

class PercentileAggField : public Field {
public:
    PercentileAggField() = default;
    explicit PercentileAggField(const TabletColumn& column) : Field(column) {}
};

class FieldFactory {
public:
    static Field* create(const TabletColumn& column) {
        // for key column
        if (column.is_key()) {
            switch (column.type()) {
            case TYPE_CHAR:
                return new CharField(column);
            case TYPE_VARCHAR:
                return new VarcharField(column);
            case TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.subcolumn(0)));
                auto* local = new Field(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64:
            case TYPE_DECIMAL128:
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
            case TYPE_CHAR:
                return new CharField(column);
            case TYPE_VARCHAR:
                return new VarcharField(column);
            case TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.subcolumn(0)));
                std::unique_ptr<Field> local = std::make_unique<Field>(column);
                local->add_sub_field(std::move(item_field));
                return local.release();
            }
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64:
            case TYPE_DECIMAL128:
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

    static Field* create_by_type(const LogicalType& type) {
        TabletColumn column(OLAP_FIELD_AGGREGATION_NONE, type);
        return create(column);
    }
};

} // namespace starrocks
