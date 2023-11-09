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

#pragma once

#include <memory>
#include <vector>

#include "column/column.h"
#include "column/datum.h"
#include "common/status.h"
#include "storage/olap_common.h"
#include "types/logical_type.h"

namespace starrocks {
class TabletSchema;
class Schema;
class TabletColumn;
class TypeInfo;
class Schema;

// Used for schema change
class TypeConverter {
public:
    TypeConverter() = default;
    ~TypeConverter() = default;

    virtual Status convert(void* dest, const void* src, MemPool* memPool) const = 0;

    virtual Status convert_column(TypeInfo* src_type, const Column& src, TypeInfo* dst_type, Column* dst,
                                  MemPool* mem_pool) const;

private:
    friend class SchemaChangeTest;

    virtual Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                                 MemPool* mem_pool) const = 0;
};

const TypeConverter* get_type_converter(LogicalType from_type, LogicalType to_type);

class MaterializeTypeConverter {
public:
    MaterializeTypeConverter() = default;
    ~MaterializeTypeConverter() = default;

    virtual Status convert_materialized(ColumnPtr src_col, ColumnPtr dst_col, TypeInfo* src_type) const = 0;
};

const MaterializeTypeConverter* get_materialized_converter(LogicalType from_type, MaterializeType to_type);

class FieldConverter {
public:
    FieldConverter() = default;
    virtual ~FieldConverter() = default;
    virtual void convert(void* dst, const void* src) const = 0;

    virtual void convert(Datum* dst, const Datum& src) const = 0;

    virtual ColumnPtr copy_convert(const Column& src) const = 0;

    virtual ColumnPtr move_convert(Column* src) const {
        auto ret = copy_convert(*src);
        src->reset_column();
        return ret;
    }
};

const FieldConverter* get_field_converter(LogicalType from_type, LogicalType to_type);

class RowConverter {
public:
    RowConverter() = default;

    Status init(const TabletSchema& in_schema, const TabletSchema& out_schema);

    Status init(const Schema& in_schema, const Schema& out_schema);

    void convert(std::vector<Datum>* dst, const std::vector<Datum>& src) const;

private:
    std::vector<ColumnId> _cids;
    std::vector<const FieldConverter*> _converters;
};

} // namespace starrocks
