// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <vector>

#include "column/column.h"
#include "column/datum.h"
#include "common/status.h"
#include "storage/olap_common.h"

namespace starrocks {
class ColumnVectorBatch;
class TabletSchema;
class Schema;
class TabletColumn;
class TypeInfo;
} // namespace starrocks

namespace starrocks::vectorized {

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

const TypeConverter* get_type_converter(FieldType from_type, FieldType to_type);

class MaterializeTypeConverter {
public:
    MaterializeTypeConverter() = default;
    ~MaterializeTypeConverter() = default;

    virtual Status convert_materialized(ColumnPtr src_col, ColumnPtr dst_col, TypeInfo* src_type) const = 0;
};

const MaterializeTypeConverter* get_materialized_converter(FieldType from_type, MaterializeType to_type);

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

    virtual void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                         uint16_t selected_size) const = 0;
};

const FieldConverter* get_field_converter(FieldType from_type, FieldType to_type);

class RowConverter {
public:
    RowConverter() = default;

    Status init(const TabletSchema& in_schema, const TabletSchema& out_schema);

    Status init(const ::starrocks::Schema& in_schema, const ::starrocks::Schema& out_schema);
    Status init(const Schema& in_schema, const Schema& out_schema);

    void convert(std::vector<Datum>* dst, const std::vector<Datum>& src) const;

private:
    std::vector<ColumnId> _cids;
    std::vector<const FieldConverter*> _converters;
};

class ChunkConverter {
public:
    ChunkConverter() = default;

    Status init(const Schema& in_schema, const Schema& out_schema);

    std::unique_ptr<Chunk> copy_convert(const Chunk& from) const;

    std::unique_ptr<Chunk> move_convert(Chunk* from) const;

private:
    std::shared_ptr<Schema> _out_schema;
    std::vector<const FieldConverter*> _converters;
};

} // namespace starrocks::vectorized
