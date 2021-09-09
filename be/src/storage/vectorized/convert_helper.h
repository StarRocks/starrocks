// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <vector>

#include "column/column.h"
#include "column/datum.h"
#include "common/status.h"
#include "storage/olap_common.h"

namespace starrocks {
class ColumnVectorBatch;
class RowCursor;
class TabletSchema;
class Schema;
class RowBlockV2;
} // namespace starrocks

namespace starrocks::vectorized {

class Schema;

// Used for schema change
class TypeConverter {
public:
    TypeConverter() = default;
    ~TypeConverter() = default;

    virtual Status convert(void* dest, const void* src, MemPool* memPool) const = 0;
};

const TypeConverter* get_type_converter(FieldType from_type, FieldType to_type);

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

    template <typename RowType>
    void convert(RowCursor* dst, const RowType& src) const;

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

class BlockConverter {
public:
    BlockConverter() = default;

    Status init(const ::starrocks::Schema& in_schema, const ::starrocks::Schema& out_schema);

    Status convert(::starrocks::RowBlockV2* dst, ::starrocks::RowBlockV2* src) const;

private:
    std::vector<ColumnId> _cids;
    std::vector<const FieldConverter*> _converters;
};

} // namespace starrocks::vectorized
