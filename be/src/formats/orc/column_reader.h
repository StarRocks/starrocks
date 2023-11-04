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

#include <orc/OrcFile.hh>
#include <utility>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "common/statusor.h"
#include "formats/orc/orc_mapping.h"
#include "runtime/types.h"
#include "types/logical_type.h"

namespace starrocks {

class OrcChunkReader;

template <typename T>
struct DecimalVectorBatchSelector {
    using Type = std::conditional_t<
            std::is_same_v<T, int64_t>, orc::Decimal64VectorBatch,
            std::conditional_t<std::is_same_v<T, starrocks::int128_t>, orc::Decimal128VectorBatch, void>>;
};

const std::unordered_map<orc::TypeKind, LogicalType> g_orc_starrocks_logical_type_mapping = {
        {orc::BOOLEAN, starrocks::TYPE_BOOLEAN},
        {orc::BYTE, starrocks::TYPE_TINYINT},
        {orc::SHORT, starrocks::TYPE_SMALLINT},
        {orc::INT, starrocks::TYPE_INT},
        {orc::LONG, starrocks::TYPE_BIGINT},
        {orc::FLOAT, starrocks::TYPE_FLOAT},
        {orc::DOUBLE, starrocks::TYPE_DOUBLE},
        {orc::DECIMAL, starrocks::TYPE_DECIMALV2},
        {orc::DATE, starrocks::TYPE_DATE},
        {orc::TIMESTAMP, starrocks::TYPE_DATETIME},
        {orc::STRING, starrocks::TYPE_VARCHAR},
        {orc::BINARY, starrocks::TYPE_VARBINARY},
        {orc::CHAR, starrocks::TYPE_CHAR},
        {orc::VARCHAR, starrocks::TYPE_VARCHAR},
        {orc::TIMESTAMP_INSTANT, starrocks::TYPE_DATETIME},
};

// NOLINTNEXTLINE
const std::set<LogicalType> g_starrocks_int_type = {
        starrocks::TYPE_BOOLEAN, starrocks::TYPE_TINYINT,  starrocks::TYPE_SMALLINT, starrocks::TYPE_INT,
        starrocks::TYPE_BIGINT,  starrocks::TYPE_LARGEINT, starrocks::TYPE_FLOAT,    starrocks::TYPE_DOUBLE};
const std::set<orc::TypeKind> g_orc_decimal_type = {orc::DECIMAL};
const std::set<LogicalType> g_starrocks_decimal_type = {starrocks::TYPE_DECIMAL32, starrocks::TYPE_DECIMAL64,
                                                        starrocks::TYPE_DECIMAL128, starrocks::TYPE_DECIMALV2,
                                                        starrocks::TYPE_DECIMAL};

class ORCColumnReader {
public:
    ORCColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : _type(type), _orc_type(orc_type), _nullable(nullable), _reader(reader) {}
    virtual ~ORCColumnReader() = default;
    virtual Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) = 0;

    static StatusOr<std::unique_ptr<ORCColumnReader>> create(const TypeDescriptor& type,
                                                             const OrcMappingContext& orc_mapping_context,
                                                             bool nullable, OrcChunkReader* reader);
    const orc::Type* get_orc_type() { return _orc_type; }

protected:
    inline void handle_null(orc::ColumnVectorBatch* cvb, NullableColumn* col, size_t column_from, size_t cvb_from,
                            size_t size, bool need_update_has_null = true) {
        DCHECK(_nullable);
        DCHECK(col->is_nullable());
        uint8_t* column_nulls = col->null_column()->get_data().data();
        if (!cvb->hasNulls) {
            memset(column_nulls + column_from, 0, size);
            return;
        }
        uint8_t* cvb_not_nulls = reinterpret_cast<uint8_t*>(cvb->notNull.data());

        for (size_t column_pos = column_from, cvb_pos = cvb_from; column_pos < column_from + size;
             column_pos++, cvb_pos++) {
            column_nulls[column_pos] = !cvb_not_nulls[cvb_pos];
        }
        if (need_update_has_null) {
            col->update_has_null();
        }
    }

    const TypeDescriptor& _type;
    const orc::Type* _orc_type;
    bool _nullable;
    OrcChunkReader* _reader;
};

class PrimitiveColumnReader : public ORCColumnReader {
public:
    PrimitiveColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : ORCColumnReader(type, orc_type, nullable, reader) {}
    ~PrimitiveColumnReader() override = default;
};

class BooleanColumnReader : public PrimitiveColumnReader {
public:
    BooleanColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~BooleanColumnReader() override = default;
    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
};

template <LogicalType Type>
class IntColumnReader : public PrimitiveColumnReader {
public:
    IntColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~IntColumnReader() override = default;
    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

private:
    template <typename OrcColumnVectorBatch>
    Status _fill_int_column_with_null_from_cvb(OrcColumnVectorBatch* data, ColumnPtr& col, size_t from, size_t size);

    template <typename OrcColumnVectorBatch>
    Status _fill_int_column_from_cvb(OrcColumnVectorBatch* data, starrocks::ColumnPtr& col, size_t from, size_t size);
};

template <LogicalType Type>
class DoubleColumnReader : public PrimitiveColumnReader {
public:
    DoubleColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~DoubleColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
};

class DecimalColumnReader : public PrimitiveColumnReader {
public:
    DecimalColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~DecimalColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

private:
    void _fill_decimal_column_from_orc_decimal64(orc::Decimal64VectorBatch* cvb, Column* col, size_t col_start,
                                                 size_t from, size_t size);

    void _fill_decimal_column_from_orc_decimal128(orc::Decimal128VectorBatch* cvb, Column* col, size_t col_start,
                                                  size_t from, size_t size);
};

template <LogicalType DecimalType>
class Decimal32Or64Or128ColumnReader : public PrimitiveColumnReader {
public:
    Decimal32Or64Or128ColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable,
                                   OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~Decimal32Or64Or128ColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

private:
    template <typename T>
    inline void _fill_decimal_column_generic(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
};

class StringColumnReader : public PrimitiveColumnReader {
public:
    StringColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~StringColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
};

class VarbinaryColumnReader : public PrimitiveColumnReader {
public:
    VarbinaryColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~VarbinaryColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
};

class DateColumnReader : public PrimitiveColumnReader {
public:
    DateColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}

    ~DateColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
};

template <bool IsInstant>
class TimestampColumnReader : public PrimitiveColumnReader {
public:
    TimestampColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader)
            : PrimitiveColumnReader(type, orc_type, nullable, reader) {}
    ~TimestampColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;
};

class ComplexColumnReader : public ORCColumnReader {
public:
    ComplexColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader,
                        std::vector<std::unique_ptr<ORCColumnReader>> child_readers)
            : ORCColumnReader(type, orc_type, nullable, reader) {
        _child_readers = std::move(child_readers);
    }
    ~ComplexColumnReader() override = default;

protected:
    static void copy_array_offset(orc::DataBuffer<int64_t>& src, int from, int size, UInt32Column* dst) {
        DCHECK_GT(size, 0);
        if (from == 0 && dst->size() == 1) {
            //           ^^^^^^^^^^^^^^^^ offset column size is 1, means the array column is empty.
            DCHECK_EQ(0, src[0]);
            DCHECK_EQ(0, dst->get_data()[0]);
            dst->resize(size);
            uint32_t* dst_data = dst->get_data().data();
            for (int i = 1; i < size; i++) {
                dst_data[i] = static_cast<uint32_t>(src[i]);
            }
        } else {
            DCHECK_GT(dst->size(), 1);
            int dst_pos = dst->size();
            dst->resize(dst_pos + size - 1);
            uint32_t* dst_data = dst->get_data().data();

            // Equivalent to the following code:
            // ```
            //  for (int i = from + 1; i < from + size; i++, dst_pos++) {
            //      dst_data[dst_pos] = dst_data[dst_pos-1] + (src[i] - src[i-1]);
            //  }
            // ```
            uint32_t prev_starrocks_offset = dst_data[dst_pos - 1];
            int64_t prev_orc_offset = src[from];
            for (int i = from + 1; i < from + size; i++, dst_pos++) {
                int64_t curr_orc_offset = src[i];
                int64_t diff = curr_orc_offset - prev_orc_offset;
                uint32_t curr_starrocks_offset = prev_starrocks_offset + diff;
                dst_data[dst_pos] = curr_starrocks_offset;
                prev_orc_offset = curr_orc_offset;
                prev_starrocks_offset = curr_starrocks_offset;
            }
        }
    }

    std::vector<std::unique_ptr<ORCColumnReader>> _child_readers;
};

class ArrayColumnReader : public ComplexColumnReader {
public:
    ArrayColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader,
                      std::vector<std::unique_ptr<ORCColumnReader>> child_readers)
            : ComplexColumnReader(type, orc_type, nullable, reader, std::move(child_readers)) {}
    ~ArrayColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

private:
    Status _fill_array_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
};

class MapColumnReader : public ComplexColumnReader {
public:
    MapColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader,
                    std::vector<std::unique_ptr<ORCColumnReader>> child_readers)
            : ComplexColumnReader(type, orc_type, nullable, reader, std::move(child_readers)) {}
    ~MapColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

private:
    Status _fill_map_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
};

class StructColumnReader : public ComplexColumnReader {
public:
    StructColumnReader(const TypeDescriptor& type, const orc::Type* orc_type, bool nullable, OrcChunkReader* reader,
                       std::vector<std::unique_ptr<ORCColumnReader>> child_readers)
            : ComplexColumnReader(type, orc_type, nullable, reader, std::move(child_readers)) {}
    ~StructColumnReader() override = default;

    Status get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) override;

private:
    Status _fill_struct_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size);
};

} // namespace starrocks