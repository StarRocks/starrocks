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

#include "formats/orc/fill_function.h"

#include <glog/logging.h>

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner_orc.h"
#include "exprs/cast_expr.h"
#include "exprs/literal.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "types/logical_type.h"
#include "util/runtime_profile.h"
#include "util/timezone_utils.h"

namespace starrocks {

// NOLINTNEXTLINE
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
        {orc::BINARY, starrocks::TYPE_VARCHAR},
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

static void fill_boolean_column(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from, size_t size,
                                const starrocks::TypeDescriptor& type_desc, const starrocks::OrcMappingPtr& mapping,
                                void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col_start + size);

    auto* values = starrocks::ColumnHelper::cast_to_raw<starrocks::TYPE_BOOLEAN>(col)->get_data().data();

    auto* cvbd = data->data.data();
    for (int i = col_start; i < col_start + size; ++i, ++from) {
        values[i] = (cvbd[from] != 0);
    }
}
static void fill_boolean_column_with_null(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from,
                                          size_t size, const starrocks::TypeDescriptor& type_desc,
                                          const starrocks::OrcMappingPtr& mapping, void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);
    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = starrocks::ColumnHelper::as_raw_column<starrocks::NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = starrocks::ColumnHelper::cast_to_raw<starrocks::TYPE_BOOLEAN>(c->data_column())->get_data().data();

    auto* cvbd = data->data.data();
    auto pos = from;
    if (cvb->hasNulls) {
        auto* cvbn = reinterpret_cast<uint8_t*>(cvb->notNull.data());
        for (int i = col_start; i < col_start + size; ++i, ++pos) {
            nulls[i] = !cvbn[pos];
        }
    }
    pos = from;
    for (int i = col_start; i < col_start + size; ++i, ++pos) {
        values[i] = (cvbd[pos] != 0);
    }
    c->update_has_null();
}
template <starrocks::LogicalType Type, typename OrcColumnVectorBatch>
static void fill_int_column_from_cvb(OrcColumnVectorBatch* data, starrocks::ColumnPtr& col, size_t from, size_t size,
                                     const starrocks::TypeDescriptor& type_desc,
                                     const starrocks::OrcMappingPtr& mapping, void* ctx) {
    auto* reader = static_cast<starrocks::OrcChunkReader*>(ctx);

    int col_start = col->size();
    col->resize(col_start + size);

    auto* values = starrocks::ColumnHelper::cast_to_raw<Type>(col)->get_data().data();

    auto* cvbd = data->data.data();

    auto pos = from;
    for (int i = col_start; i < col_start + size; ++i, ++pos) {
        values[i] = cvbd[pos];
    }

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    constexpr bool wild_type = (Type == starrocks::TYPE_BIGINT || Type == starrocks::TYPE_LARGEINT);
    // don't do overflow check on BIGINT(int64_t) or LARGEINT(int128_t)
    if constexpr (!wild_type) {
        if (reader->get_broker_load_mode() && from == 0 && col_start == 0) {
            auto filter = reader->get_broker_load_fiter()->data();
            bool reported = false;
            for (int i = 0; i < size; i++) {
                int64_t value = cvbd[i];
                // overflow.
                if (value < numeric_limits<starrocks::RunTimeCppType<Type>>::lowest() ||
                    value > numeric_limits<starrocks::RunTimeCppType<Type>>::max()) {
                    // can not accept null, so we have to discard it.
                    filter[i] = 0;
                    if (!reported) {
                        reported = true;
                        auto slot = reader->get_current_slot();
                        std::string error_msg = strings::Substitute(
                                "Value '$0' is out of range. The type of '$1' is $2'", std::to_string(value),
                                slot->col_name(), slot->type().debug_string());
                        reader->report_error_message(error_msg);
                    }
                }
            }
        }
    }
}
template <starrocks::LogicalType Type, typename OrcColumnVectorBatch>
static void fill_int_column_with_null_from_cvb(OrcColumnVectorBatch* data, starrocks::ColumnPtr& col, size_t from,
                                               size_t size, const starrocks::TypeDescriptor& type_desc,
                                               const starrocks::OrcMappingPtr& mapping, void* ctx) {
    auto* reader = static_cast<starrocks::OrcChunkReader*>(ctx);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = starrocks::ColumnHelper::as_raw_column<starrocks::NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = starrocks::ColumnHelper::cast_to_raw<Type>(c->data_column())->get_data().data();

    auto* cvbd = data->data.data();
    auto pos = from;
    if (data->hasNulls) {
        auto* cvbn = reinterpret_cast<uint8_t*>(data->notNull.data());
        for (int i = col_start; i < col_start + size; ++i, ++pos) {
            nulls[i] = !cvbn[pos];
        }
    }
    pos = from;
    for (int i = col_start; i < col_start + size; ++i, ++pos) {
        values[i] = cvbd[pos];
    }

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    if (reader->get_broker_load_mode() && from == 0 && col_start == 0) {
        auto filter = reader->get_broker_load_fiter()->data();
        auto strict_mode = reader->get_strict_mode();
        bool reported = false;

        if (strict_mode) {
            for (int i = 0; i < size; i++) {
                int64_t value = cvbd[i];
                // overflow.
                if (nulls[i] == 0 && (value < numeric_limits<starrocks::RunTimeCppType<Type>>::lowest() ||
                                      value > numeric_limits<starrocks::RunTimeCppType<Type>>::max())) {
                    filter[i] = 0;
                    if (!reported) {
                        reported = true;
                        auto slot = reader->get_current_slot();
                        std::string error_msg = strings::Substitute(
                                "Value '$0' is out of range. The type of '$1' is $2'", std::to_string(value),
                                slot->col_name(), slot->type().debug_string());
                        reader->report_error_message(error_msg);
                    }
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                int64_t value = cvbd[i];
                // overflow.
                if (nulls[i] == 0 && (value < numeric_limits<starrocks::RunTimeCppType<Type>>::lowest() ||
                                      value > numeric_limits<starrocks::RunTimeCppType<Type>>::max())) {
                    nulls[i] = 1;
                }
            }
        }
    }

    c->update_has_null();
}
template <starrocks::LogicalType Type>
static void fill_int_column(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from, size_t size,
                            const starrocks::TypeDescriptor& type_desc, const starrocks::OrcMappingPtr& mapping,
                            void* ctx) {
    // try to dyn_cast to long vector batch first. in most case, the cast will succeed.
    // so there is no performance loss comparing to call `down_cast` directly
    // because in `down_cast` implementation, there is also a dyn_cast.
    {
        auto* data = dynamic_cast<orc::LongVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_from_cvb<Type, orc::LongVectorBatch>(data, col, from, size, type_desc, nullptr, ctx);
        }
    }
    // if dyn_cast to long vector batch failed, try to dyn_cast to double vector batch for best effort
    // it only happens when slot type and orc type don't match.
    {
        auto* data = dynamic_cast<orc::DoubleVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_from_cvb<Type, orc::DoubleVectorBatch>(data, col, from, size, type_desc, nullptr,
                                                                          ctx);
        }
    }
    // we have nothing to fill, but have to resize column to save from crash.
    col->resize(col->size() + size);
}
template <starrocks::LogicalType Type>
static void fill_int_column_with_null(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from, size_t size,
                                      const starrocks::TypeDescriptor& type_desc,
                                      const starrocks::OrcMappingPtr& mapping, void* ctx) {
    {
        auto* data = dynamic_cast<orc::LongVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_with_null_from_cvb<Type, orc::LongVectorBatch>(data, col, from, size, type_desc,
                                                                                  nullptr, ctx);
        }
    }
    {
        auto* data = dynamic_cast<orc::DoubleVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_with_null_from_cvb<Type, orc::DoubleVectorBatch>(data, col, from, size, type_desc,
                                                                                    nullptr, ctx);
        }
    }
    // we have nothing to fill, but have to resize column to save from crash.
    col->resize(col->size() + size);
}
template <starrocks::LogicalType Type>
static void fill_float_column(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from, size_t size,
                              const starrocks::TypeDescriptor& type_desc, const starrocks::OrcMappingPtr& mapping,
                              void* ctx) {
    auto* data = down_cast<orc::DoubleVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col_start + size);

    auto* values = starrocks::ColumnHelper::cast_to_raw<Type>(col)->get_data().data();

    auto* cvbd = data->data.data();
    for (int i = col_start; i < col_start + size; ++i, ++from) {
        values[i] = cvbd[from];
    }
}
template <starrocks::LogicalType Type>
static void fill_float_column_with_null(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from,
                                        size_t size, const starrocks::TypeDescriptor& type_desc,
                                        const starrocks::OrcMappingPtr& mapping, void* ctx) {
    auto* data = down_cast<orc::DoubleVectorBatch*>(cvb);
    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = starrocks::ColumnHelper::as_raw_column<starrocks::NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = starrocks::ColumnHelper::cast_to_raw<Type>(c->data_column())->get_data().data();

    auto* cvbd = data->data.data();
    auto pos = from;
    if (cvb->hasNulls) {
        auto* cvbn = reinterpret_cast<uint8_t*>(cvb->notNull.data());
        for (int i = col_start; i < col_start + size; ++i, ++pos) {
            nulls[i] = !cvbn[pos];
        }
    }
    pos = from;
    for (int i = col_start; i < col_start + size; ++i, ++pos) {
        values[i] = cvbd[pos];
    }
    c->update_has_null();
}
static void fill_decimal_column_from_orc_decimal64(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from,
                                                   size_t size, const starrocks::TypeDescriptor& type_desc,
                                                   const starrocks::OrcMappingPtr& mapping, void* ctx) {
    auto* data = down_cast<orc::Decimal64VectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    static_assert(sizeof(starrocks::DecimalV2Value) == sizeof(starrocks::int128_t));
    auto* values =
            reinterpret_cast<starrocks::int128_t*>(down_cast<starrocks::DecimalColumn*>(col.get())->get_data().data());

    auto* cvbd = data->values.data();

    for (int i = col_start; i < col_start + size; ++i, ++from) {
        values[i] = static_cast<starrocks::int128_t>(cvbd[from]);
    }

    if (starrocks::DecimalV2Value::SCALE < data->scale) {
        starrocks::int128_t d =
                starrocks::DecimalV2Value::get_scale_base(data->scale - starrocks::DecimalV2Value::SCALE);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] / d;
        }
    } else if (starrocks::DecimalV2Value::SCALE > data->scale) {
        starrocks::int128_t m =
                starrocks::DecimalV2Value::get_scale_base(starrocks::DecimalV2Value::SCALE - data->scale);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] * m;
        }
    }
}
static void fill_decimal_column_with_null_from_orc_decimal64(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col,
                                                             size_t from, size_t size,
                                                             const starrocks::TypeDescriptor& type_desc,
                                                             const starrocks::OrcMappingPtr& mapping, void* ctx) {
    int col_start = col->size();
    auto c = starrocks::ColumnHelper::as_raw_column<starrocks::NullableColumn>(col);
    auto& null_column = c->null_column();
    auto& data_column = c->data_column();

    fill_decimal_column_from_orc_decimal64(cvb, data_column, from, size, type_desc, nullptr, ctx);
    DCHECK_EQ(col_start + size, data_column->size());
    null_column->resize(data_column->size());
    auto* nulls = null_column->get_data().data();

    auto pos = from;
    if (cvb->hasNulls) {
        auto* cvbn = reinterpret_cast<uint8_t*>(cvb->notNull.data());
        for (int i = col_start; i < col_start + size; ++i, ++pos) {
            nulls[i] = !cvbn[pos];
        }
        c->update_has_null();
    }
}
static void fill_decimal_column_from_orc_decimal128(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from,
                                                    size_t size, const starrocks::TypeDescriptor& type_desc,
                                                    const starrocks::OrcMappingPtr& mapping, void* ctx) {
    auto* data = down_cast<orc::Decimal128VectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto* values =
            reinterpret_cast<starrocks::int128_t*>(down_cast<starrocks::DecimalColumn*>(col.get())->get_data().data());

    for (int i = col_start; i < col_start + size; ++i, ++from) {
        uint64_t hi = data->values[from].getHighBits();
        uint64_t lo = data->values[from].getLowBits();
        values[i] = (((starrocks::int128_t)hi) << 64) | (starrocks::int128_t)lo;
    }
    if (starrocks::DecimalV2Value::SCALE < data->scale) {
        starrocks::int128_t d =
                starrocks::DecimalV2Value::get_scale_base(data->scale - starrocks::DecimalV2Value::SCALE);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] / d;
        }
    } else if (starrocks::DecimalV2Value::SCALE > data->scale) {
        starrocks::int128_t m =
                starrocks::DecimalV2Value::get_scale_base(starrocks::DecimalV2Value::SCALE - data->scale);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] * m;
        }
    }
}
static void fill_decimal_column_with_null_from_orc_decimal128(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col,
                                                              size_t from, size_t size,
                                                              const starrocks::TypeDescriptor& type_desc,
                                                              const starrocks::OrcMappingPtr& mapping, void* ctx) {
    int col_start = col->size();
    auto c = starrocks::ColumnHelper::as_raw_column<starrocks::NullableColumn>(col);
    auto& null_column = c->null_column();
    auto& data_column = c->data_column();

    fill_decimal_column_from_orc_decimal128(cvb, data_column, from, size, type_desc, nullptr, ctx);
    DCHECK_EQ(col_start + size, data_column->size());
    null_column->resize(data_column->size());
    auto* nulls = null_column->get_data().data();

    auto pos = from;
    if (cvb->hasNulls) {
        auto* cvbn = reinterpret_cast<uint8_t*>(cvb->notNull.data());
        for (int i = col_start; i < col_start + size; ++i, ++pos) {
            nulls[i] = !cvbn[pos];
        }
        c->update_has_null();
    }
}
static void fill_decimal_column(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from, size_t size,
                                const starrocks::TypeDescriptor& type_desc, const starrocks::OrcMappingPtr& mapping,
                                void* ctx) {
    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        fill_decimal_column_from_orc_decimal64(cvb, col, from, size, type_desc, nullptr, ctx);
    } else {
        fill_decimal_column_from_orc_decimal128(cvb, col, from, size, type_desc, nullptr, ctx);
    }
}
static void fill_decimal_column_with_null(orc::ColumnVectorBatch* cvb, starrocks::ColumnPtr& col, size_t from,
                                          size_t size, const starrocks::TypeDescriptor& type_desc,
                                          const starrocks::OrcMappingPtr& mapping, void* ctx) {
    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        fill_decimal_column_with_null_from_orc_decimal64(cvb, col, from, size, type_desc, nullptr, ctx);
    } else {
        fill_decimal_column_with_null_from_orc_decimal128(cvb, col, from, size, type_desc, nullptr, ctx);
    }
}
template <typename T, LogicalType DecimalType, bool is_nullable>
static inline void fill_decimal_column_generic(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                               const OrcMappingPtr& mapping, void* ctx) {
    static_assert(is_decimal_column<RunTimeColumnType<DecimalType>>,
                  "Only support TYPE_DECIMAL32|TYPE_DECIMAL64|TYPE_DECIMAL128");

    using OrcDecimalBatchType = typename DecimalVectorBatchSelector<T>::Type;
    using ColumnType = RunTimeColumnType<DecimalType>;
    using Type = RunTimeCppType<DecimalType>;

    auto data = (OrcDecimalBatchType*)cvb;
    int col_start = col->size();
    col->resize(col->size() + size);

    ColumnType* decimal_column = nullptr;
    if constexpr (is_nullable) {
        auto nullable_column = ColumnHelper::as_raw_column<NullableColumn>(col);
        if (cvb->hasNulls) {
            auto nulls = nullable_column->null_column()->get_data().data();
            auto cvbn = reinterpret_cast<uint8_t*>(cvb->notNull.data());
            bool has_null = col->has_null();
            auto src_idx = from;
            for (auto dst_idx = col_start; dst_idx < col_start + size; ++dst_idx, ++src_idx) {
                has_null |= cvbn[src_idx] == 0;
                nulls[dst_idx] = !cvbn[src_idx];
            }
            nullable_column->set_has_null(has_null);
        }
        decimal_column = ColumnHelper::cast_to_raw<DecimalType>(nullable_column->data_column());
    } else {
        decimal_column = ColumnHelper::cast_to_raw<DecimalType>(col);
    }

    auto values = decimal_column->get_data().data();
    if (data->scale == decimal_column->scale()) {
        auto src_idx = from;
        for (int dst_idx = col_start; dst_idx < col_start + size; ++dst_idx, ++src_idx) {
            T original_value;
            if constexpr (std::is_same_v<T, int128_t>) {
                original_value = ((int128_t)data->values[src_idx].getHighBits()) << 64;
                original_value |= (int128_t)data->values[src_idx].getLowBits();
            } else {
                original_value = data->values[src_idx];
            }
            DecimalV3Cast::to_decimal_trivial<T, Type, false>(original_value, &values[dst_idx]);
        }
    } else if (data->scale > decimal_column->scale()) {
        auto scale_factor = get_scale_factor<T>(data->scale - decimal_column->scale());
        auto src_idx = from;
        for (int dst_idx = col_start; dst_idx < col_start + size; ++dst_idx, ++src_idx) {
            T original_value;
            if constexpr (std::is_same_v<T, int128_t>) {
                original_value = ((int128_t)data->values[src_idx].getHighBits()) << 64;
                original_value |= (int128_t)data->values[src_idx].getLowBits();
            } else {
                original_value = data->values[src_idx];
            }
            DecimalV3Cast::to_decimal<T, Type, T, false, false>(original_value, scale_factor, &values[dst_idx]);
        }
    } else {
        auto scale_factor = get_scale_factor<Type>(decimal_column->scale() - data->scale);
        auto src_idx = from;
        for (int dst_idx = col_start; dst_idx < col_start + size; ++dst_idx, ++src_idx) {
            T original_value;
            if constexpr (std::is_same_v<T, int128_t>) {
                original_value = ((int128_t)data->values[src_idx].getHighBits()) << 64;
                original_value |= (int128_t)data->values[src_idx].getLowBits();
            } else {
                original_value = data->values[src_idx];
            }
            DecimalV3Cast::to_decimal<T, Type, Type, true, false>(original_value, scale_factor, &values[dst_idx]);
        }
    }
}
template <LogicalType DecimalType, bool is_nullable>
static inline void fill_decimal_column_from_orc_decimal64_or_decimal128(orc::ColumnVectorBatch* cvb, ColumnPtr& col,
                                                                        size_t from, size_t size,
                                                                        const TypeDescriptor& type_desc,
                                                                        const OrcMappingPtr& mapping, void* ctx) {
    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        fill_decimal_column_generic<int64_t, DecimalType, is_nullable>(cvb, col, from, size, nullptr, ctx);
    } else {
        fill_decimal_column_generic<int128_t, DecimalType, is_nullable>(cvb, col, from, size, nullptr, ctx);
    }
}
static void fill_decimal32_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                  const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL32, false>(cvb, col, from, size, type_desc,
                                                                                nullptr, ctx);
}
static void fill_decimal64_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                  const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL64, false>(cvb, col, from, size, type_desc,
                                                                                nullptr, ctx);
}
static void fill_decimal128_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                   const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL128, false>(cvb, col, from, size, type_desc,
                                                                                 nullptr, ctx);
}
static void fill_decimal32_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                            const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL32, true>(cvb, col, from, size, type_desc, nullptr,
                                                                               ctx);
}
static void fill_decimal64_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                            const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL64, true>(cvb, col, from, size, type_desc, nullptr,
                                                                               ctx);
}
static void fill_decimal128_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                             const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL128, true>(cvb, col, from, size, type_desc,
                                                                                nullptr, ctx);
}
static void fill_string_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                               const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* reader = static_cast<OrcChunkReader*>(ctx);

    auto* data = down_cast<orc::StringVectorBatch*>(cvb);

    size_t len = 0;
    for (size_t i = 0; i < size; ++i) {
        len += data->length[from + i];
    }

    int col_start = col->size();
    auto* values = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(col);

    values->get_offset().reserve(col_start + size);
    values->get_bytes().reserve(len);

    auto& vb = values->get_bytes();
    auto& vo = values->get_offset();
    int pos = from;

    if (type_desc.type == TYPE_CHAR) {
        // Possibly there are some zero padding characters in value, we have to strip them off.
        for (int i = col_start; i < col_start + size; ++i, ++pos) {
            size_t str_size = remove_trailing_spaces(data->data[pos], data->length[pos]);
            vb.insert(vb.end(), data->data[pos], data->data[pos] + str_size);
            vo.emplace_back(vb.size());
        }
    } else {
        for (int i = col_start; i < col_start + size; ++i, ++pos) {
            vb.insert(vb.end(), data->data[pos], data->data[pos] + data->length[pos]);
            vo.emplace_back(vb.size());
        }
    }

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    if (reader->get_broker_load_mode() && from == 0 && col_start == 0) {
        auto filter = reader->get_broker_load_fiter()->data();
        // only report once.
        bool reported = false;
        for (int i = 0; i < size; i++) {
            // overflow.
            if (type_desc.len > 0 && data->length[i] > type_desc.len) {
                // can not accept null, so we have to discard it.
                filter[i] = 0;
                if (!reported) {
                    reported = true;
                    std::string raw_data(data->data[i], data->length[i]);
                    auto slot = reader->get_current_slot();
                    std::string error_msg =
                            strings::Substitute("String '$0' is too long. The type of '$1' is $2'", raw_data,
                                                slot->col_name(), slot->type().debug_string());
                    reader->report_error_message(error_msg);
                }
            }
        }
    }
}
static void fill_string_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                         const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* reader = static_cast<OrcChunkReader*>(ctx);

    auto* data = down_cast<orc::StringVectorBatch*>(cvb);

    size_t len = 0;
    for (size_t i = 0; i < size; ++i) {
        len += data->length[from + i];
    }

    int col_start = col->size();

    auto* c = ColumnHelper::as_raw_column<NullableColumn>(col);

    c->null_column()->resize(col->size() + size);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(c->data_column());

    values->get_offset().reserve(values->get_offset().size() + size);
    values->get_bytes().reserve(values->get_bytes().size() + len);

    auto& vb = values->get_bytes();
    auto& vo = values->get_offset();

    int pos = from;
    if (cvb->hasNulls) {
        if (type_desc.type == TYPE_CHAR) {
            // Possibly there are some zero padding characters in value, we have to strip them off.
            for (int i = col_start; i < col_start + size; ++i, ++pos) {
                nulls[i] = !cvb->notNull[pos];
                if (cvb->notNull[pos]) {
                    size_t str_size = remove_trailing_spaces(data->data[pos], data->length[pos]);
                    vb.insert(vb.end(), data->data[pos], data->data[pos] + str_size);
                    vo.emplace_back(vb.size());
                } else {
                    vo.emplace_back(vb.size());
                }
            }
        } else {
            for (int i = col_start; i < col_start + size; ++i, ++pos) {
                nulls[i] = !cvb->notNull[pos];
                if (cvb->notNull[pos]) {
                    vb.insert(vb.end(), data->data[pos], data->data[pos] + data->length[pos]);
                    vo.emplace_back(vb.size());
                } else {
                    vo.emplace_back(vb.size());
                }
            }
        }
    } else {
        if (type_desc.type == TYPE_CHAR) {
            // Possibly there are some zero padding characters in value, we have to strip them off.
            for (int i = col_start; i < col_start + size; ++i, ++pos) {
                size_t str_size = remove_trailing_spaces(data->data[pos], data->length[pos]);
                vb.insert(vb.end(), data->data[pos], data->data[pos] + str_size);
                vo.emplace_back(vb.size());
            }
        } else {
            for (int i = col_start; i < col_start + size; ++i, ++pos) {
                vb.insert(vb.end(), data->data[pos], data->data[pos] + data->length[pos]);
                vo.emplace_back(vb.size());
            }
        }
    }

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    if (reader->get_broker_load_mode() && from == 0 && col_start == 0) {
        auto* filter = reader->get_broker_load_fiter()->data();
        auto strict_mode = reader->get_strict_mode();
        bool reported = false;

        if (strict_mode) {
            for (int i = 0; i < size; i++) {
                // overflow.
                if (nulls[i] == 0 && type_desc.len > 0 && data->length[i] > type_desc.len) {
                    filter[i] = 0;
                    if (!reported) {
                        reported = true;
                        std::string raw_data(data->data[i], data->length[i]);
                        auto slot = reader->get_current_slot();
                        std::string error_msg =
                                strings::Substitute("String '$0' is too long. The type of '$1' is $2'", raw_data,
                                                    slot->col_name(), slot->type().debug_string());
                        reader->report_error_message(error_msg);
                    }
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                // overflow.
                if (nulls[i] == 0 && type_desc.len > 0 && data->length[i] > type_desc.len) {
                    nulls[i] = 1;
                }
            }
        }
    }

    c->update_has_null();
}

static void fill_date_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                             const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto* values = ColumnHelper::cast_to_raw<TYPE_DATE>(col)->get_data().data();
    for (int i = col_start; i < col_start + size; ++i, ++from) {
        OrcDateHelper::orc_date_to_native_date(&(values[i]), data->data[from]);
    }
}
static void fill_date_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                       const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = ColumnHelper::cast_to_raw<TYPE_DATE>(c->data_column())->get_data().data();

    for (int i = col_start; i < col_start + size; ++i, ++from) {
        nulls[i] = cvb->hasNulls && !cvb->notNull[from];
        if (!nulls[i]) {
            OrcDateHelper::orc_date_to_native_date(&(values[i]), data->data[from]);
        }
    }
    c->update_has_null();
}

static void fill_timestamp_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                  const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* reader = static_cast<OrcChunkReader*>(ctx);
    auto* data = down_cast<orc::TimestampVectorBatch*>(cvb);
    int col_start = col->size();
    col->resize(col->size() + size);
    auto* values = ColumnHelper::cast_to_raw<TYPE_DATETIME>(col)->get_data().data();
    bool use_ns = reader->use_nanoseconds_in_datetime();
    for (int i = col_start; i < col_start + size; ++i, ++from) {
        int64_t ns = 0;
        if (use_ns) {
            ns = data->nanoseconds[from];
        }
        OrcTimestampHelper::orc_ts_to_native_ts(&(values[i]), reader->tzinfo(), reader->tzoffset_in_seconds(),
                                                data->data[from], ns);
    }
}
static void fill_timestamp_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                            const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* reader = static_cast<OrcChunkReader*>(ctx);
    auto* data = down_cast<orc::TimestampVectorBatch*>(cvb);
    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = ColumnHelper::cast_to_raw<TYPE_DATETIME>(c->data_column())->get_data().data();
    bool use_ns = reader->use_nanoseconds_in_datetime();

    for (int i = col_start; i < col_start + size; ++i, ++from) {
        nulls[i] = cvb->hasNulls && !cvb->notNull[from];
        if (!nulls[i]) {
            int64_t ns = 0;
            if (use_ns) {
                ns = data->nanoseconds[from];
            }
            OrcTimestampHelper::orc_ts_to_native_ts(&(values[i]), reader->tzinfo(), reader->tzoffset_in_seconds(),
                                                    data->data[from], ns);
        }
    }

    c->update_has_null();
}
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
static void fill_array_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                              const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
    auto* col_array = down_cast<ArrayColumn*>(col.get());

    UInt32Column* offsets = col_array->offsets_column().get();
    copy_array_offset(orc_list->offsets, from, size + 1, offsets);

    ColumnPtr& elements = col_array->elements_column();
    const TypeDescriptor& child_type = type_desc.children[0];
    const FillColumnFunction& fn_fill_elements = find_fill_func(child_type.type, true);
    const int elements_from = implicit_cast<int>(orc_list->offsets[from]);
    const int elements_size = implicit_cast<int>(orc_list->offsets[from + size] - elements_from);

    fn_fill_elements(orc_list->elements.get(), elements, elements_from, elements_size, child_type,
                     mapping->get_column_id_or_child_mapping(0).orc_mapping, ctx);
}
static void fill_array_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                        const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
    auto* col_nullable = down_cast<NullableColumn*>(col.get());
    auto* col_array = down_cast<ArrayColumn*>(col_nullable->data_column().get());

    if (!orc_list->hasNulls) {
        fill_array_column(orc_list, col_nullable->data_column(), from, size, type_desc, mapping, ctx);
        col_nullable->null_column()->resize(col_array->size());
        return;
    }
    // else

    const int end = from + size;

    int i = from;
    while (i < end) {
        int j = i;
        // Loop until NULL or end of batch.
        while (j < end && orc_list->notNull[j]) {
            j++;
        }
        if (j > i) {
            fill_array_column(orc_list, col_nullable->data_column(), i, j - i, type_desc, mapping, ctx);
            col_nullable->null_column()->resize(col_array->size());
        }

        if (j == end) {
            break;
        }
        DCHECK(!orc_list->notNull[j]);
        i = j++;
        // Loop until not NULL or end of batch.
        while (j < end && !orc_list->notNull[j]) {
            j++;
        }
        col_nullable->append_nulls(j - i);
        i = j;
    }
}
static void fill_map_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                            const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* orc_map = down_cast<orc::MapVectorBatch*>(cvb);
    auto* col_map = down_cast<MapColumn*>(col.get());

    UInt32Column* offsets = col_map->offsets_column().get();
    copy_array_offset(orc_map->offsets, from, size + 1, offsets);

    ColumnPtr& keys = col_map->keys_column();
    const int keys_from = implicit_cast<int>(orc_map->offsets[from]);
    const int keys_size = implicit_cast<int>(orc_map->offsets[from + size] - keys_from);

    if (type_desc.children[0].is_unknown_type()) {
        keys->append_default(keys_size);
    } else {
        const TypeDescriptor& key_type = type_desc.children[0];
        const FillColumnFunction& fn_fill_keys = find_fill_func(key_type.type, true);
        fn_fill_keys(orc_map->keys.get(), keys, keys_from, keys_size, key_type,
                     mapping->get_column_id_or_child_mapping(0).orc_mapping, ctx);
    }

    ColumnPtr& values = col_map->values_column();
    const int values_from = implicit_cast<int>(orc_map->offsets[from]);
    const int values_size = implicit_cast<int>(orc_map->offsets[from + size] - values_from);
    if (type_desc.children[1].is_unknown_type()) {
        values->append_default(values_size);
    } else {
        const TypeDescriptor& value_type = type_desc.children[1];
        const FillColumnFunction& fn_fill_values = find_fill_func(value_type.type, true);
        fn_fill_values(orc_map->elements.get(), values, values_from, values_size, value_type,
                       mapping->get_column_id_or_child_mapping(1).orc_mapping, ctx);
    }
}
static void fill_map_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                      const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* orc_map = down_cast<orc::MapVectorBatch*>(cvb);
    auto* col_nullable = down_cast<NullableColumn*>(col.get());
    auto* col_map = down_cast<MapColumn*>(col_nullable->data_column().get());

    if (!orc_map->hasNulls) {
        fill_map_column(orc_map, col_nullable->data_column(), from, size, type_desc, mapping, ctx);
        col_nullable->null_column()->resize(col_map->size());
        return;
    }
    // else

    const int end = from + size;

    int i = from;
    while (i < end) {
        int j = i;
        // Loop until NULL or end of batch.
        while (j < end && orc_map->notNull[j]) {
            j++;
        }
        if (j > i) {
            fill_map_column(orc_map, col_nullable->data_column(), i, j - i, type_desc, mapping, ctx);
            col_nullable->null_column()->resize(col_map->size());
        }

        if (j == end) {
            break;
        }
        DCHECK(!orc_map->notNull[j]);
        i = j++;
        // Loop until not NULL or end of batch.
        while (j < end && !orc_map->notNull[j]) {
            j++;
        }
        col_nullable->append_nulls(j - i);
        i = j;
    }
}
static void fill_struct_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                               const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* orc_struct = down_cast<orc::StructVectorBatch*>(cvb);
    auto* col_struct = down_cast<StructColumn*>(col.get());

    Columns& field_columns = col_struct->fields_column();

    for (size_t i = 0; i < type_desc.children.size(); i++) {
        const TypeDescriptor& field_type = type_desc.children[i];
        size_t column_id = mapping->get_column_id_or_child_mapping(i).orc_column_id;

        orc::ColumnVectorBatch* field_cvb = orc_struct->fieldsColumnIdMap[column_id];
        const FillColumnFunction& fn_fill_elements = find_fill_func(field_type.type, true);
        fn_fill_elements(field_cvb, field_columns[i], from, size, field_type,
                         mapping->get_column_id_or_child_mapping(i).orc_mapping, ctx);
    }
}
static void fill_struct_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                         const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx) {
    auto* orc_struct = down_cast<orc::StructVectorBatch*>(cvb);
    auto* col_nullable = down_cast<NullableColumn*>(col.get());
    auto* col_struct = down_cast<StructColumn*>(col_nullable->data_column().get());

    if (!orc_struct->hasNulls) {
        fill_struct_column(cvb, col_nullable->data_column(), from, size, type_desc, mapping, ctx);
        col_nullable->null_column()->resize(col_struct->size());
    } else {
        const int end = from + size;
        int i = from;

        while (i < end) {
            int j = i;
            // Loop until NULL or end of batch.
            while (j < end && orc_struct->notNull[j]) {
                j++;
            }
            if (j > i) {
                fill_struct_column(orc_struct, col_nullable->data_column(), i, j - i, type_desc, mapping, ctx);
                col_nullable->null_column()->resize(col_struct->size());
            }

            if (j == end) {
                break;
            }
            DCHECK(!orc_struct->notNull[j]);
            i = j++;
            // Loop until not NULL or end of batch.
            while (j < end && !orc_struct->notNull[j]) {
                j++;
            }
            col_nullable->append_nulls(j - i);
            i = j;
        }
    }
}

FunctionsMap::FunctionsMap() : _funcs(), _nullable_funcs() {
    _funcs[TYPE_BOOLEAN] = &fill_boolean_column;
    _funcs[TYPE_TINYINT] = &fill_int_column<TYPE_TINYINT>;
    _funcs[TYPE_SMALLINT] = &fill_int_column<TYPE_SMALLINT>;
    _funcs[TYPE_INT] = &fill_int_column<TYPE_INT>;
    _funcs[TYPE_BIGINT] = &fill_int_column<TYPE_BIGINT>;
    _funcs[TYPE_LARGEINT] = &fill_int_column<TYPE_LARGEINT>;
    _funcs[TYPE_FLOAT] = &fill_float_column<TYPE_FLOAT>;
    _funcs[TYPE_DOUBLE] = &fill_float_column<TYPE_DOUBLE>;
    _funcs[TYPE_DECIMAL] = &fill_decimal_column;
    _funcs[TYPE_DECIMALV2] = &fill_decimal_column;
    _funcs[TYPE_DECIMAL32] = &fill_decimal32_column;
    _funcs[TYPE_DECIMAL64] = &fill_decimal64_column;
    _funcs[TYPE_DECIMAL128] = &fill_decimal128_column;
    _funcs[TYPE_CHAR] = &fill_string_column;
    _funcs[TYPE_VARCHAR] = &fill_string_column;
    _funcs[TYPE_DATE] = &fill_date_column;
    _funcs[TYPE_DATETIME] = &fill_timestamp_column;
    _funcs[TYPE_ARRAY] = &fill_array_column;
    _funcs[TYPE_MAP] = &fill_map_column;
    _funcs[TYPE_STRUCT] = &fill_struct_column;

    _nullable_funcs[TYPE_BOOLEAN] = &fill_boolean_column_with_null;
    _nullable_funcs[TYPE_TINYINT] = &fill_int_column_with_null<TYPE_TINYINT>;
    _nullable_funcs[TYPE_SMALLINT] = &fill_int_column_with_null<TYPE_SMALLINT>;
    _nullable_funcs[TYPE_INT] = &fill_int_column_with_null<TYPE_INT>;
    _nullable_funcs[TYPE_BIGINT] = &fill_int_column_with_null<TYPE_BIGINT>;
    _nullable_funcs[TYPE_LARGEINT] = &fill_int_column_with_null<TYPE_LARGEINT>;
    _nullable_funcs[TYPE_FLOAT] = &fill_float_column_with_null<TYPE_FLOAT>;
    _nullable_funcs[TYPE_DOUBLE] = &fill_float_column_with_null<TYPE_DOUBLE>;
    _nullable_funcs[TYPE_DECIMAL] = &fill_decimal_column_with_null;
    _nullable_funcs[TYPE_DECIMALV2] = &fill_decimal_column_with_null;
    _nullable_funcs[TYPE_DECIMAL32] = &fill_decimal32_column_with_null;
    _nullable_funcs[TYPE_DECIMAL64] = &fill_decimal64_column_with_null;
    _nullable_funcs[TYPE_DECIMAL128] = &fill_decimal128_column_with_null;
    _nullable_funcs[TYPE_CHAR] = &fill_string_column_with_null;
    _nullable_funcs[TYPE_VARCHAR] = &fill_string_column_with_null;
    _nullable_funcs[TYPE_DATE] = &fill_date_column_with_null;
    _nullable_funcs[TYPE_DATETIME] = &fill_timestamp_column_with_null;
    _nullable_funcs[TYPE_ARRAY] = &fill_array_column_with_null;
    _nullable_funcs[TYPE_MAP] = &fill_map_column_with_null;
    _nullable_funcs[TYPE_STRUCT] = &fill_struct_column_with_null;
};

const FillColumnFunction& find_fill_func(LogicalType type, bool nullable) {
    return nullable ? FunctionsMap::instance()->get_nullable_func(type) : FunctionsMap::instance()->get_func(type);
}

} // namespace starrocks
