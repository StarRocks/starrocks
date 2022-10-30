// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/orc/orc_chunk_reader.h"

#include <glog/logging.h>

#include <exception>
#include <limits>
#include <set>
#include <unordered_map>
#include <utility>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "column/array_column.h"
#include "column/map_column.h"
#include "exprs/vectorized/cast_expr.h"
#include "exprs/vectorized/literal.h"
#include "fs/fs.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/orc_proto.pb.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/primitive_type.h"
#include "simd/simd.h"
#include "util/timezone_utils.h"

namespace starrocks::vectorized {

const FillColumnFunction& find_fill_func(PrimitiveType type, bool nullable);

// NOLINTNEXTLINE
const static std::unordered_map<orc::TypeKind, PrimitiveType> g_orc_starrocks_type_mapping = {
        {orc::BOOLEAN, TYPE_BOOLEAN},
        {orc::BYTE, TYPE_TINYINT},
        {orc::SHORT, TYPE_SMALLINT},
        {orc::INT, TYPE_INT},
        {orc::LONG, TYPE_BIGINT},
        {orc::FLOAT, TYPE_FLOAT},
        {orc::DOUBLE, TYPE_DOUBLE},
        {orc::DECIMAL, TYPE_DECIMALV2},
        {orc::DATE, TYPE_DATE},
        {orc::TIMESTAMP, TYPE_DATETIME},
        {orc::STRING, TYPE_VARCHAR},
        {orc::BINARY, TYPE_VARCHAR},
        {orc::CHAR, TYPE_CHAR},
        {orc::VARCHAR, TYPE_VARCHAR},
        {orc::TIMESTAMP_INSTANT, TYPE_DATETIME},
};

// NOLINTNEXTLINE
const static std::set<orc::TypeKind> g_orc_int_type = {
        orc::BOOLEAN, orc::BYTE, orc::SHORT, orc::INT, orc::LONG, orc::FLOAT, orc::DOUBLE,
};

// NOLINTNEXTLINE
const static std::set<PrimitiveType> g_starrocks_int_type = {TYPE_BOOLEAN, TYPE_TINYINT,  TYPE_SMALLINT, TYPE_INT,
                                                             TYPE_BIGINT,  TYPE_LARGEINT, TYPE_FLOAT,    TYPE_DOUBLE};

const static std::set<orc::TypeKind> g_orc_decimal_type = {orc::DECIMAL};

const static std::set<PrimitiveType> g_starrocks_decimal_type = {TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128,
                                                                 TYPE_DECIMALV2, TYPE_DECIMAL};

const static cctz::time_point<cctz::sys_seconds> CCTZ_UNIX_EPOCH =
        std::chrono::time_point_cast<cctz::sys_seconds>(std::chrono::system_clock::from_time_t(0));

// Hive ORC char type will pad trailing spaces.
// https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_char.html
static inline size_t remove_trailing_spaces(const char* s, size_t size) {
    while (size > 0 && s[size - 1] == ' ') size--;
    return size;
}

static void fill_boolean_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col_start + size);

    auto* values = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(col)->get_data().data();

    auto* cvbd = data->data.data();
    for (int i = col_start; i < col_start + size; ++i, ++from) {
        values[i] = (cvbd[from] != 0);
    }
}

static void fill_boolean_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                          const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);
    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(c->data_column())->get_data().data();

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

template <PrimitiveType Type, typename OrcColumnVectorBatch>
static void fill_int_column_from_cvb(OrcColumnVectorBatch* data, ColumnPtr& col, int from, int size,
                                     const TypeDescriptor& type_desc, void* ctx) {
    OrcChunkReader* reader = static_cast<OrcChunkReader*>(ctx);

    int col_start = col->size();
    col->resize(col_start + size);

    auto* values = ColumnHelper::cast_to_raw<Type>(col)->get_data().data();

    auto* cvbd = data->data.data();

    auto pos = from;
    for (int i = col_start; i < col_start + size; ++i, ++pos) {
        values[i] = cvbd[pos];
    }

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    // don't do overflow check on BIGINT(int64_t) or LARGEINT(int128_t)
    constexpr bool wild_type = (Type == PrimitiveType::TYPE_BIGINT || Type == PrimitiveType::TYPE_LARGEINT);
    if constexpr (!wild_type) {
        if (reader->get_broker_load_mode() && from == 0 && col_start == 0) {
            auto filter = reader->get_broker_load_fiter()->data();
            bool reported = false;
            for (int i = 0; i < size; i++) {
                int64_t value = cvbd[i];
                // overflow.
                if (value < std::numeric_limits<RunTimeCppType<Type>>::lowest() ||
                    value > std::numeric_limits<RunTimeCppType<Type>>::max()) {
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

template <PrimitiveType Type, typename OrcColumnVectorBatch>
static void fill_int_column_with_null_from_cvb(OrcColumnVectorBatch* data, ColumnPtr& col, int from, int size,
                                               const TypeDescriptor& type_desc, void* ctx) {
    OrcChunkReader* reader = static_cast<OrcChunkReader*>(ctx);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = ColumnHelper::cast_to_raw<Type>(c->data_column())->get_data().data();

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
                if (nulls[i] == 0 && (value < std::numeric_limits<RunTimeCppType<Type>>::lowest() ||
                                      value > std::numeric_limits<RunTimeCppType<Type>>::max())) {
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
                if (nulls[i] == 0 && (value < std::numeric_limits<RunTimeCppType<Type>>::lowest() ||
                                      value > std::numeric_limits<RunTimeCppType<Type>>::max())) {
                    nulls[i] = 1;
                }
            }
        }
    }

    c->update_has_null();
}

template <PrimitiveType Type>
static void fill_int_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                            const TypeDescriptor& type_desc, void* ctx) {
    // try to dyn_cast to long vector batch first. in most case, the cast will succeed.
    // so there is no performance loss comparing to call `down_cast` directly
    // because in `down_cast` implementation, there is also a dyn_cast.
    {
        orc::LongVectorBatch* data = dynamic_cast<orc::LongVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_from_cvb<Type, orc::LongVectorBatch>(data, col, from, size, type_desc, ctx);
        }
    }
    // if dyn_cast to long vector batch failed, try to dyn_cast to double vector batch for best effort
    // it only happens when slot type and orc type don't match.
    {
        orc::DoubleVectorBatch* data = dynamic_cast<orc::DoubleVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_from_cvb<Type, orc::DoubleVectorBatch>(data, col, from, size, type_desc, ctx);
        }
    }
    // we have nothing to fill, but have to resize column to save from crash.
    col->resize(col->size() + size);
}

template <PrimitiveType Type>
static void fill_int_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                      const TypeDescriptor& type_desc, void* ctx) {
    {
        orc::LongVectorBatch* data = dynamic_cast<orc::LongVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_with_null_from_cvb<Type, orc::LongVectorBatch>(data, col, from, size, type_desc,
                                                                                  ctx);
        }
    }
    {
        orc::DoubleVectorBatch* data = dynamic_cast<orc::DoubleVectorBatch*>(cvb);
        if (data != nullptr) {
            return fill_int_column_with_null_from_cvb<Type, orc::DoubleVectorBatch>(data, col, from, size, type_desc,
                                                                                    ctx);
        }
    }
    // we have nothing to fill, but have to resize column to save from crash.
    col->resize(col->size() + size);
}

template <PrimitiveType Type>
static void fill_float_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                              const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::DoubleVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col_start + size);

    auto* values = ColumnHelper::cast_to_raw<Type>(col)->get_data().data();

    auto* cvbd = data->data.data();
    for (int i = col_start; i < col_start + size; ++i, ++from) {
        values[i] = cvbd[from];
    }
}

template <PrimitiveType Type>
static void fill_float_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                        const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::DoubleVectorBatch*>(cvb);
    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = ColumnHelper::cast_to_raw<Type>(c->data_column())->get_data().data();

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

static void fill_decimal_column_from_orc_decimal64(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                                   const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::Decimal64VectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    static_assert(sizeof(DecimalV2Value) == sizeof(int128_t));
    auto* values = reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(col.get())->get_data().data());

    auto* cvbd = data->values.data();

    for (int i = col_start; i < col_start + size; ++i, ++from) {
        values[i] = static_cast<int128_t>(cvbd[from]);
    }

    if (DecimalV2Value::SCALE < data->scale) {
        int128_t d = DecimalV2Value::get_scale_base(data->scale - DecimalV2Value::SCALE);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] / d;
        }
    } else if (DecimalV2Value::SCALE > data->scale) {
        int128_t m = DecimalV2Value::get_scale_base(DecimalV2Value::SCALE - data->scale);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] * m;
        }
    }
}

static void fill_decimal_column_with_null_from_orc_decimal64(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from,
                                                             int size, const TypeDescriptor& type_desc, void* ctx) {
    int col_start = col->size();
    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto& null_column = c->null_column();
    auto& data_column = c->data_column();

    fill_decimal_column_from_orc_decimal64(cvb, data_column, from, size, type_desc, ctx);
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

static void fill_decimal_column_from_orc_decimal128(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                                    const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::Decimal128VectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto* values = reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(col.get())->get_data().data());

    for (int i = col_start; i < col_start + size; ++i, ++from) {
        uint64_t hi = data->values[from].getHighBits();
        uint64_t lo = data->values[from].getLowBits();
        values[i] = (((int128_t)hi) << 64) | (int128_t)lo;
    }
    if (DecimalV2Value::SCALE < data->scale) {
        int128_t d = DecimalV2Value::get_scale_base(data->scale - DecimalV2Value::SCALE);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] / d;
        }
    } else if (DecimalV2Value::SCALE > data->scale) {
        int128_t m = DecimalV2Value::get_scale_base(DecimalV2Value::SCALE - data->scale);
        for (int i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] * m;
        }
    }
}

static void fill_decimal_column_with_null_from_orc_decimal128(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from,
                                                              int size, const TypeDescriptor& type_desc, void* ctx) {
    int col_start = col->size();
    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto& null_column = c->null_column();
    auto& data_column = c->data_column();

    fill_decimal_column_from_orc_decimal128(cvb, data_column, from, size, type_desc, ctx);
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

static void fill_decimal_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                const TypeDescriptor& type_desc, void* ctx) {
    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        fill_decimal_column_from_orc_decimal64(cvb, col, from, size, type_desc, ctx);
    } else {
        fill_decimal_column_from_orc_decimal128(cvb, col, from, size, type_desc, ctx);
    }
}

static void fill_decimal_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                          const TypeDescriptor& type_desc, void* ctx) {
    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        fill_decimal_column_with_null_from_orc_decimal64(cvb, col, from, size, type_desc, ctx);
    } else {
        fill_decimal_column_with_null_from_orc_decimal128(cvb, col, from, size, type_desc, ctx);
    }
}

template <typename T>
struct DecimalVectorBatchSelector {
    using Type = std::conditional_t<std::is_same_v<T, int64_t>, orc::Decimal64VectorBatch,
                                    std::conditional_t<std::is_same_v<T, int128_t>, orc::Decimal128VectorBatch, void>>;
};

template <typename T, PrimitiveType DecimalType, bool is_nullable>
static inline void fill_decimal_column_generic(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                               void* ctx) {
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

template <PrimitiveType DecimalType, bool is_nullable>
static inline void fill_decimal_column_from_orc_decimal64_or_decimal128(orc::ColumnVectorBatch* cvb, ColumnPtr& col,
                                                                        int from, int size,
                                                                        const TypeDescriptor& type_desc, void* ctx) {
    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        fill_decimal_column_generic<int64_t, DecimalType, is_nullable>(cvb, col, from, size, ctx);
    } else {
        fill_decimal_column_generic<int128_t, DecimalType, is_nullable>(cvb, col, from, size, ctx);
    }
}

static void fill_decimal32_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                  const TypeDescriptor& type_desc, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL32, false>(cvb, col, from, size, type_desc, ctx);
}

static void fill_decimal64_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                  const TypeDescriptor& type_desc, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL64, false>(cvb, col, from, size, type_desc, ctx);
}

static void fill_decimal128_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                   const TypeDescriptor& type_desc, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL128, false>(cvb, col, from, size, type_desc, ctx);
}

static void fill_decimal32_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                            const TypeDescriptor& type_desc, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL32, true>(cvb, col, from, size, type_desc, ctx);
}

static void fill_decimal64_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                            const TypeDescriptor& type_desc, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL64, true>(cvb, col, from, size, type_desc, ctx);
}

static void fill_decimal128_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                             const TypeDescriptor& type_desc, void* ctx) {
    fill_decimal_column_from_orc_decimal64_or_decimal128<TYPE_DECIMAL128, true>(cvb, col, from, size, type_desc, ctx);
}

static void fill_string_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                               const TypeDescriptor& type_desc, void* ctx) {
    OrcChunkReader* reader = static_cast<OrcChunkReader*>(ctx);

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

static void fill_string_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                         const TypeDescriptor& type_desc, void* ctx) {
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

// orc date value is days since unix epoch time.
// so conversion will be very simple.
static inline void orc_date_to_native_date(DateValue* dv, int64_t value) {
    dv->_julian = value + date::UNIX_EPOCH_JULIAN;
}
static inline void orc_date_to_native_date(JulianDate* jd, int64_t value) {
    *jd = value + date::UNIX_EPOCH_JULIAN;
}
static inline int64_t native_date_to_orc_date(const DateValue& dv) {
    return dv._julian - date::UNIX_EPOCH_JULIAN;
}
static inline int64_t native_date_to_orc_date(const JulianDate& jd) {
    return jd - date::UNIX_EPOCH_JULIAN;
}

static void fill_date_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                             const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto* values = ColumnHelper::cast_to_raw<TYPE_DATE>(col)->get_data().data();
    for (int i = col_start; i < col_start + size; ++i, ++from) {
        orc_date_to_native_date(&(values[i]), data->data[from]);
    }
}

static void fill_date_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                       const TypeDescriptor& type_desc, void* ctx) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);

    int col_start = col->size();
    col->resize(col->size() + size);

    auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto* nulls = c->null_column()->get_data().data();
    auto* values = ColumnHelper::cast_to_raw<TYPE_DATE>(c->data_column())->get_data().data();

    for (int i = col_start; i < col_start + size; ++i, ++from) {
        nulls[i] = cvb->hasNulls && !cvb->notNull[from];
        if (!nulls[i]) {
            orc_date_to_native_date(&(values[i]), data->data[from]);
        }
    }
    c->update_has_null();
}

// orc timestamp is millseconds since unix epoch time.
// timestamp conversion is quite tricky, because it involves timezone info,
// and it affects how we interpret `value`. according to orc v1 spec
// https://orc.apache.org/specification/ORCv1/ writer timezoe  is in stripe footer.

// time conversion involves two aspects:
// 1. timezone (UTC/GMT and local timezone)
// 2. timestamp representation. (StarRocks timestampvalue or ORC seconds/nanoseconds)
// so to simplify handling timestamp conversion, we force to read timestamp from orc file in UTC timezone
// which liborc will do timestamp conversion for us efficiently. and we just handle mismatch of timestamp representation.

// in the following code, seconds has already be adjusted according to timezone.
// Timestamp: {Jualian Date}{microsecond in one day, 0 ~ 86400000000}
// JulianDate use high 22 bits, microsecond use low 40 bits
static inline void orc_ts_to_native_ts_after_unix_epoch(Timestamp* ts, int64_t seconds, int64_t nanoseconds) {
    int64_t days = seconds / SECS_PER_DAY;
    int64_t microseconds = (seconds % SECS_PER_DAY) * 1000000L + nanoseconds / 1000;
    JulianDate jd;
    orc_date_to_native_date(&jd, days);
    *ts = timestamp::from_julian_and_time(jd, microseconds);
}
static inline void orc_ts_to_native_ts_after_unix_epoch(TimestampValue* tv, int64_t seconds, int64_t nanoseconds) {
    return orc_ts_to_native_ts_after_unix_epoch(&tv->_timestamp, seconds, nanoseconds);
}

static inline void orc_ts_to_native_ts_before_unix_epoch(TimestampValue* tv, const cctz::time_zone& tz, int64_t seconds,
                                                         int64_t nanoseconds) {
    cctz::time_point<cctz::sys_seconds> t = CCTZ_UNIX_EPOCH + cctz::seconds(seconds);
    const auto tp = cctz::convert(t, tz);
    tv->from_timestamp(tp.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second(), 0);
}

static inline void orc_ts_to_native_ts(TimestampValue* tv, const cctz::time_zone& tz, int64_t tzoffset, int64_t seconds,
                                       int64_t nanoseconds) {
    if (seconds >= 0) {
        orc_ts_to_native_ts_after_unix_epoch(tv, seconds + tzoffset, nanoseconds);
    } else {
        orc_ts_to_native_ts_before_unix_epoch(tv, tz, seconds, nanoseconds);
    }
}

static void fill_timestamp_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                  const TypeDescriptor& type_desc, void* ctx) {
    OrcChunkReader* reader = static_cast<OrcChunkReader*>(ctx);
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
        orc_ts_to_native_ts(&(values[i]), reader->tzinfo(), reader->tzoffset_in_seconds(), data->data[from], ns);
    }
}

static void fill_timestamp_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                            const TypeDescriptor& type_desc, void* ctx) {
    OrcChunkReader* reader = static_cast<OrcChunkReader*>(ctx);
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
            orc_ts_to_native_ts(&(values[i]), reader->tzinfo(), reader->tzoffset_in_seconds(), data->data[from], ns);
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

static void fill_array_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                              const TypeDescriptor& type_desc, void* ctx) {
    auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
    auto* col_array = down_cast<ArrayColumn*>(col.get());

    UInt32Column* offsets = col_array->offsets_column().get();
    copy_array_offset(orc_list->offsets, from, size + 1, offsets);

    ColumnPtr& elements = col_array->elements_column();
    const TypeDescriptor& child_type = type_desc.children[0];
    const FillColumnFunction& fn_fill_elements = find_fill_func(child_type.type, true);
    const int elements_from = implicit_cast<int>(orc_list->offsets[from]);
    const int elements_size = implicit_cast<int>(orc_list->offsets[from + size] - elements_from);

    fn_fill_elements(orc_list->elements.get(), elements, elements_from, elements_size, child_type, ctx);
}

static void fill_array_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                        const TypeDescriptor& type_desc, void* ctx) {
    auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
    auto* col_nullable = down_cast<NullableColumn*>(col.get());
    auto* col_array = down_cast<ArrayColumn*>(col_nullable->data_column().get());

    if (!orc_list->hasNulls) {
        fill_array_column(orc_list, col_nullable->data_column(), from, size, type_desc, ctx);
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
            fill_array_column(orc_list, col_nullable->data_column(), i, j - i, type_desc, ctx);
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

static void fill_map_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                            const TypeDescriptor& type_desc, void* ctx) {
    auto* orc_map = down_cast<orc::MapVectorBatch*>(cvb);
    auto* col_map = down_cast<MapColumn*>(col.get());

    UInt32Column* offsets = col_map->offsets_column().get();
    copy_array_offset(orc_map->offsets, from, size + 1, offsets);

    ColumnPtr& keys = col_map->keys_column();
    const TypeDescriptor& key_type = type_desc.children[0];
    const FillColumnFunction& fn_fill_keys = find_fill_func(key_type.type, true);
    const int keys_from = implicit_cast<int>(orc_map->offsets[from]);
    const int keys_size = implicit_cast<int>(orc_map->offsets[from + size] - keys_from);

    fn_fill_keys(orc_map->keys.get(), keys, keys_from, keys_size, key_type, ctx);

    ColumnPtr& values = col_map->values_column();
    const TypeDescriptor& value_type = type_desc.children[1];
    const FillColumnFunction& fn_fill_values = find_fill_func(value_type.type, true);

    fn_fill_values(orc_map->elements.get(), values, keys_from, keys_size, value_type, ctx);
}

static void fill_map_column_with_null(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                      const TypeDescriptor& type_desc, void* ctx) {
    auto* orc_map = down_cast<orc::MapVectorBatch*>(cvb);
    auto* col_nullable = down_cast<NullableColumn*>(col.get());
    auto* col_map = down_cast<MapColumn*>(col_nullable->data_column().get());

    if (!orc_map->hasNulls) {
        fill_map_column(orc_map, col_nullable->data_column(), from, size, type_desc, ctx);
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
            fill_map_column(orc_map, col_nullable->data_column(), i, j - i, type_desc, ctx);
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

class FunctionsMap {
public:
    static FunctionsMap* instance() {
        static FunctionsMap map;
        return &map;
    }

    const FillColumnFunction& get_func(PrimitiveType type) const { return _funcs[type]; }

    const FillColumnFunction& get_nullable_func(PrimitiveType type) const { return _nullable_funcs[type]; }

private:
    FunctionsMap() : _funcs(), _nullable_funcs() {
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
    }

    std::array<FillColumnFunction, 64> _funcs;
    std::array<FillColumnFunction, 64> _nullable_funcs;
};

const FillColumnFunction& find_fill_func(PrimitiveType type, bool nullable) {
    return nullable ? FunctionsMap::instance()->get_nullable_func(type) : FunctionsMap::instance()->get_func(type);
}

OrcChunkReader::OrcChunkReader(RuntimeState* state, const std::vector<SlotDescriptor*>& src_slot_descriptors)
        : _src_slot_descriptors(src_slot_descriptors),
          _read_chunk_size(state->chunk_size()),
          _tzinfo(cctz::utc_time_zone()),
          _tzoffset_in_seconds(0),
          _drop_nanoseconds_in_datetime(false),
          _broker_load_mode(true),
          _strict_mode(true),
          _broker_load_filter(nullptr),
          _num_rows_filtered(0),
          _error_message_counter(0),
          _lazy_load_ctx(nullptr) {
    if (_read_chunk_size == 0) {
        _read_chunk_size = 4096;
    }
    _row_reader_options.useWriterTimezone();
    for (SlotDescriptor* slot_desc : _src_slot_descriptors) {
        if (slot_desc == nullptr) continue;
        _slot_id_to_desc[slot_desc->id()] = slot_desc;
    }
}

Status OrcChunkReader::init(std::unique_ptr<orc::InputStream> input_stream) {
    try {
        auto reader = orc::createReader(std::move(input_stream), _reader_options);
        return init(std::move(reader));
    } catch (std::exception& e) {
        auto s = strings::Substitute("OrcChunkReader::init failed. reason = $0, file = $1", e.what(),
                                     _current_file_name);
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }
    return Status::OK();
}

void OrcChunkReader::build_column_name_to_id_mapping(std::unordered_map<std::string, int>* mapping,
                                                     const std::vector<std::string>* hive_column_names,
                                                     const orc::Type& root_type, bool case_sensitive) {
    mapping->clear();
    if (hive_column_names != nullptr) {
        // build hive column names index.
        // if there are 64 columns in hive meta, but actually there are 63 columns in orc file
        // then we will read invalid column id.
        int size = std::min(hive_column_names->size(), root_type.getSubtypeCount());
        for (int i = 0; i < size; i++) {
            const auto& sub_type = root_type.getSubtype(i);
            std::string col_name = format_column_name(hive_column_names->at(i), case_sensitive);
            mapping->insert(make_pair(col_name, static_cast<int>(sub_type->getColumnId())));
        }
    } else {
        // build orc column names index.
        for (int i = 0; i < root_type.getSubtypeCount(); i++) {
            const auto& sub_type = root_type.getSubtype(i);
            std::string col_name = format_column_name(root_type.getFieldName(i), case_sensitive);
            mapping->insert(make_pair(col_name, static_cast<int>(sub_type->getColumnId())));
        }
    }
}

void OrcChunkReader::build_column_name_set(std::unordered_set<std::string>* name_set,
                                           const std::vector<std::string>* hive_column_names,
                                           const orc::Type& root_type, bool case_sensitive) {
    name_set->clear();
    if (hive_column_names != nullptr && hive_column_names->size() > 0) {
        // build hive column names index.
        int size = std::min(hive_column_names->size(), root_type.getSubtypeCount());
        for (int i = 0; i < size; i++) {
            std::string col_name = format_column_name(hive_column_names->at(i), case_sensitive);
            name_set->insert(col_name);
        }
    } else {
        // build orc column names index.
        for (int i = 0; i < root_type.getSubtypeCount(); i++) {
            std::string col_name = format_column_name(root_type.getFieldName(i), case_sensitive);
            name_set->insert(col_name);
        }
    }
}

Status OrcChunkReader::_slot_to_orc_column_name(const SlotDescriptor* desc,
                                                const std::unordered_map<int, std::string>& column_id_to_orc_name,
                                                std::string* orc_column_name) {
    auto col_name = format_column_name(desc->col_name(), _case_sensitive);
    auto it = _name_to_column_id.find(col_name);
    if (it == _name_to_column_id.end()) {
        auto s = strings::Substitute("OrcChunkReader::init_include_columns. col name = $0 not found, file = $1",
                                     desc->col_name(), _current_file_name);
        return Status::NotFound(s);
    }
    auto it2 = column_id_to_orc_name.find(it->second);
    if (it2 == column_id_to_orc_name.end()) {
        auto s = strings::Substitute("OrcChunkReader::init_include_columns. col name = $0 not found, file = $1",
                                     desc->col_name(), _current_file_name);
        return Status::NotFound(s);
    }
    *orc_column_name = it2->second;
    return Status::OK();
}

Status OrcChunkReader::_init_include_columns() {
    build_column_name_to_id_mapping(&_name_to_column_id, _hive_column_names, _reader->getType(), _case_sensitive);
    std::unordered_map<int, std::string> column_id_to_orc_name;
    std::list<std::string> orc_column_names;

    const auto& root_type = _reader->getType();
    for (size_t i = 0; i < root_type.getSubtypeCount(); i++) {
        const auto& sub_type = root_type.getSubtype(i);
        column_id_to_orc_name.emplace(sub_type->getColumnId(), root_type.getFieldName(i));
    }

    for (SlotDescriptor* desc : _src_slot_descriptors) {
        if (desc == nullptr) continue;
        std::string orc_column_name;
        RETURN_IF_ERROR(_slot_to_orc_column_name(desc, column_id_to_orc_name, &orc_column_name));
        orc_column_names.emplace_back(orc_column_name);
    }

    _row_reader_options.include(orc_column_names);

    if (_lazy_load_ctx != nullptr) {
        std::list<std::string> orc_lazy_load_column_names;
        for (SlotDescriptor* desc : _lazy_load_ctx->lazy_load_slots) {
            if (desc == nullptr) continue;
            std::string orc_column_name;
            RETURN_IF_ERROR(_slot_to_orc_column_name(desc, column_id_to_orc_name, &orc_column_name));
            orc_lazy_load_column_names.emplace_back(orc_column_name);
        }
        _row_reader_options.includeLazyLoadColumnNames(orc_lazy_load_column_names);
    }

    return Status::OK();
}

Status OrcChunkReader::init(std::unique_ptr<orc::Reader> reader) {
    _reader = std::move(reader);
    // ORC writes empty schema (struct<>) to ORC files containing zero rows.
    // Hive 0.12
    if (_reader->getNumberOfRows() == 0) {
        return Status::EndOfFile("number of rows is 0");
    }

    // ensure search argument is not null.
    // we are going to put row reader filter into search argument applier
    // and search argument applier only be constructed when search argument is not null.
    if (_row_reader_options.getSearchArgument() == nullptr) {
        std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
        builder->literal(orc::TruthValue::YES_NO_NULL);
        _row_reader_options.searchArgument(builder->build());
    }
    RETURN_IF_ERROR(_init_include_columns());
    try {
        _row_reader = _reader->createRowReader(_row_reader_options);
    } catch (std::exception& e) {
        auto s = strings::Substitute("OrcChunkReader::init failed. reason = $0, file = $1", e.what(),
                                     _current_file_name);
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }
    RETURN_IF_ERROR(_init_position_in_orc());
    RETURN_IF_ERROR(_init_src_types());
    RETURN_IF_ERROR(_init_cast_exprs());
    RETURN_IF_ERROR(_init_fill_functions());
    return Status::OK();
}

Status OrcChunkReader::_init_position_in_orc() {
    int column_size = _src_slot_descriptors.size();
    _position_in_orc.clear();
    _position_in_orc.resize(column_size);
    _slot_id_to_position.clear();

    std::unordered_map<int, int> column_id_to_pos;

    const auto& type = _row_reader->getSelectedType();
    for (int i = 0; i < type.getSubtypeCount(); i++) {
        const auto& sub_type = type.getSubtype(i);
        int col_id = static_cast<int>(sub_type->getColumnId());
        column_id_to_pos[col_id] = i;
    }

    for (int i = 0; i < column_size; i++) {
        auto slot_desc = _src_slot_descriptors[i];

        if (slot_desc == nullptr) continue;
        auto it = _name_to_column_id.find(slot_desc->col_name());
        DCHECK(it != _name_to_column_id.end());
        int col_id = it->second;
        auto it2 = column_id_to_pos.find(col_id);
        if (it2 == column_id_to_pos.end()) {
            auto s = strings::Substitute(
                    "OrcChunkReader::init_position_in_orc. failed to find position. col_id = $0, file = $1",
                    std::to_string(col_id), _current_file_name);
            return Status::NotFound(s);
        }
        int pos = it2->second;
        _position_in_orc[i] = pos;
        SlotId id = slot_desc->id();
        _slot_id_to_position[id] = pos;
    }

    if (_lazy_load_ctx != nullptr) {
        for (int i = 0; i < _lazy_load_ctx->active_load_slots.size(); i++) {
            int src_index = _lazy_load_ctx->active_load_indices[i];
            int pos = _position_in_orc[src_index];
            _lazy_load_ctx->active_load_orc_positions[i] = pos;
        }
        for (int i = 0; i < _lazy_load_ctx->lazy_load_slots.size(); i++) {
            int src_index = _lazy_load_ctx->lazy_load_indices[i];
            int pos = _position_in_orc[src_index];
            _lazy_load_ctx->lazy_load_orc_positions[i] = pos;
        }
    }
    return Status::OK();
}

static Status _orc_type_to_type_descriptor(const orc::Type* orc_type, TypeDescriptor* result) {
    orc::TypeKind kind = orc_type->getKind();
    if (kind == orc::LIST) {
        result->type = TYPE_ARRAY;
        DCHECK_EQ(0, result->children.size());
        result->children.emplace_back();
        TypeDescriptor& element_type = result->children.back();
        RETURN_IF_ERROR(_orc_type_to_type_descriptor(orc_type->getSubtype(0), &element_type));
    } else if (kind == orc::MAP) {
        result->type = TYPE_MAP;
        DCHECK_EQ(0, result->children.size());
        result->children.emplace_back();
        TypeDescriptor& key_type = result->children.back();
        RETURN_IF_ERROR(_orc_type_to_type_descriptor(orc_type->getSubtype(0), &key_type));
        result->children.emplace_back();
        TypeDescriptor& value_type = result->children.back();
        RETURN_IF_ERROR(_orc_type_to_type_descriptor(orc_type->getSubtype(1), &value_type));
    } else {
        auto precision = (int)orc_type->getPrecision();
        auto scale = (int)orc_type->getScale();
        auto len = (int)orc_type->getMaximumLength();
        auto iter = g_orc_starrocks_type_mapping.find(kind);
        if (iter == g_orc_starrocks_type_mapping.end()) {
            return Status::NotSupported("Unsupported ORC type: " + orc_type->toString());
        }
        result->type = iter->second;
        result->len = len;
        result->precision = precision;
        result->scale = scale;
    }
    return Status::OK();
}

static void _try_implicit_cast(TypeDescriptor* from, const TypeDescriptor& to) {
    auto is_integer_type = [](PrimitiveType t) { return g_starrocks_int_type.count(t) > 0; };
    auto is_decimal_type = [](PrimitiveType t) { return g_starrocks_decimal_type.count(t) > 0; };

    PrimitiveType t1 = from->type;
    PrimitiveType t2 = to.type;
    if (t1 == TYPE_ARRAY && t2 == TYPE_ARRAY) {
        _try_implicit_cast(&from->children[0], to.children[0]);
    } else if (is_integer_type(t1) && is_integer_type(t2)) {
        from->type = t2;
    } else if (is_decimal_type(t1) && is_decimal_type(t2)) {
        // if target type is decimal v3 type, the from->type should be assigned to an correct
        // primitive type according to the precision in original orc files so that the invariant
        // 0 <= scale <= precision <= decimal_precision_limit<RuntimeCppType<from_type>> is not
        // violated during creating a DecimalV3Column via ColumnHelper::create(...).
        if (t2 == PrimitiveType::TYPE_DECIMALV2) {
            from->type = t2;
        } else if (from->precision > decimal_precision_limit<int64_t>) {
            from->type = PrimitiveType::TYPE_DECIMAL128;
        } else if (from->precision > decimal_precision_limit<int32_t>) {
            from->type = PrimitiveType::TYPE_DECIMAL64;
        } else {
            from->type = PrimitiveType::TYPE_DECIMAL32;
        }
    } else {
        // nothing to do.
    }
}

Status OrcChunkReader::_init_src_types() {
    int column_size = _src_slot_descriptors.size();
    // update source types.
    _src_types.clear();
    _src_types.resize(column_size);
    for (int i = 0; i < column_size; i++) {
        auto slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        int pos_of_orc = _position_in_orc[i];
        const orc::Type* orc_type = _row_reader->getSelectedType().getSubtype(pos_of_orc);
        RETURN_IF_ERROR(_orc_type_to_type_descriptor(orc_type, &_src_types[i]));
        _try_implicit_cast(&_src_types[i], slot_desc->type());
    }
    return Status::OK();
}

Status OrcChunkReader::_init_cast_exprs() {
    int column_size = _src_slot_descriptors.size();
    _cast_exprs.clear();
    _cast_exprs.resize(column_size);
    _pool.clear();

    for (int column_pos = 0; column_pos < column_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        auto& orc_type = _src_types[column_pos];
        auto& starrocks_type = slot_desc->type();
        Expr* slot = _pool.add(new ColumnRef(slot_desc));
        if (starrocks_type.is_assignable(orc_type)) {
            _cast_exprs[column_pos] = slot;
            continue;
        }
        // we don't support implicit cast column in query external hive table case.
        // if we query external table, we heavily rely on type match to do optimization.
        // For example, if we assume column A is an integer column, but it's stored as string in orc file
        // then min/max of A is almost unusable. Think that there are values ["10", "10000", "100001", "11"]
        // min/max will be "10" and "11", and we expect min/max is 10/100001
        if (!_broker_load_mode && !starrocks_type.is_implicit_castable(orc_type)) {
            return Status::NotSupported(strings::Substitute("Type mismatch: orc $0 to native $1. file = $2",
                                                            orc_type.debug_string(), starrocks_type.debug_string(),
                                                            _current_file_name));
        }
        Expr* cast = VectorizedCastExprFactory::from_type(orc_type, starrocks_type, slot, &_pool);
        if (cast == nullptr) {
            return Status::InternalError(strings::Substitute("Not support cast $0 to $1. file = $2",
                                                             orc_type.debug_string(), starrocks_type.debug_string(),
                                                             _current_file_name));
        }
        _cast_exprs[column_pos] = cast;
    }
    return Status::OK();
}

Status OrcChunkReader::_init_fill_functions() {
    int column_size = _src_slot_descriptors.size();
    _fill_functions.clear();
    _fill_functions.resize(column_size);

    for (int column_pos = 0; column_pos < column_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        PrimitiveType type = _src_types[column_pos].type;
        _fill_functions[column_pos] = find_fill_func(type, slot_desc->is_nullable());
    }
    return Status::OK();
}

OrcChunkReader::~OrcChunkReader() {
    _batch.reset(nullptr);
    _reader.reset(nullptr);
    _row_reader.reset(nullptr);
    _src_types.clear();
    _slot_id_to_desc.clear();
    _slot_id_to_position.clear();
    _position_in_orc.clear();
    _cast_exprs.clear();
    _fill_functions.clear();
}

Status OrcChunkReader::read_next(orc::RowReader::ReadPosition* pos) {
    if (_batch == nullptr) {
        _batch = _row_reader->createRowBatch(_read_chunk_size);
    }
    try {
        if (!_row_reader->next(*_batch, pos)) {
            return Status::EndOfFile("");
        }
    } catch (std::exception& e) {
        auto s = strings::Substitute("OrcChunkReader::read_next failed. reason = $0, file = $1", e.what(),
                                     _current_file_name);
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }
    return Status::OK();
}

size_t OrcChunkReader::get_cvb_size() {
    return _batch->numElements;
}

Status OrcChunkReader::_fill_chunk(ChunkPtr* chunk, const std::vector<SlotDescriptor*>& src_slot_descriptors,
                                   const std::vector<int>* indices) {
    int column_size = src_slot_descriptors.size();
    DCHECK_GT(_batch->numElements, 0);
    const auto& batch_vec = down_cast<orc::StructVectorBatch*>(_batch.get())->fields;
    if (_broker_load_mode) {
        // always allocate load filter. it's much easier to use in fill chunk function.
        if (_broker_load_filter == nullptr) {
            _broker_load_filter = std::make_shared<Column::Filter>(_read_chunk_size);
        }
        _broker_load_filter->assign(_batch->numElements, 1);
    }
    for (int column_pos = 0; column_pos < column_size; ++column_pos) {
        SlotDescriptor* slot_desc = src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        int src_index = column_pos;
        if (indices != nullptr) {
            src_index = (*indices)[src_index];
        }
        set_current_slot(slot_desc);
        orc::ColumnVectorBatch* cvb = batch_vec[_position_in_orc[src_index]];
        if (!slot_desc->is_nullable() && cvb->hasNulls) {
            if (_broker_load_mode) {
                std::string error_msg =
                        strings::Substitute("NULL value in non-nullable column '$0'", _current_slot->col_name());
                report_error_message(error_msg);
                bool all_zero = false;
                ColumnHelper::merge_two_filters(_broker_load_filter.get(),
                                                reinterpret_cast<uint8_t*>(cvb->notNull.data()), &all_zero);
                if (all_zero) {
                    (*chunk)->set_num_rows(0);
                    break;
                }
            } else {
                auto s = strings::Substitute("column '$0' is not nullable", slot_desc->col_name());
                return Status::InternalError(s);
            }
        }
        ColumnPtr& col = (*chunk)->get_column_by_slot_id(slot_desc->id());
        _fill_functions[src_index](cvb, col, 0, _batch->numElements, slot_desc->type(), this);
    }

    if (_broker_load_mode) {
        if ((*chunk)->num_rows() != 0) {
            size_t zero_count = SIMD::count_zero(_broker_load_filter->data(), _broker_load_filter->size());
            if (zero_count != 0) {
                _num_rows_filtered = zero_count;
                (*chunk)->filter(*_broker_load_filter);
            }
        } else {
            _num_rows_filtered = _broker_load_filter->size();
        }
    }

    return Status::OK();
}

ChunkPtr OrcChunkReader::_create_chunk(const std::vector<SlotDescriptor*>& src_slot_descriptors,
                                       const std::vector<int>* indices) {
    auto chunk = std::make_shared<Chunk>();
    int column_size = src_slot_descriptors.size();
    chunk->columns().reserve(column_size);

    for (int column_pos = 0; column_pos < column_size; ++column_pos) {
        auto slot_desc = src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        int src_index = column_pos;
        if (indices != nullptr) {
            src_index = (*indices)[src_index];
        }
        auto col = ColumnHelper::create_column(_src_types[src_index], slot_desc->is_nullable());
        chunk->append_column(std::move(col), slot_desc->id());
    }
    return chunk;
}

ChunkPtr OrcChunkReader::_cast_chunk(ChunkPtr* chunk, const std::vector<SlotDescriptor*>& src_slot_descriptors,
                                     const std::vector<int>* indices) {
    ChunkPtr& src = (*chunk);
    size_t chunk_size = src->num_rows();
    ChunkPtr cast_chunk = std::make_shared<Chunk>();
    int column_size = src_slot_descriptors.size();
    for (int column_pos = 0; column_pos < column_size; ++column_pos) {
        auto slot = src_slot_descriptors[column_pos];
        if (slot == nullptr) {
            continue;
        }
        int src_index = column_pos;
        if (indices != nullptr) {
            src_index = (*indices)[src_index];
        }
        ColumnPtr col = _cast_exprs[src_index]->evaluate(nullptr, src.get());
        col = ColumnHelper::unfold_const_column(slot->type(), chunk_size, col);
        DCHECK_LE(col->size(), chunk_size);
        cast_chunk->append_column(std::move(col), slot->id());
    }
    return cast_chunk;
}

ChunkPtr OrcChunkReader::create_chunk() {
    return _create_chunk(_src_slot_descriptors, nullptr);
}
Status OrcChunkReader::fill_chunk(ChunkPtr* chunk) {
    return _fill_chunk(chunk, _src_slot_descriptors, nullptr);
}

ChunkPtr OrcChunkReader::cast_chunk(ChunkPtr* chunk) {
    return _cast_chunk(chunk, _src_slot_descriptors, nullptr);
}

StatusOr<ChunkPtr> OrcChunkReader::get_chunk() {
    ChunkPtr ptr = create_chunk();
    RETURN_IF_ERROR(fill_chunk(&ptr));
    ChunkPtr ret = cast_chunk(&ptr);
    return ret;
}

StatusOr<ChunkPtr> OrcChunkReader::get_active_chunk() {
    ChunkPtr ptr = _create_chunk(_lazy_load_ctx->active_load_slots, &_lazy_load_ctx->active_load_indices);
    RETURN_IF_ERROR(_fill_chunk(&ptr, _lazy_load_ctx->active_load_slots, &_lazy_load_ctx->active_load_indices));
    ChunkPtr ret = _cast_chunk(&ptr, _lazy_load_ctx->active_load_slots, &_lazy_load_ctx->active_load_indices);
    return ret;
}

void OrcChunkReader::lazy_filter_on_cvb(Filter* filter) {
    size_t true_size = SIMD::count_nonzero(*filter);
    if (filter->size() != true_size) {
        _batch->filterOnFields(filter->data(), filter->size(), true_size, _lazy_load_ctx->lazy_load_orc_positions,
                               true);
    }
}

StatusOr<ChunkPtr> OrcChunkReader::get_lazy_chunk() {
    ChunkPtr ptr = _create_chunk(_lazy_load_ctx->lazy_load_slots, &_lazy_load_ctx->lazy_load_indices);
    RETURN_IF_ERROR(_fill_chunk(&ptr, _lazy_load_ctx->lazy_load_slots, &_lazy_load_ctx->lazy_load_indices));
    ChunkPtr ret = _cast_chunk(&ptr, _lazy_load_ctx->lazy_load_slots, &_lazy_load_ctx->lazy_load_indices);
    return ret;
}

void OrcChunkReader::lazy_read_next(size_t numValues) {
    _row_reader->lazyLoadNext(*_batch, numValues);
}

void OrcChunkReader::lazy_seek_to(size_t rowInStripe) {
    _row_reader->lazyLoadSeekTo(rowInStripe);
}

void OrcChunkReader::set_row_reader_filter(std::shared_ptr<orc::RowReaderFilter> filter) {
    _row_reader_options.rowReaderFilter(std::move(filter));
}

static std::unordered_set<TExprOpcode::type> _supported_binary_ops = {
        TExprOpcode::EQ,          TExprOpcode::NE,        TExprOpcode::LT,
        TExprOpcode::LE,          TExprOpcode::GT,        TExprOpcode::GE,
        TExprOpcode::EQ_FOR_NULL, TExprOpcode::FILTER_IN, TExprOpcode::FILTER_NOT_IN,
};

static std::unordered_set<TExprNodeType::type> _supported_literal_types = {
        TExprNodeType::type::BOOL_LITERAL,   TExprNodeType::type::DATE_LITERAL,      TExprNodeType::type::FLOAT_LITERAL,
        TExprNodeType::type::INT_LITERAL,    TExprNodeType::type::DECIMAL_LITERAL,   TExprNodeType::type::NULL_LITERAL,
        TExprNodeType::type::STRING_LITERAL, TExprNodeType::type::LARGE_INT_LITERAL,
};

static std::unordered_set<TExprNodeType::type> _supported_expr_node_types = {
        // predicates
        TExprNodeType::type::COMPOUND_PRED,
        TExprNodeType::type::BINARY_PRED,
        TExprNodeType::type::IN_PRED,
        TExprNodeType::type::IS_NULL_PRED,
        // literal & slot ref
        TExprNodeType::type::BOOL_LITERAL,
        TExprNodeType::type::DATE_LITERAL,
        TExprNodeType::type::FLOAT_LITERAL,
        TExprNodeType::type::INT_LITERAL,
        TExprNodeType::type::DECIMAL_LITERAL,
        TExprNodeType::type::NULL_LITERAL,
        TExprNodeType::type::SLOT_REF,
        TExprNodeType::type::STRING_LITERAL,
        TExprNodeType::type::LARGE_INT_LITERAL,
};

static std::unordered_map<PrimitiveType, orc::PredicateDataType> _supported_primitive_types = {
        {PrimitiveType::TYPE_BOOLEAN, orc::PredicateDataType::BOOLEAN},
        {PrimitiveType::TYPE_TINYINT, orc::PredicateDataType::LONG},
        {PrimitiveType::TYPE_SMALLINT, orc::PredicateDataType::LONG},
        {PrimitiveType::TYPE_INT, orc::PredicateDataType::LONG},
        {PrimitiveType::TYPE_BIGINT, orc::PredicateDataType::LONG},
        // TYPE_LARGEINT, /* 7 */
        {PrimitiveType::TYPE_FLOAT, orc::PredicateDataType::FLOAT},
        {PrimitiveType::TYPE_DOUBLE, orc::PredicateDataType::FLOAT},
        {PrimitiveType::TYPE_VARCHAR, orc::PredicateDataType::STRING},
        {PrimitiveType::TYPE_DATE, orc::PredicateDataType::DATE},
        //TYPE_DATETIME, /* 12 */
        {PrimitiveType::TYPE_BINARY, orc::PredicateDataType::STRING},
        {PrimitiveType::TYPE_CHAR, orc::PredicateDataType::STRING},
        {PrimitiveType::TYPE_DECIMALV2, orc::PredicateDataType::DECIMAL},
        // TYPE_TIME,       /* 21 */
        {PrimitiveType::TYPE_DECIMAL32, orc::PredicateDataType::DECIMAL},
        {PrimitiveType::TYPE_DECIMAL64, orc::PredicateDataType::DECIMAL},
        {PrimitiveType::TYPE_DECIMAL128, orc::PredicateDataType::DECIMAL},
};

bool OrcChunkReader::_ok_to_add_conjunct(const Expr* conjunct) {
    TExprNodeType::type node_type = conjunct->node_type();
    TExprOpcode::type op_type = conjunct->op();
    if (_supported_expr_node_types.find(node_type) == _supported_expr_node_types.end()) {
        return false;
    }

    // compound pred: and, or not.
    if (node_type == TExprNodeType::COMPOUND_PRED) {
        if (!(op_type == TExprOpcode::COMPOUND_AND || op_type == TExprOpcode::COMPOUND_NOT ||
              op_type == TExprOpcode::COMPOUND_OR)) {
            return false;
        }
        for (Expr* c : conjunct->children()) {
            if (!_ok_to_add_conjunct(c)) {
                return false;
            }
        }
        return true;
    }

    // binary pred: EQ, NE, LT etc.
    if (node_type == TExprNodeType::BINARY_PRED || node_type == TExprNodeType::IN_PRED) {
        if (_supported_binary_ops.find(op_type) == _supported_binary_ops.end()) {
            return false;
        }
    }

    // supported one level. first child is slot, and others are literal values.
    // and only support some of primitive types.
    if (node_type == TExprNodeType::BINARY_PRED || node_type == TExprNodeType::IN_PRED ||
        node_type == TExprNodeType::IS_NULL_PRED) {
        // first child should be slot
        // and others should be literal.
        Expr* c = conjunct->get_child(0);
        if (c->node_type() != TExprNodeType::type::SLOT_REF) {
            return false;
        }
        ColumnRef* ref = down_cast<ColumnRef*>(c);
        SlotId slot_id = ref->slot_id();
        // slot can not be found.
        auto iter = _slot_id_to_desc.find(slot_id);
        if (iter == _slot_id_to_desc.end()) {
            return false;
        }
        SlotDescriptor* slot_desc = iter->second;
        // It's unsafe to do eval on char type because of padding problems.
        if (slot_desc->type().type == TYPE_CHAR) {
            return false;
        }

        if (conjunct->get_num_children() == 1) {
            return false;
        }

        for (int i = 1; i < conjunct->get_num_children(); i++) {
            c = conjunct->get_child(i);
            if (_supported_literal_types.find(c->node_type()) == _supported_literal_types.end()) {
                return false;
            }
        }
        for (int i = 0; i < conjunct->get_num_children(); i++) {
            Expr* c = conjunct->get_child(i);
            PrimitiveType pt = c->type().type;
            if (_supported_primitive_types.find(pt) == _supported_primitive_types.end()) {
                return false;
            }
        }
        return true;
    }

    return true;
}

static inline orc::Int128 to_orc128(int128_t value) {
    return orc::Int128(uint64_t(value >> 64), uint64_t(value));
}

static orc::Literal translate_to_orc_literal(Expr* lit, orc::PredicateDataType pred_type) {
    TExprNodeType::type node_type = lit->node_type();
    PrimitiveType ptype = lit->type().type;
    if (node_type == TExprNodeType::type::NULL_LITERAL) {
        return orc::Literal(pred_type);
    }

    VectorizedLiteral* vlit = down_cast<VectorizedLiteral*>(lit);
    ColumnPtr ptr = vlit->evaluate(nullptr, nullptr);
    if (ptr->only_null()) {
        return orc::Literal(pred_type);
    }

    const Datum& datum = ptr->get(0);
    switch (ptype) {
    case PrimitiveType::TYPE_BOOLEAN:
        return orc::Literal(bool(datum.get_int8()));
    case PrimitiveType::TYPE_TINYINT:
        return orc::Literal(int64_t(datum.get_int8()));
    case PrimitiveType::TYPE_SMALLINT:
        return orc::Literal(int64_t(datum.get_int16()));
    case PrimitiveType::TYPE_INT:
        return orc::Literal(int64_t(datum.get_int32()));
    case PrimitiveType::TYPE_BIGINT:
        return orc::Literal(datum.get_int64());
    case PrimitiveType::TYPE_FLOAT:
        return orc::Literal(double(datum.get_float()));
    case PrimitiveType::TYPE_DOUBLE:
        return orc::Literal(datum.get_double());
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_BINARY: {
        const Slice& slice = datum.get_slice();
        return orc::Literal(slice.data, slice.size);
    }
    case PrimitiveType::TYPE_DATE:
        return orc::Literal(orc::PredicateDataType::DATE, native_date_to_orc_date(datum.get_date()));
    case PrimitiveType::TYPE_DECIMAL:
    case PrimitiveType::TYPE_DECIMALV2: {
        const DecimalV2Value& value = datum.get_decimal();
        return orc::Literal(to_orc128(value.value()), value.PRECISION, value.SCALE);
    }
    case PrimitiveType::TYPE_DECIMAL32:
        return orc::Literal(orc::Int128(datum.get_int32()), lit->type().precision, lit->type().scale);
    case PrimitiveType::TYPE_DECIMAL64:
        return orc::Literal(orc::Int128(datum.get_int64()), lit->type().precision, lit->type().scale);
    case PrimitiveType::TYPE_DECIMAL128:
        return orc::Literal(to_orc128(datum.get_int128()), lit->type().precision, lit->type().scale);
    default:
        CHECK(false) << "failed to handle primitive type = " << std::to_string(ptype);
    }
}

void OrcChunkReader::_add_conjunct(const Expr* conjunct, std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    TExprNodeType::type node_type = conjunct->node_type();
    TExprOpcode::type op_type = conjunct->op();
    if (node_type == TExprNodeType::type::COMPOUND_PRED) {
        if (op_type == TExprOpcode::COMPOUND_AND) {
            builder->startAnd();
        } else if (op_type == TExprOpcode::COMPOUND_OR) {
            builder->startOr();
        } else if (op_type == TExprOpcode::COMPOUND_NOT) {
            builder->startNot();
        } else {
            CHECK(false) << "unexpected op_type in compound_pred type. op_type = " << std::to_string(op_type);
        }
        for (Expr* c : conjunct->children()) {
            _add_conjunct(c, builder);
        }
        builder->end();
        return;
    }

    // handle conjuncts
    // where (NULL) or (slot == $val)
    // where (true) or (slot == $val)
    // If FE can simplify this predicate, then literal processing is no longer needed here
    if (node_type == TExprNodeType::BOOL_LITERAL || node_type == TExprNodeType::NULL_LITERAL) {
        orc::TruthValue val = orc::TruthValue::NO;
        if (node_type == TExprNodeType::BOOL_LITERAL) {
            Expr* literal = const_cast<Expr*>(conjunct);
            ColumnPtr ptr = literal->evaluate(nullptr, nullptr);
            const Datum& datum = ptr->get(0);
            if (datum.get_int8()) {
                val = orc::TruthValue::YES;
            }
        }
        builder->literal(val);
        return;
    }

    Expr* slot = conjunct->get_child(0);
    DCHECK(slot->is_slotref());
    ColumnRef* ref = down_cast<ColumnRef*>(slot);
    SlotId slot_id = ref->slot_id();
    std::string name = _slot_id_to_desc[slot_id]->col_name();
    orc::PredicateDataType pred_type = _supported_primitive_types[slot->type().type];

    if (node_type == TExprNodeType::type::BINARY_PRED) {
        Expr* lit = conjunct->get_child(1);
        orc::Literal literal = translate_to_orc_literal(lit, pred_type);

        switch (op_type) {
        case TExprOpcode::EQ:
            builder->equals(name, pred_type, literal);
            break;

        case TExprOpcode::NE:
            builder->startNot();
            builder->equals(name, pred_type, literal);
            builder->end();
            break;

        case TExprOpcode::LT:
            builder->lessThan(name, pred_type, literal);
            break;

        case TExprOpcode::LE:
            builder->lessThanEquals(name, pred_type, literal);
            break;

        case TExprOpcode::GT:
            builder->startNot();
            builder->lessThanEquals(name, pred_type, literal);
            builder->end();
            break;

        case TExprOpcode::GE:
            builder->startNot();
            builder->lessThan(name, pred_type, literal);
            builder->end();
            break;

        case TExprOpcode::EQ_FOR_NULL:
            builder->nullSafeEquals(name, pred_type, literal);
            break;

        default:
            CHECK(false) << "unexpected op_type in binary_pred type. op_type = " << std::to_string(op_type);
        }
        return;
    }

    if (node_type == TExprNodeType::IN_PRED) {
        bool neg = (op_type == TExprOpcode::FILTER_NOT_IN);
        if (neg) {
            builder->startNot();
        }
        std::vector<orc::Literal> literals;
        for (int i = 1; i < conjunct->get_num_children(); i++) {
            Expr* lit = conjunct->get_child(i);
            orc::Literal literal = translate_to_orc_literal(lit, pred_type);
            literals.emplace_back(literal);
        }
        builder->in(name, pred_type, literals);
        if (neg) {
            builder->end();
        }
        return;
    }

    if (node_type == TExprNodeType::IS_NULL_PRED) {
        builder->isNull(name, pred_type);
        return;
    }

    CHECK(false) << "unexpected node_type = " << std::to_string(node_type);
}

#define ADD_RF_TO_BUILDER                                            \
    {                                                                \
        builder->lessThanEquals(slot->col_name(), pred_type, upper); \
        builder->startNot();                                         \
        builder->lessThan(slot->col_name(), pred_type, lower);       \
        builder->end();                                              \
        return true;                                                 \
    }

#define ADD_RF_BOOLEAN_TYPE(type)                                      \
    case type: {                                                       \
        auto* xrf = dynamic_cast<const RuntimeBloomFilter<type>*>(rf); \
        if (xrf == nullptr) return false;                              \
        auto lower = orc::Literal(bool(xrf->min_value()));             \
        auto upper = orc::Literal(bool(xrf->max_value()));             \
        ADD_RF_TO_BUILDER                                              \
    }

#define ADD_RF_INT_TYPE(type)                                          \
    case type: {                                                       \
        auto* xrf = dynamic_cast<const RuntimeBloomFilter<type>*>(rf); \
        if (xrf == nullptr) return false;                              \
        auto lower = orc::Literal(int64_t(xrf->min_value()));          \
        auto upper = orc::Literal(int64_t(xrf->max_value()));          \
        ADD_RF_TO_BUILDER                                              \
    }

#define ADD_RF_DOUBLE_TYPE(type)                                       \
    case type: {                                                       \
        auto* xrf = dynamic_cast<const RuntimeBloomFilter<type>*>(rf); \
        if (xrf == nullptr) return false;                              \
        auto lower = orc::Literal(double(xrf->min_value()));           \
        auto upper = orc::Literal(double(xrf->max_value()));           \
        ADD_RF_TO_BUILDER                                              \
    }

#define ADD_RF_STRING_TYPE(type)                                                 \
    case type: {                                                                 \
        auto* xrf = dynamic_cast<const RuntimeBloomFilter<type>*>(rf);           \
        if (xrf == nullptr) return false;                                        \
        auto lower = orc::Literal(xrf->min_value().data, xrf->min_value().size); \
        auto upper = orc::Literal(xrf->max_value().data, xrf->max_value().size); \
        ADD_RF_TO_BUILDER                                                        \
    }

#define ADD_RF_DATE_TYPE(type)                                                                              \
    case type: {                                                                                            \
        auto* xrf = dynamic_cast<const RuntimeBloomFilter<type>*>(rf);                                      \
        if (xrf == nullptr) return false;                                                                   \
        auto lower = orc::Literal(orc::PredicateDataType::DATE, native_date_to_orc_date(xrf->min_value())); \
        auto upper = orc::Literal(orc::PredicateDataType::DATE, native_date_to_orc_date(xrf->max_value())); \
        ADD_RF_TO_BUILDER                                                                                   \
    }

#define ADD_RF_DECIMALV2_TYPE(type)                                                                                    \
    case type: {                                                                                                       \
        auto* xrf = dynamic_cast<const RuntimeBloomFilter<type>*>(rf);                                                 \
        if (xrf == nullptr) return false;                                                                              \
        auto lower =                                                                                                   \
                orc::Literal(to_orc128(xrf->min_value().value()), xrf->min_value().PRECISION, xrf->min_value().SCALE); \
        auto upper =                                                                                                   \
                orc::Literal(to_orc128(xrf->max_value().value()), xrf->max_value().PRECISION, xrf->max_value().SCALE); \
        ADD_RF_TO_BUILDER                                                                                              \
    }

#define ADD_RF_DECIMALV3_TYPE(xtype)                                                                          \
    case xtype: {                                                                                             \
        auto* xrf = dynamic_cast<const RuntimeBloomFilter<xtype>*>(rf);                                       \
        if (xrf == nullptr) return false;                                                                     \
        auto lower = orc::Literal(orc::Int128(xrf->min_value()), slot->type().precision, slot->type().scale); \
        auto upper = orc::Literal(orc::Int128(xrf->max_value()), slot->type().precision, slot->type().scale); \
        ADD_RF_TO_BUILDER                                                                                     \
    }

bool OrcChunkReader::_add_runtime_filter(const SlotDescriptor* slot, const JoinRuntimeFilter* rf,
                                         std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    PrimitiveType ptype = slot->type().type;
    auto type_it = _supported_primitive_types.find(ptype);
    if (type_it == _supported_primitive_types.end()) return false;
    orc::PredicateDataType pred_type = type_it->second;
    switch (ptype) {
        ADD_RF_BOOLEAN_TYPE(PrimitiveType::TYPE_BOOLEAN);
        ADD_RF_INT_TYPE(PrimitiveType::TYPE_TINYINT);
        ADD_RF_INT_TYPE(PrimitiveType::TYPE_SMALLINT);
        ADD_RF_INT_TYPE(PrimitiveType::TYPE_INT);
        ADD_RF_INT_TYPE(PrimitiveType::TYPE_BIGINT);
        ADD_RF_DOUBLE_TYPE(PrimitiveType::TYPE_DOUBLE);
        ADD_RF_DOUBLE_TYPE(PrimitiveType::TYPE_FLOAT);
        ADD_RF_STRING_TYPE(PrimitiveType::TYPE_VARCHAR);
        ADD_RF_STRING_TYPE(PrimitiveType::TYPE_CHAR);
        // ADD_RF_STRING_TYPE(PrimitiveType::TYPE_BINARY);
        ADD_RF_DATE_TYPE(PrimitiveType::TYPE_DATE);
        // ADD_RF_DECIMALV2_TYPE(PrimitiveType::TYPE_DECIMAL);
        ADD_RF_DECIMALV2_TYPE(PrimitiveType::TYPE_DECIMALV2);
        ADD_RF_DECIMALV3_TYPE(PrimitiveType::TYPE_DECIMAL32);
        ADD_RF_DECIMALV3_TYPE(PrimitiveType::TYPE_DECIMAL64);
        ADD_RF_DECIMALV3_TYPE(PrimitiveType::TYPE_DECIMAL128);
    default:;
    }
    return false;
}

void OrcChunkReader::set_conjuncts(const std::vector<Expr*>& conjuncts) {
    set_conjuncts_and_runtime_filters(conjuncts, nullptr);
}

void OrcChunkReader::set_conjuncts_and_runtime_filters(const std::vector<Expr*>& conjuncts,
                                                       const RuntimeFilterProbeCollector* rf_collector) {
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
    int ok = 0;
    builder->startAnd();
    for (Expr* expr : conjuncts) {
        bool applied = _ok_to_add_conjunct(expr);
        VLOG_FILE << "OrcChunkReader: add_conjunct: " << expr->debug_string() << ", applied: " << applied;
        if (!applied) {
            continue;
        }
        ok += 1;
        _add_conjunct(expr, builder);
    }

    if (rf_collector != nullptr) {
        for (auto& it : rf_collector->descriptors()) {
            RuntimeFilterProbeDescriptor* rf_desc = it.second;
            const JoinRuntimeFilter* filter = rf_desc->runtime_filter();
            SlotId probe_slot_id;
            if (filter == nullptr || filter->has_null() || !rf_desc->is_probe_slot_ref(&probe_slot_id)) continue;
            auto it2 = _slot_id_to_desc.find(probe_slot_id);
            if (it2 == _slot_id_to_desc.end()) continue;
            SlotDescriptor* slot_desc = it2->second;
            if (_add_runtime_filter(slot_desc, filter, builder)) {
                ok += 1;
            }
        }
    }

    if (ok) {
        builder->end();
        std::unique_ptr<orc::SearchArgument> sargs = builder->build();
        VLOG_FILE << "OrcChunkReader::set_conjuncts. search argument = " << sargs->toString();
        _row_reader_options.searchArgument(std::move(sargs));
    }
}

#define DOWN_CAST_ASSIGN_MIN_MAX(TYPE)                         \
    do {                                                       \
        ColumnHelper::cast_to_raw<TYPE>(min_col)->append(min); \
        ColumnHelper::cast_to_raw<TYPE>(max_col)->append(max); \
        return Status::OK();                                   \
    } while (0)

static Status decode_int_min_max(PrimitiveType ptype, const orc::proto::ColumnStatistics& colStats,
                                 const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_intstatistics() && colStats.intstatistics().has_minimum() &&
        colStats.intstatistics().has_maximum()) {
        const auto& stats = colStats.intstatistics();
        int64_t min = stats.minimum();
        int64_t max = stats.maximum();

        switch (ptype) {
        case PrimitiveType::TYPE_TINYINT:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_TINYINT);
        case PrimitiveType::TYPE_SMALLINT:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_SMALLINT);
        case PrimitiveType::TYPE_INT:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_INT);
        case PrimitiveType::TYPE_BIGINT:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_BIGINT);
        default:
            break;
        }
    }
    return Status::NotFound("int column stats not found");
}

static Status decode_double_min_max(PrimitiveType ptype, const orc::proto::ColumnStatistics& colStats,
                                    const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_doublestatistics() && colStats.doublestatistics().has_minimum() &&
        colStats.doublestatistics().has_maximum()) {
        const auto& stats = colStats.doublestatistics();
        double min = stats.minimum();
        double max = stats.maximum();
        switch (ptype) {
        case PrimitiveType::TYPE_FLOAT:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_FLOAT);
        case PrimitiveType::TYPE_DOUBLE:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_DOUBLE);
        default:
            break;
        }
    }
    return Status::NotFound("double column stats not found");
}
static Status decode_string_min_max(PrimitiveType ptype, const orc::proto::ColumnStatistics& colStats,
                                    const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_stringstatistics() && colStats.stringstatistics().has_minimum() &&
        colStats.stringstatistics().has_maximum()) {
        const auto& stats = colStats.stringstatistics();
        const std::string& min_value = stats.minimum();
        const std::string& max_value = stats.maximum();
        size_t min_value_size = min_value.size();
        size_t max_value_size = max_value.size();
        if (ptype == TYPE_CHAR) {
            min_value_size = remove_trailing_spaces(min_value.c_str(), min_value_size);
            max_value_size = remove_trailing_spaces(max_value.c_str(), max_value_size);
        }
        const Slice min(min_value.c_str(), min_value_size);
        const Slice max(max_value.c_str(), max_value_size);
        switch (ptype) {
        case PrimitiveType::TYPE_VARCHAR:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_VARCHAR);
        case PrimitiveType::TYPE_CHAR:
            DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_CHAR);
        default:
            break;
        }
    }
    return Status::NotFound("string column stats not found");
}

static Status decode_date_min_max(PrimitiveType ptype, const orc::proto::ColumnStatistics& colStats,
                                  const ColumnPtr& min_col, const ColumnPtr& max_col) {
    if (colStats.has_datestatistics() && colStats.datestatistics().has_minimum() &&
        colStats.datestatistics().has_maximum()) {
        const auto& stats = colStats.datestatistics();
        DateValue min, max;
        orc_date_to_native_date(&min, stats.minimum());
        orc_date_to_native_date(&max, stats.maximum());
        DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_DATE);
    }
    return Status::NotFound("date column stats not found");
}

// It's quite odd that, timestamp statistics stores milliseconds since unix epoch time.
// but timestamp column vector batch stores seconds since unix epoch time.
// https://orc.apache.org/specification/ORCv1/
static Status decode_datetime_min_max(PrimitiveType ptype, const orc::proto::ColumnStatistics& colStats,
                                      int64_t tz_offset_in_seconds, const ColumnPtr& min_col,
                                      const ColumnPtr& max_col) {
    if (colStats.has_timestampstatistics() && colStats.timestampstatistics().has_minimumutc() &&
        colStats.timestampstatistics().has_maximumutc()) {
        const auto& stats = colStats.timestampstatistics();
        TimestampValue min, max;
        const cctz::time_zone utc_tzinfo = cctz::utc_time_zone();
        {
            int64_t ms = stats.minimumutc();
            int64_t ns = 0;
            if (stats.has_minimumnanos()) {
                ns = stats.minimumnanos();
            }
            int64_t secs = ms / 1000;
            ns += (ms - secs * 1000) * 1000000L;
            orc_ts_to_native_ts(&min, utc_tzinfo, tz_offset_in_seconds, secs, ns);
        }

        {
            int64_t ms = stats.maximumutc();
            int64_t ns = 0;
            if (stats.has_maximumnanos()) {
                ns = stats.maximumnanos();
            }
            int64_t secs = ms / 1000;
            ns += (ms - secs * 1000) * 1000000L;
            orc_ts_to_native_ts(&max, utc_tzinfo, tz_offset_in_seconds, secs, ns);
        }

        DOWN_CAST_ASSIGN_MIN_MAX(PrimitiveType::TYPE_DATETIME);
    }
    return Status::NotFound("date column stats not found");
}

Status OrcChunkReader::decode_min_max_value(SlotDescriptor* slot, const orc::proto::ColumnStatistics& stats,
                                            ColumnPtr min_col, ColumnPtr max_col, int64_t tz_offset_in_seconds) {
    if (slot->is_nullable()) {
        auto* a = ColumnHelper::as_raw_column<NullableColumn>(min_col);
        auto* b = ColumnHelper::as_raw_column<NullableColumn>(max_col);
        a->mutable_null_column()->append(0);
        b->mutable_null_column()->append(0);
        min_col = a->data_column();
        max_col = b->data_column();
    }
    PrimitiveType ptype = slot->type().type;
    switch (ptype) {
    case PrimitiveType::TYPE_TINYINT:
    case PrimitiveType::TYPE_SMALLINT:
    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_BIGINT:
        // case PrimitiveType::TYPE_LARGEINT:
        return decode_int_min_max(ptype, stats, min_col, max_col);

    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_DOUBLE:
        return decode_double_min_max(ptype, stats, min_col, max_col);

    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_CHAR:
        return decode_string_min_max(ptype, stats, min_col, max_col);

    case PrimitiveType::TYPE_DATE:
        return decode_date_min_max(ptype, stats, min_col, max_col);

    case PrimitiveType::TYPE_DATETIME:
        return decode_datetime_min_max(ptype, stats, tz_offset_in_seconds, min_col, max_col);

    default:
        return Status::NotSupported("Not support to decode min/max from orc column stats. type = " +
                                    std::to_string(ptype));
    }
}

Status OrcChunkReader::apply_dict_filter_eval_cache(const std::unordered_map<SlotId, FilterPtr>& dict_filter_eval_cache,
                                                    Filter* filter) {
    if (dict_filter_eval_cache.size() == 0) {
        return Status::OK();
    }

    const uint32_t size = _batch->numElements;
    filter->assign(size, 1);
    const auto& batch_vec = down_cast<orc::StructVectorBatch*>(_batch.get())->fields;
    bool filter_all = false;

    for (const auto& it : dict_filter_eval_cache) {
        int pos = _slot_id_to_position[it.first];
        const Filter& dict_filter = (*it.second);
        ColumnPtr data_filter = BooleanColumn::create(size);
        Filter& data = static_cast<BooleanColumn*>(data_filter.get())->get_data();
        DCHECK(data.size() == size);

        orc::StringVectorBatch* batch = down_cast<orc::StringVectorBatch*>(batch_vec[pos]);
        for (uint32_t i = 0; i < size; i++) {
            int64_t code = batch->codes[i];
            DCHECK(code < dict_filter.size());
            data[i] = dict_filter[code];
        }

        bool all_zero = false;
        ColumnHelper::merge_two_filters(data_filter, filter, &all_zero);
        if (all_zero) {
            filter_all = true;
            break;
        }
    }

    if (!filter_all) {
        uint32_t one_count = filter->size() - SIMD::count_zero(*filter);
        if (one_count != filter->size()) {
            if (has_lazy_load_context()) {
                _batch->filterOnFields(filter->data(), filter->size(), one_count,
                                       _lazy_load_ctx->active_load_orc_positions, false);
            } else {
                _batch->filter(filter->data(), filter->size(), one_count);
            }
        }
    } else {
        _batch->numElements = 0;
    }
    return Status::OK();
}

Status OrcChunkReader::set_timezone(const std::string& tz) {
    if (!TimezoneUtils::find_cctz_time_zone(tz, _tzinfo)) {
        return Status::InternalError(strings::Substitute("can not find cctz time zone $0", tz));
    }
    _tzoffset_in_seconds = TimezoneUtils::to_utc_offset(_tzinfo);
    return Status::OK();
}

static const int MAX_ERROR_MESSAGE_COUNTER = 100;
void OrcChunkReader::report_error_message(const std::string& error_msg) {
    if (_state == nullptr) return;
    if (_error_message_counter > MAX_ERROR_MESSAGE_COUNTER) return;
    _error_message_counter += 1;
    _state->append_error_msg_to_file("", error_msg);
}

int OrcChunkReader::get_column_id_by_name(const std::string& name) const {
    const auto& it = _name_to_column_id.find(name);
    if (it != _name_to_column_id.end()) {
        return it->second;
    }
    return -1;
}

// ======================================================================================

ORCHdfsFileStream::ORCHdfsFileStream(RandomAccessFile* file, uint64_t length)
        : _file(file), _length(length), _cache_buffer(0), _cache_offset(0), _buffer_stream(_file) {
    SharedBufferedInputStream::CoalesceOptions options = {.max_dist_size = config::io_coalesce_read_max_distance_size,
                                                          .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    _buffer_stream.set_coalesce_options(options);
}

void ORCHdfsFileStream::prepareCache(orc::InputStream::PrepareCacheScope scope, uint64_t offset, uint64_t length) {
    const size_t cache_max_size = config::orc_file_cache_max_size;
    if (length > cache_max_size) return;
    if (canUseCacheBuffer(offset, length)) return;

    // If this stripe is small, probably other stripes are also small
    // we combine those reads into one, and try to read several stripes in one shot.
    if (scope == orc::InputStream::PrepareCacheScope::READ_FULL_STRIPE) {
        length = std::min(_length - offset, cache_max_size);
    }

    _cache_buffer.resize(length);
    _cache_offset = offset;
    doRead(_cache_buffer.data(), length, offset, true);
}

bool ORCHdfsFileStream::canUseCacheBuffer(uint64_t offset, uint64_t length) {
    if ((_cache_buffer.size() != 0) && (offset >= _cache_offset) &&
        ((offset + length) <= (_cache_offset + _cache_buffer.size()))) {
        return true;
    }
    return false;
}

void ORCHdfsFileStream::read(void* buf, uint64_t length, uint64_t offset) {
    if (canUseCacheBuffer(offset, length)) {
        size_t idx = offset - _cache_offset;
        memcpy(buf, _cache_buffer.data() + idx, length);
    } else {
        doRead(buf, length, offset, false);
    }
}

const std::string& ORCHdfsFileStream::getName() const {
    return _file->filename();
}

void ORCHdfsFileStream::doRead(void* buf, uint64_t length, uint64_t offset, bool direct) {
    if (buf == nullptr) {
        throw orc::ParseError("Buffer is null");
    }
    Status status;
    if (direct || !_buffer_stream_enabled) {
        status = _file->read_at_fully(offset, buf, length);
    } else {
        const uint8_t* ptr = nullptr;
        size_t nbytes = length;
        status = _buffer_stream.get_bytes(&ptr, offset, &nbytes, false);
        DCHECK_EQ(nbytes, length);
        if (status.ok()) {
            ::memcpy(buf, ptr, length);
        }
    }
    if (!status.ok()) {
        auto msg = strings::Substitute("Failed to read $0: $1", _file->filename(), status.to_string());
        throw orc::ParseError(msg);
    }
}

void ORCHdfsFileStream::clearIORanges() {
    _buffer_stream_enabled = false;
    _buffer_stream.release();
}

void ORCHdfsFileStream::setIORanges(std::vector<orc::InputStream::IORange>& io_ranges) {
    _buffer_stream_enabled = true;
    std::vector<SharedBufferedInputStream::IORange> bs_io_ranges;
    bs_io_ranges.reserve(io_ranges.size());
    for (const auto& r : io_ranges) {
        bs_io_ranges.emplace_back(SharedBufferedInputStream::IORange{.offset = static_cast<int64_t>(r.offset),
                                                                     .size = static_cast<int64_t>(r.size)});
    }
    Status st = _buffer_stream.set_io_ranges(bs_io_ranges);
    if (!st.ok()) {
        auto msg = strings::Substitute("Failed to setIORanges $0: $1", _file->filename(), st.to_string());
        throw orc::ParseError(msg);
    }
}

} // namespace starrocks::vectorized
