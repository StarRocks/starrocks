#pragma once

#include <memory>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/field.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/type_list.h"
#include "storage/olap_type_infra.h"
#include "storage/schema.h"

namespace starrocks {
namespace pipeline {
class PipelineColumnPool {
private:
    template <typename T, size_t NITEM>
    struct ColumnPoolFreeBlock {
        int64_t nfree;
        int64_t bytes;
        T* ptrs[NITEM];
    };

    template <typename T, size_t NITEM>
    struct ColumnPoolBuffer {
        std::mutex mtx;
        std::vector<ColumnPoolFreeBlock<T, NITEM>*> blocks;
    };

#define APPLY_FOR_ALL_SUPPORT_COLUMNS(M) \
    M(Int8Column)                        \
    M(UInt8Column)                       \
    M(Int16Column)                       \
    M(Int32Column)                       \
    M(UInt32Column)                      \
    M(Int64Column)                       \
    M(Int128Column)                      \
    M(FloatColumn)                       \
    M(DoubleColumn)                      \
    M(BinaryColumn)                      \
    M(DateColumn)                        \
    M(TimestampColumn)                   \
    M(DecimalColumn)                     \
    M(Decimal32Column)                   \
    M(Decimal64Column)                   \
    M(Decimal128Column)

    static constexpr int kBufferSize = 128;
    /*
     * All column types that support by PipelineColumnPool.
     */
    ColumnPoolBuffer<vectorized::Int8Column, kBufferSize> Int8ColumnBuffer;
    ColumnPoolBuffer<vectorized::UInt8Column, kBufferSize> UInt8ColumnBuffer;
    ColumnPoolBuffer<vectorized::Int16Column, kBufferSize> Int16ColumnBuffer;
    ColumnPoolBuffer<vectorized::Int32Column, kBufferSize> Int32ColumnBuffer;
    ColumnPoolBuffer<vectorized::UInt32Column, kBufferSize> UInt32ColumnBuffer;
    ColumnPoolBuffer<vectorized::Int64Column, kBufferSize> Int64ColumnBuffer;
    ColumnPoolBuffer<vectorized::Int128Column, kBufferSize> Int128ColumnBuffer;
    ColumnPoolBuffer<vectorized::FloatColumn, kBufferSize> FloatColumnBuffer;
    ColumnPoolBuffer<vectorized::DoubleColumn, kBufferSize> DoubleColumnBuffer;
    ColumnPoolBuffer<vectorized::BinaryColumn, kBufferSize> BinaryColumnBuffer;
    ColumnPoolBuffer<vectorized::DateColumn, kBufferSize> DateColumnBuffer;
    ColumnPoolBuffer<vectorized::TimestampColumn, kBufferSize> TimestampColumnBuffer;
    ColumnPoolBuffer<vectorized::DecimalColumn, kBufferSize> DecimalColumnBuffer;
    ColumnPoolBuffer<vectorized::Decimal32Column, kBufferSize> Decimal32ColumnBuffer;
    ColumnPoolBuffer<vectorized::Decimal64Column, kBufferSize> Decimal64ColumnBuffer;
    ColumnPoolBuffer<vectorized::Decimal128Column, kBufferSize> Decimal128ColumnBuffer;

    template <typename T>
    struct PipelineColumnDeleter {
        PipelineColumnDeleter(PipelineColumnPool* pipeline_column_pool, uint32_t chunk_size)
                : pipeline_column_pool(pipeline_column_pool), chunk_size(chunk_size) {}
        void operator()(vectorized::Column* ptr) const {
            pipeline_column_pool->return_column<T>(down_cast<T*>(ptr), chunk_size);
        }
        PipelineColumnPool* pipeline_column_pool;
        uint32_t chunk_size;
    };

    template <typename T, bool force>
    T* get_column();

    template <typename T>
    void return_column(T* ptr, size_t chunk_size);

    template <typename T, bool force>
    std::shared_ptr<T> get_column_ptr(size_t chunk_size);

    template <typename T, bool force>
    std::shared_ptr<vectorized::DecimalColumnType<T>> get_decimal_column_ptr(int precision, int scale,
                                                                             size_t chunk_size);

    template <bool force>
    struct PipelineColumnPtrBuild {
        PipelineColumnPtrBuild(PipelineColumnPool* column_pool);
        template <FieldType ftype>
        vectorized::ColumnPtr operator()(size_t chunk_size, const vectorized::Field& field, int precision, int scale);

        PipelineColumnPool* _column_pool;
    };

    template <bool force>
    vectorized::ColumnPtr column_from_pool(const vectorized::Field& field, size_t chunk_size);

public:
    vectorized::Chunk* new_chunk_pooled(const vectorized::Schema& schema, size_t chunk_size, bool force);

    template <typename T>
    static int64_t column_bytes(const T* col) {
        static_assert(std::is_base_of_v<vectorized::Column, T>, "must_derived_of_Column");
        return col->memory_usage();
    }

    // Returns the number of bytes freed to tcmalloc.
    template <typename T>
    static size_t release_large_column(T* col, size_t limit) {
        auto old_usage = column_bytes(col);
        if (old_usage < limit) {
            return 0;
        }
        if constexpr (std::is_same_v<vectorized::BinaryColumn, T>) {
            auto& bytes = col->get_bytes();
            vectorized::BinaryColumn::Bytes tmp;
            tmp.swap(bytes);
        } else {
            typename T::Container tmp;
            tmp.swap(col->get_data());
        }
        auto new_usage = column_bytes(col);
        DCHECK_LT(new_usage, old_usage);
        return old_usage - new_usage;
    }

    template <typename T>
    void release_large_columns(size_t limit) {
        if constexpr (false) {
        }
#define DISPATCH_DIFFERENT_COLUMN(NAME)                                     \
    else if constexpr (std::is_same_v<T, vectorized::NAME>) {               \
        auto& buffer = NAME##Buffer;                                        \
        auto& blocks = buffer.blocks;                                       \
        std::lock_guard<std::mutex> l(buffer.mtx);                          \
        for (int i = 0; i < blocks.size(); ++i) {                           \
            auto block = blocks[i];                                         \
            size_t freed_bytes = 0;                                         \
            for (int j = 0; j < block->nfree; ++j) {                        \
                freed_bytes += release_large_column(block->ptrs[j], limit); \
            }                                                               \
            if (freed_bytes > 0) {                                          \
                block->bytes -= freed_bytes;                                \
            }                                                               \
        }                                                                   \
    }
        APPLY_FOR_ALL_SUPPORT_COLUMNS(DISPATCH_DIFFERENT_COLUMN)
#undef DISPATCH_DIFFERENT_COLUMN
    }
};

using PipelineColumnPoolPtr = std::shared_ptr<PipelineColumnPool>;

class PipelineColumnPoolManager {
public:
    PipelineColumnPoolManager() = default;
    ~PipelineColumnPoolManager() = default;

    PipelineColumnPoolManager(const PipelineColumnPoolManager&) = delete;
    PipelineColumnPoolManager(PipelineColumnPoolManager&&) = delete;

    PipelineColumnPool* get_or_register(const TUniqueId& fragment_id);
    PipelineColumnPoolPtr get(const TUniqueId& fragment_id);

private:
    std::mutex _lock;
    std::unordered_map<TUniqueId, PipelineColumnPoolPtr> _pipeline_column_pool_contexts;
};

// All column types that support by PipelineColumnPool.
using PipelineColumnPoolList =
        vectorized::TypeList<vectorized::Int8Column, vectorized::UInt8Column, vectorized::Int16Column,
                             vectorized::Int32Column, vectorized::UInt32Column, vectorized::Int64Column,
                             vectorized::Int128Column, vectorized::FloatColumn, vectorized::DoubleColumn,
                             vectorized::BinaryColumn, vectorized::DateColumn, vectorized::TimestampColumn,
                             vectorized::DecimalColumn, vectorized::Decimal32Column, vectorized::Decimal64Column,
                             vectorized::Decimal128Column>;

template <typename T>
struct HasPipelineColumnPool : public std::bool_constant<vectorized::InList<T, PipelineColumnPoolList>::value> {};

} // namespace pipeline
} // namespace starrocks
