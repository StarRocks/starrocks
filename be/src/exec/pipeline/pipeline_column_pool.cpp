
#include "exec/pipeline/pipeline_column_pool.h"

#include "column/chunk.h"
#include "column/column_pool.h"
#include "column/schema.h"
#include "column/type_column_traits.h"
#include "storage/olap_type_infra.h"
#include "storage/tablet_schema.h"
#include "storage/type_utils.h"
#include "storage/types.h"
#include "util/metrics.h"
#include "util/percentile_value.h"

namespace starrocks {
namespace pipeline {
template <typename T, bool force>
T* PipelineColumnPool::get_column() {
    if constexpr (false) {
    }
#define DISPATCH_DIFFERENT_COLUMN(NAME)                       \
    else if constexpr (std::is_same_v<T, vectorized::NAME>) { \
        auto& buffer = NAME##Buffer;                          \
        auto& blocks = buffer.blocks;                         \
        std::lock_guard<std::mutex> l(buffer.mtx);            \
        while (!blocks.empty()) {                             \
            auto block = blocks.back();                       \
            if (block->nfree > 0) {                           \
                T* column = block->ptrs[--block->nfree];      \
                auto bytes = column_bytes(column);            \
                block->bytes -= bytes;                        \
                return column;                                \
            } else {                                          \
                blocks.pop_back();                            \
                free(block);                                  \
            }                                                 \
        }                                                     \
        if constexpr (force) {                                \
            return new (std::nothrow) T();                    \
        } else {                                              \
            return nullptr;                                   \
        }                                                     \
    }
    APPLY_FOR_ALL_SUPPORT_COLUMNS(DISPATCH_DIFFERENT_COLUMN)
#undef DISPATCH_DIFFERENT_COLUMN
}

template <typename T>
void PipelineColumnPool::return_column(T* ptr, size_t chunk_size) {
    using FreeBlock = PipelineColumnPool::ColumnPoolFreeBlock<T, PipelineColumnPool::kBufferSize>;
    ptr->reset_column();
    if constexpr (false) {
    }
#define DISPATCH_DIFFERENT_COLUMN(NAME)                               \
    else if constexpr (std::is_same_v<T, vectorized::NAME>) {         \
        auto& buffer = NAME##Buffer;                                  \
        auto& blocks = buffer.blocks;                                 \
        auto bytes = column_bytes(ptr);                               \
        auto append_block = [&blocks, ptr, bytes](FreeBlock* block) { \
            block->nfree = 1;                                         \
            block->ptrs[0] = ptr;                                     \
            block->bytes = bytes;                                     \
            blocks.push_back(block);                                  \
        };                                                            \
        std::lock_guard<std::mutex> l(buffer.mtx);                    \
        if (blocks.empty()) {                                         \
            auto block = (FreeBlock*)malloc(sizeof(FreeBlock));       \
            if (UNLIKELY(block == nullptr)) {                         \
                delete ptr;                                           \
                return;                                               \
            }                                                         \
            append_block(block);                                      \
        } else {                                                      \
            auto block = blocks.back();                               \
            if (block->nfree < kBufferSize) {                         \
                block->ptrs[block->nfree++] = ptr;                    \
                block->bytes += bytes;                                \
            } else {                                                  \
                auto block = (FreeBlock*)malloc(sizeof(FreeBlock));   \
                if (UNLIKELY(block == nullptr)) {                     \
                    delete ptr;                                       \
                    return;                                           \
                }                                                     \
                append_block(block);                                  \
            }                                                         \
        }                                                             \
    }
    APPLY_FOR_ALL_SUPPORT_COLUMNS(DISPATCH_DIFFERENT_COLUMN)
#undef DISPATCH_DIFFERENT_COLUMN
}

template <typename T, bool force>
std::shared_ptr<T> PipelineColumnPool::get_column_ptr(size_t chunk_size) {
    if constexpr (std::negation_v<HasPipelineColumnPool<T>>) {
        return std::make_shared<T>();
    } else {
        T* ptr = get_column<T, force>();
        if (LIKELY(ptr != nullptr)) {
            return std::shared_ptr<T>(ptr, PipelineColumnPool::PipelineColumnDeleter<T>(this, chunk_size));
        } else {
            return std::make_shared<T>();
        }
    }
}

template <typename T, bool force>
std::shared_ptr<vectorized::DecimalColumnType<T>> PipelineColumnPool::get_decimal_column_ptr(int precision, int scale,
                                                                                             size_t chunk_size) {
    auto column = get_column_ptr<T, force>(chunk_size);
    column->set_precision(precision);
    column->set_scale(scale);
    return column;
}

template <bool force>
PipelineColumnPool::PipelineColumnPtrBuild<force>::PipelineColumnPtrBuild(PipelineColumnPool* column_pool) {
    _column_pool = column_pool;
}

template <bool force>
template <FieldType ftype>
vectorized::ColumnPtr PipelineColumnPool::PipelineColumnPtrBuild<force>::operator()(size_t chunk_size,
                                                                                    const vectorized::Field& field,
                                                                                    int precision, int scale) {
    auto nullable = [this, chunk_size, field, precision, scale](vectorized::ColumnPtr c) -> vectorized::ColumnPtr {
        return field.is_nullable()
                       ? vectorized::NullableColumn::create(
                                 std::move(c), _column_pool->get_column_ptr<vectorized::NullColumn, force>(chunk_size))
                       : c;
    };

    if constexpr (ftype == OLAP_FIELD_TYPE_ARRAY) {
        auto elements = field.sub_field(0).create_column();
        auto offsets = _column_pool->get_column_ptr<vectorized::UInt32Column, force>(chunk_size);
        auto array = vectorized::ArrayColumn::create(std::move(elements), offsets);
        return nullable(array);
    } else {
        switch (ftype) {
        case OLAP_FIELD_TYPE_DECIMAL32:
            return nullable(_column_pool->get_decimal_column_ptr<vectorized::Decimal32Column, force>(precision, scale,
                                                                                                     chunk_size));
        case OLAP_FIELD_TYPE_DECIMAL64:
            return nullable(_column_pool->get_decimal_column_ptr<vectorized::Decimal64Column, force>(precision, scale,
                                                                                                     chunk_size));
        case OLAP_FIELD_TYPE_DECIMAL128:
            return nullable(_column_pool->get_decimal_column_ptr<vectorized::Decimal128Column, force>(precision, scale,
                                                                                                      chunk_size));
        default: {
            return nullable(
                    _column_pool->get_column_ptr<typename vectorized::CppColumnTraits<ftype>::ColumnType, force>(
                            chunk_size));
        }
        }
    }
}

template <bool force>
vectorized::ColumnPtr PipelineColumnPool::column_from_pool(const vectorized::Field& field, size_t chunk_size) {
    auto precision = field.type()->precision();
    auto scale = field.type()->scale();
    return field_type_dispatch_column(field.type()->type(), PipelineColumnPool::PipelineColumnPtrBuild<force>(this),
                                      chunk_size, field, precision, scale);
}

/*
 * new_chunk_pooled, column_from_pool and so on, are like same method in non-pipeline's column pool,
 * except that need a columnpool instance.
 *
 */
vectorized::Chunk* PipelineColumnPool::new_chunk_pooled(const vectorized::Schema& schema, size_t chunk_size,
                                                        bool force) {
    vectorized::Columns columns;
    columns.reserve(schema.num_fields());
    for (size_t i = 0; i < schema.num_fields(); i++) {
        const vectorized::FieldPtr& f = schema.field(i);
        auto column = (force && !config::disable_column_pool) ? column_from_pool<true>(*f, chunk_size)
                                                              : column_from_pool<false>(*f, chunk_size);
        column->reserve(chunk_size);
        columns.emplace_back(std::move(column));
    }
    return new vectorized::Chunk(std::move(columns), std::make_shared<vectorized::Schema>(schema));
}

PipelineColumnPool* PipelineColumnPoolManager::get_or_register(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _pipeline_column_pool_contexts.find(fragment_id);
    if (it != _pipeline_column_pool_contexts.end()) {
        return it->second.get();
    } else {
        auto&& ctx = std::make_unique<PipelineColumnPool>();
        auto* raw_ctx = ctx.get();
        _pipeline_column_pool_contexts.emplace(fragment_id, std::move(ctx));
        return raw_ctx;
    }
}

PipelineColumnPoolPtr PipelineColumnPoolManager::get(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _pipeline_column_pool_contexts.find(fragment_id);
    if (it != _pipeline_column_pool_contexts.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

} // namespace pipeline
} // namespace starrocks
