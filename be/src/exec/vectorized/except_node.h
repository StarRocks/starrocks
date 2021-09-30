// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <unordered_set>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exec/olap_common.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks {
class DescriptorTbl;
class SlotDescriptor;
class TupleDescriptor;
} // namespace starrocks

namespace starrocks::vectorized {
class ExceptNode : public ExecNode {
    class SliceFlag {
    public:
        SliceFlag(const uint8_t* d, size_t n) : slice(d, n), deleted(false) {}

        Slice slice;
        mutable bool deleted;
    };

    struct SliceFlagEqual {
        bool operator()(const SliceFlag& x, const SliceFlag& y) const {
            return memequal(x.slice.data, x.slice.size, y.slice.data, y.slice.size);
        }
    };

    struct SliceFlagHash {
        static const uint32_t CRC_SEED = 0x811C9DC5;
        std::size_t operator()(const SliceFlag& sliceMayUnneed) const {
            const Slice& slice = sliceMayUnneed.slice;
            return crc_hash_64(slice.data, slice.size, CRC_SEED);
        }
    };

    template <typename HashSet>
    struct HashSetFromExprs {
        using Iterator = typename HashSet::iterator;
        using ResultVector = typename std::vector<Slice>;
        std::unique_ptr<HashSet> hash_set;

        HashSetFromExprs()
                : hash_set(std::make_unique<HashSet>()),
                  _tracker(std::make_unique<MemTracker>()),
                  _mem_pool(std::make_unique<MemPool>(_tracker.get())),
                  _buffer(_mem_pool->allocate(_max_one_row_size * config::vector_chunk_size)) {}

        Iterator begin() { return hash_set->begin(); }

        Iterator end() { return hash_set->end(); }

        void serialize_columns(const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs, size_t chunk_size,
                               const std::function<void(const ColumnPtr&, int)>& get_type) {
            const bool null = false;
            for (size_t i = 0; i < exprs.size(); i++) {
                ColumnPtr key_column = exprs[i]->evaluate(chunkPtr.get());
                get_type(key_column, i);
                if (key_column->is_nullable()) {
                    key_column->serialize_batch(_buffer, _slice_sizes, chunk_size, _max_one_row_size);
                } else {
                    for (size_t j = 0; j < chunk_size; ++j) {
                        memcpy(_buffer + j * _max_one_row_size + _slice_sizes[j], &null, sizeof(bool));
                        _slice_sizes[j] += sizeof(bool);
                        _slice_sizes[j] += key_column->serialize(j, _buffer + j * _max_one_row_size + _slice_sizes[j]);
                    }
                }
            }
        }

        Status build_set(RuntimeState* state, ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs, MemPool* pool,
                         const std::function<void(const ColumnPtr&, int)>& get_type) {
            size_t chunk_size = chunkPtr->num_rows();
            _slice_sizes.assign(config::vector_chunk_size, 0);
            size_t cur_max_one_row_size = get_max_serialize_size(chunkPtr, exprs);
            if (UNLIKELY(cur_max_one_row_size > _max_one_row_size)) {
                _max_one_row_size = cur_max_one_row_size;
                _mem_pool->clear();
                _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
                if (UNLIKELY(_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
            }

            serialize_columns(chunkPtr, exprs, chunk_size, get_type);

            for (size_t i = 0; i < chunk_size; ++i) {
                SliceFlag key(_buffer + i * _max_one_row_size, _slice_sizes[i]);
                hash_set->lazy_emplace(key, [&](const auto& ctor) {
                    uint8_t* pos = pool->allocate(key.slice.size);
                    memcpy(pos, key.slice.data, key.slice.size);
                    ctor(pos, key.slice.size);
                });
            }
            RETURN_IF_LIMIT_EXCEEDED(state, "Except, while build hash table.");
            return Status::OK();
        }

        Status erase_duplicate_row(RuntimeState* state, size_t chunk_size, ChunkPtr& chunkPtr,
                                   const std::vector<ExprContext*>& exprs) {
            _slice_sizes.assign(config::vector_chunk_size, 0);
            size_t cur_max_one_row_size = get_max_serialize_size(chunkPtr, exprs);
            if (UNLIKELY(cur_max_one_row_size > _max_one_row_size)) {
                _max_one_row_size = cur_max_one_row_size;
                _mem_pool->clear();
                _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
                if (UNLIKELY(_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
                RETURN_IF_LIMIT_EXCEEDED(state, "Except, while probe hash table.");
            }

            serialize_columns(chunkPtr, exprs, chunk_size, [](const ColumnPtr& column, int i) -> void {});

            for (size_t i = 0; i < chunk_size; ++i) {
                SliceFlag key(_buffer + i * _max_one_row_size, _slice_sizes[i]);
                auto iter = hash_set->find(key);
                if (iter != hash_set->end()) {
                    iter->deleted = true;
                }
            }
            return Status::OK();
        }

        size_t get_max_serialize_size(const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs) {
            size_t max_size = 0;
            for (auto expr : exprs) {
                ColumnPtr key_column = expr->evaluate(chunkPtr.get());
                max_size += key_column->max_one_element_serialize_size();
                if (!key_column->is_nullable()) {
                    max_size += sizeof(bool);
                }
            }
            return max_size;
        }

        void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, int32_t batch_size) {
            for (auto& key_column : key_columns) {
                DCHECK(!key_column->is_constant());
                if (!key_column->is_nullable()) {
                    for (auto& key : keys) {
                        key.data += sizeof(bool);
                    }
                }

                key_column->deserialize_and_append_batch(keys, batch_size);
            }
        }

        Buffer<uint32_t> _slice_sizes;
        size_t _max_one_row_size = 8;
        std::unique_ptr<MemTracker> _tracker;
        std::unique_ptr<MemPool> _mem_pool;
        uint8_t* _buffer;
        ResultVector _results;
    };

public:
    ExceptNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status get_next(RuntimeState* state, ChunkPtr* row_batch, bool* eos) override;
    Status close(RuntimeState* state) override;

private:
    /// Tuple id resolved in Prepare() to set tuple_desc_;
    const int _tuple_id;
    /// Descriptor for tuples this union node constructs.
    const TupleDescriptor* _tuple_desc;
    // Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<ExprContext*>> _child_expr_lists;

    struct ExceptColumnTypes {
        TypeDescriptor result_type;
        bool is_nullable;
        bool is_constant;
    };
    std::vector<ExceptColumnTypes> _types;

    using HashSerializeSet = HashSetFromExprs<phmap::flat_hash_set<SliceFlag, SliceFlagHash, SliceFlagEqual>>;
    std::unique_ptr<HashSerializeSet> _hash_set;
    HashSerializeSet::Iterator _hash_set_iterator;

    // pool for allocate key.
    std::unique_ptr<MemPool> _build_pool;

    RuntimeProfile::Counter* _build_set_timer = nullptr; // time to build hash set
    RuntimeProfile::Counter* _erase_duplicate_row_timer = nullptr;
    RuntimeProfile::Counter* _get_result_timer = nullptr;
};

} // namespace starrocks::vectorized
