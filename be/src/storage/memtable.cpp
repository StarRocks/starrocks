// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/memtable.h"

#include <memory>

#include "column/json_column.h"
#include "column/type_traits.h"
#include "common/logging.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/current_thread.h"
#include "runtime/primitive_type_infra.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/schema.h"
#include "util/orlp/pdqsort.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks::vectorized {

// TODO(cbl): move to common space latter
static const string LOAD_OP_COLUMN = "__op";
static const size_t kPrimaryKeyLimitSize = 128;

MemTable::MemTable(int64_t tablet_id, const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>* slot_descs,
                   RowsetWriter* rowset_writer, MemTracker* mem_tracker, bool is_load)
        : _tablet_id(tablet_id),
          _tablet_schema(tablet_schema),
          _slot_descs(slot_descs),
          _keys_type(tablet_schema->keys_type()),
          _rowset_writer(rowset_writer),
          _aggregator(nullptr),
          _mem_tracker(mem_tracker),
          _is_load(is_load) {
    if (_keys_type == KeysType::PRIMARY_KEYS && _slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
        _vectorized_schema = tablet_schema->schema_with_op();
        _has_op_slot = true;
    } else {
        _vectorized_schema = tablet_schema->schema();
    }

    if (_keys_type != KeysType::DUP_KEYS) {
        // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
        // which is not suitable for obtaining Chunk from ColumnPool,
        // otherwise it will take up a lot of memory and may not be released.
        if (_is_load) {
            _aggregator = std::make_unique<ChunkAggregator>(_vectorized_schema, 0, INT_MAX, 0, false, false, false);
        } else {
            _aggregator = std::make_unique<ChunkAggregator>(_vectorized_schema, 0, INT_MAX, 0);
        }
    }
}

MemTable::MemTable(int64_t tablet_id, Schema* schema, RowsetWriter* rowset_writer, int64_t max_buffer_size,
                   MemTracker* mem_tracker, bool is_load)
        : _tablet_id(tablet_id),
          _vectorized_schema(schema),
          _tablet_schema(nullptr),
          _slot_descs(nullptr),
          _keys_type(schema->keys_type()),
          _rowset_writer(rowset_writer),
          _aggregator(nullptr),
          _use_slot_desc(false),
          _max_buffer_size(max_buffer_size),
          _mem_tracker(mem_tracker),
          _is_load(is_load) {
    if (_keys_type != KeysType::DUP_KEYS) {
        // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
        // which is not suitable for obtaining Chunk from ColumnPool,
        // otherwise it will take up a lot of memory and may not be released.
        _aggregator = std::make_unique<ChunkAggregator>(_vectorized_schema, 0, INT_MAX, 0);
    }
}

MemTable::~MemTable() {
    _aggregator.reset();
    _deletes.reset();
    _chunk.reset();
    _result_chunk.reset();
}

void MemTable::reset_memtable() {
    DCHECK(_chunk->is_empty());
    _result_chunk.reset();
    _deletes.reset();

    if (_keys_type != KeysType::DUP_KEYS) {
        _aggregator->reset();
    }

    _permutations.clear();
    _selective_values.clear();
    _merge_count = 0;
    _chunk_memory_usage = 0;
    _chunk_bytes_usage = 0;
    _aggregator_memory_usage = 0;
    _aggregator_bytes_usage = 0;
}


size_t MemTable::memory_usage() const {
    size_t size = 0;

    // used for sort
    size += sizeof(PermutationItem) * _permutations.size();
    size += sizeof(uint32_t) * _selective_values.size();

    // _result_chunk is the final result before flush
    if (_result_chunk != nullptr && _result_chunk->num_rows() > 0) {
        size += _result_chunk->memory_usage();
    }

    // _aggregator_memory_usage is 0 if keys type is DUP_KEYS
    return size + _chunk_memory_usage + _aggregator_memory_usage;
}

size_t MemTable::write_buffer_size() const {
    if (_chunk == nullptr) {
        return 0;
    }

    // _aggregator_bytes_usage is 0 if keys type is DUP_KEYS
    return _chunk_bytes_usage + _aggregator_bytes_usage;
}

bool MemTable::is_full() const {
    return write_buffer_size() >= _max_buffer_size;
}

bool MemTable::insert(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunk == nullptr) {
        _chunk = ChunkHelper::new_chunk(*_vectorized_schema, 0);
    }

    if (_is_load && _keys_type != KeysType::DUP_KEYS && !_aggregator->has_aggregate_chunk()) {
        _aggregator->clone_empty_chunk(_chunk);
    }

    if (_use_slot_desc) {
        // For schema change, FE will construct a shadow column.
        // The shadow column is not exist in _vectorized_schema
        // So the chunk can only be accessed by the subscript
        // instead of the column name.
        for (int i = 0; i < _slot_descs->size(); ++i) {
            const ColumnPtr& src = chunk.get_column_by_slot_id((*_slot_descs)[i]->id());
            ColumnPtr& dest = _chunk->get_column_by_index(i);
            dest->append_selective(*src, indexes, from, size);
        }
    } else {
        for (int i = 0; i < _vectorized_schema->num_fields(); i++) {
            const ColumnPtr& src = chunk.get_column_by_index(i);
            ColumnPtr& dest = _chunk->get_column_by_index(i);
            dest->append_selective(*src, indexes, from, size);
        }
    }

    if (chunk.has_rows()) {
        _chunk_memory_usage += chunk.memory_usage() * size / chunk.num_rows();
        _chunk_bytes_usage += chunk.bytes_usage() * size / chunk.num_rows();
    }

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    bool suggest_flush = false;
    if (is_full()) {
        size_t orig_bytes = write_buffer_size();
        _merge(false);
        size_t new_bytes = write_buffer_size();
        if (new_bytes > orig_bytes * 2 / 3 && _merge_count <= 1) {
            // this means aggregate doesn't remove enough duplicate rows,
            // keep inserting into the buffer will cause additional sort&merge,
            // the cost of extra sort&merge is greater than extra flush IO,
            // so flush is suggested even buffer is not full
            suggest_flush = true;
        }
    }
    if (is_full()) {
        suggest_flush = true;
    }

    return suggest_flush;
}

Status MemTable::finalize() {
    if (_chunk == nullptr) {
        return Status::OK();
    }

    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);

        if (_keys_type != DUP_KEYS) {
            if (_chunk->num_rows() > 0) {
                // merge last undo merge
                _merge(false);
            }

            if (_merge_count > 1) {
                _chunk = _aggregator->aggregate_result();
                _aggregator->aggregate_reset();

                int64_t t1 = MonotonicMicros();
                _sort(true);
                int64_t t2 = MonotonicMicros();
                _aggregate(true);
                int64_t t3 = MonotonicMicros();
                VLOG(1) << Substitute("memtable final sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
            } else {
                // if there is only one data chunk and merge once,
                // no need to perform an additional merge.
                _chunk.reset();
                _result_chunk.reset();
            }
            _chunk_memory_usage = 0;
            _chunk_bytes_usage = 0;

            _result_chunk = _aggregator->aggregate_result();
            if (_keys_type == PRIMARY_KEYS &&
                PrimaryKeyEncoder::encode_exceed_limit(*_vectorized_schema, *_result_chunk.get(), 0,
                                                       _result_chunk->num_rows(), kPrimaryKeyLimitSize)) {
                _aggregator.reset();
                _aggregator_memory_usage = 0;
                _aggregator_bytes_usage = 0;
                return Status::Cancelled("primary key size exceed the limit.");
            }
            if (_has_op_slot) {
                // TODO(cbl): mem_tracker
                ChunkPtr upserts;
                RETURN_IF_ERROR(_split_upserts_deletes(_result_chunk, &upserts, &_deletes));
                if (_result_chunk != upserts) {
                    _result_chunk = upserts;
                }
            }
            _aggregator.reset();
            _aggregator_memory_usage = 0;
            _aggregator_bytes_usage = 0;
        } else {
            _sort(true);
        }
    }

    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    return Status::OK();
}

Status MemTable::load_finalize(MemTableFlushContext* flush_context) {
    if (_chunk == nullptr) {
        return Status::OK();
    }

    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);

        if (_keys_type != DUP_KEYS) {
            if (_chunk->num_rows() > 0) {
                // merge last undo merge
                _merge(false);
            }

            if (_merge_count > 1) {
                _aggregator->swap_aggregate_result(_chunk);
                _merge(true);
            }
            _chunk_memory_usage = 0;
            _chunk_bytes_usage = 0;

            _aggregator->swap_aggregate_result(_result_chunk);
            if (_keys_type == PRIMARY_KEYS &&
                PrimaryKeyEncoder::encode_exceed_limit(*_vectorized_schema, *_result_chunk.get(), 0,
                                                       _result_chunk->num_rows(), kPrimaryKeyLimitSize)) {
                return Status::Cancelled("primary key size exceed the limit.");
            }
            if (_has_op_slot) {
                // TODO(cbl): mem_tracker
                ChunkPtr upserts;
                RETURN_IF_ERROR(_split_upserts_deletes(_result_chunk, &upserts, &_deletes));
                if (_result_chunk != upserts) {
                    _result_chunk = upserts;
                }
            }
            _aggregator_memory_usage = 0;
            _aggregator_bytes_usage = 0;
        } else {
            _sort(true);
        }
    }

    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);

    flush_context->init(std::move(_deletes), std::move(_result_chunk), _rowset_writer, _mem_tracker);
    return Status::OK();
}

Status MemTable::flush() {
    if (UNLIKELY(_result_chunk == nullptr)) {
        return Status::OK();
    }
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        if (_deletes) {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk_with_deletes(*_result_chunk, *_deletes));
        } else {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk(*_result_chunk));
        }
    }
    StarRocksMetrics::instance()->memtable_flush_total.increment(1);
    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    VLOG(1) << "memtable flush: " << duration_ns / 1000 << "us";
    return Status::OK();
}

void MemTable::_merge(bool is_final) {
    if (_chunk == nullptr || _keys_type == KeysType::DUP_KEYS) {
        return;
    }

    int64_t t1 = MonotonicMicros();
    _sort(is_final);
    int64_t t2 = MonotonicMicros();
    _aggregate(is_final);
    int64_t t3 = MonotonicMicros();
    VLOG(1) << Substitute("memtable sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
    ++_merge_count;
}

void MemTable::_aggregate(bool is_final) {
    if (_result_chunk == nullptr || _result_chunk->num_rows() <= 0) {
        return;
    }

    DCHECK(_result_chunk->num_rows() < INT_MAX);
    DCHECK(_aggregator->source_exhausted());

    _aggregator->update_source(_result_chunk);

    DCHECK(_aggregator->is_do_aggregate());

    _aggregator->aggregate();
    _aggregator_memory_usage = _aggregator->memory_usage();
    _aggregator_bytes_usage = _aggregator->bytes_usage();

    // impossible finish
    DCHECK(!_aggregator->is_finish());
    DCHECK(_aggregator->source_exhausted());

    if (is_final && !_is_load) {
        _result_chunk.reset();
    } else {
        _result_chunk->reset();
    }
}

void MemTable::_sort(bool is_final) {
    SmallPermutation perm = create_small_permutation(_chunk->num_rows());
    std::swap(perm, _permutations);
    _sort_column_inc();
    if (is_final && !_is_load) {
        // No need to reserve, it will be reserve in IColumn::append_selective(),
        // Otherwise it will use more peak memory
        if (_result_chunk == nullptr) {
            _result_chunk = _chunk->clone_empty_with_schema(0);
        }
        _append_to_sorted_chunk(_chunk.get(), _result_chunk.get(), true);
        _chunk.reset();
    } else {
        if (_result_chunk == nullptr) {
            _result_chunk = _chunk->clone_empty_with_schema();
        }
        _append_to_sorted_chunk(_chunk.get(), _result_chunk.get(), false);
        _chunk->reset();
    }
    _chunk_memory_usage = 0;
    _chunk_bytes_usage = 0;
}

void MemTable::_append_to_sorted_chunk(Chunk* src, Chunk* dest, bool is_final) {
    DCHECK_EQ(src->num_rows(), _permutations.size());
    permutate_to_selective(_permutations, &_selective_values);
    if (is_final) {
        dest->rolling_append_selective(*src, _selective_values.data(), 0, src->num_rows());
    } else {
        dest->append_selective(*src, _selective_values.data(), 0, src->num_rows());
    }
}

Status MemTable::_split_upserts_deletes(ChunkPtr& src, ChunkPtr* upserts, std::unique_ptr<Column>* deletes) {
    size_t op_column_id = src->num_columns() - 1;
    auto op_column = src->get_column_by_index(op_column_id);
    src->remove_column_by_index(op_column_id);
    size_t nrows = src->num_rows();
    auto* ops = reinterpret_cast<const uint8_t*>(op_column->raw_data());
    size_t ndel = 0;
    for (size_t i = 0; i < nrows; i++) {
        ndel += (ops[i] == TOpType::DELETE);
    }
    size_t nupsert = nrows - ndel;
    if (ndel == 0) {
        // no deletes, short path
        *upserts = src;
        return Status::OK();
    }
    vector<uint32_t> indexes[2];
    indexes[TOpType::UPSERT].reserve(nupsert);
    indexes[TOpType::DELETE].reserve(ndel);
    for (uint32_t i = 0; i < nrows; i++) {
        // ops == 0: upsert  otherwise: delete
        indexes[ops[i] == TOpType::UPSERT ? TOpType::UPSERT : TOpType::DELETE].push_back(i);
    }
    *upserts = src->clone_empty_with_schema(nupsert);
    (*upserts)->append_selective(*src, indexes[TOpType::UPSERT].data(), 0, nupsert);
    if (!(*deletes)) {
        auto st = PrimaryKeyEncoder::create_column(*_vectorized_schema, deletes);
        if (!st.ok()) {
            LOG(ERROR) << "create column for primary key encoder failed, schema:" << _vectorized_schema
                       << ", status:" << st.to_string();
            return st;
        }
    }
    if (*deletes == nullptr) {
        return Status::RuntimeError("deletes pointer is null");
    } else {
        (*deletes)->reset_column();
    }
    auto& delidx = indexes[TOpType::DELETE];
    PrimaryKeyEncoder::encode_selective(*_vectorized_schema, *src, delidx.data(), delidx.size(), deletes->get());
    return Status::OK();
}

void MemTable::_sort_column_inc() {
    Columns columns;
    std::vector<int> sort_orders;
    std::vector<int> null_firsts;
    for (int i = 0; i < _vectorized_schema->num_key_fields(); i++) {
        columns.push_back(_chunk->get_column_by_index(i));
        // Ascending, null first
        sort_orders.push_back(1);
        null_firsts.push_back(-1);
    }

    Status st = stable_sort_and_tie_columns(false, columns, sort_orders, null_firsts, &_permutations);
    CHECK(st.ok());
}

Status MemTableFlushContext::flush() {
    if (UNLIKELY(_result_chunk == nullptr)) {
        return Status::OK();
    }
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        if (_deletes) {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk_with_deletes(*_result_chunk, *_deletes));
        } else {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk(*_result_chunk));
        }
    }
    StarRocksMetrics::instance()->memtable_flush_total.increment(1);
    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    VLOG(1) << "memtable flush: " << duration_ns / 1000 << "us";
    return Status::OK();
}

size_t MemTableFlushContext::memory_usage() const {
    size_t size = 0;

    // _result_chunk is the final result before flush
    if (_result_chunk != nullptr && _result_chunk->num_rows() > 0) {
        size += _result_chunk->memory_usage();
    }

    if (_deletes != nullptr && _deletes->size() > 0) {
        size += _deletes->memory_usage();
    }

    return size;
}

} // namespace starrocks::vectorized
