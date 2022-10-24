// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/memtable.h"

#include <memory>

#include "column/json_column.h"
#include "common/logging.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type_infra.h"
#include "storage/chunk_helper.h"
#include "storage/memtable_sink.h"
#include "storage/primary_key_encoder.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks::vectorized {

// TODO(cbl): move to common space latter
static const string LOAD_OP_COLUMN = "__op";
static const size_t kPrimaryKeyLimitSize = 128;

Schema MemTable::convert_schema(const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>* slot_descs) {
    Schema schema = std::move(ChunkHelper::convert_schema_to_format_v2(*tablet_schema));
    if (tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && slot_descs != nullptr &&
        slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
        // load slots have __op field, so add to _vectorized_schema
        auto op_column = std::make_shared<starrocks::vectorized::Field>((ColumnId)-1, LOAD_OP_COLUMN,
                                                                        FieldType::OLAP_FIELD_TYPE_TINYINT, false);
        op_column->set_aggregate_method(OLAP_FIELD_AGGREGATION_REPLACE);
        schema.append(op_column);
    }
    return schema;
}

void MemTable::_init_aggregator_if_needed() {
    if (_keys_type != KeysType::DUP_KEYS) {
        // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
        // which is not suitable for obtaining Chunk from ColumnPool,
        // otherwise it will take up a lot of memory and may not be released.
        _aggregator = std::make_unique<ChunkAggregator>(_vectorized_schema, 0, INT_MAX, 0);
    }
}

MemTable::MemTable(int64_t tablet_id, const Schema* schema, const std::vector<SlotDescriptor*>* slot_descs,
                   MemTableSink* sink, MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _vectorized_schema(schema),
          _slot_descs(slot_descs),
          _keys_type(schema->keys_type()),
          _sink(sink),
          _aggregator(nullptr),
          _mem_tracker(mem_tracker) {
    if (_keys_type == KeysType::PRIMARY_KEYS && _slot_descs != nullptr &&
        _slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
        _has_op_slot = true;
    }
    _init_aggregator_if_needed();
}

MemTable::MemTable(int64_t tablet_id, const Schema* schema, MemTableSink* sink, int64_t max_buffer_size,
                   MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _vectorized_schema(schema),
          _slot_descs(nullptr),
          _keys_type(schema->keys_type()),
          _sink(sink),
          _aggregator(nullptr),
          _max_buffer_size(max_buffer_size),
          _mem_tracker(mem_tracker) {
    _init_aggregator_if_needed();
}

MemTable::~MemTable() = default;

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

    size_t cur_row_count = _chunk->num_rows();
    if (_slot_descs != nullptr) {
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
        _chunk_bytes_usage += _chunk->bytes_usage(cur_row_count, size);
    }

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    bool suggest_flush = false;
    if (is_full()) {
        size_t orig_bytes = write_buffer_size();
        _merge();
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

        if (_keys_type != KeysType::DUP_KEYS) {
            if (_chunk->num_rows() > 0) {
                // merge last undo merge
                _merge();
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

Status MemTable::flush(SegmentPB* seg_info) {
    if (UNLIKELY(_result_chunk == nullptr)) {
        return Status::OK();
    }
    std::string msg;
    if (_result_chunk->capacity_limit_reached(&msg)) {
        return Status::InternalError(
                fmt::format("memtable of tablet {} reache the capacity limit, detail msg: {}", _tablet_id, msg));
    }
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        if (_deletes) {
            RETURN_IF_ERROR(_sink->flush_chunk_with_deletes(*_result_chunk, *_deletes, seg_info));
        } else {
            RETURN_IF_ERROR(_sink->flush_chunk(*_result_chunk, seg_info));
        }
    }
    StarRocksMetrics::instance()->memtable_flush_total.increment(1);
    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    VLOG(1) << "memtable flush: " << duration_ns / 1000 << "us";
    return Status::OK();
}

void MemTable::_merge() {
    if (_chunk == nullptr || _keys_type == KeysType::DUP_KEYS) {
        return;
    }

    int64_t t1 = MonotonicMicros();
    _sort(false);
    int64_t t2 = MonotonicMicros();
    _aggregate(false);
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

    if (is_final) {
        _result_chunk.reset();
    } else {
        _result_chunk->reset();
    }
}

void MemTable::_sort(bool is_final) {
    SmallPermutation perm = create_small_permutation(static_cast<uint32_t>(_chunk->num_rows()));
    std::swap(perm, _permutations);
    _sort_column_inc();

    if (is_final) {
        // No need to reserve, it will be reserve in IColumn::append_selective(),
        // Otherwise it will use more peak memory
        _result_chunk = _chunk->clone_empty_with_schema(0);
        _append_to_sorted_chunk(_chunk.get(), _result_chunk.get(), true);
        _chunk.reset();
    } else {
        _result_chunk = _chunk->clone_empty_with_schema();
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
            LOG(ERROR) << "create column for primary key encoder failed, schema:" << *_vectorized_schema
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
    int sort_columns = _vectorized_schema->num_key_fields();
    for (int i = 0; i < _vectorized_schema->num_key_fields(); i++) {
        columns.push_back(_chunk->get_column_by_index(i));
    }

    Status st = stable_sort_and_tie_columns(false, columns, SortDescs::asc_null_first(sort_columns), &_permutations);
    CHECK(st.ok());
}

} // namespace starrocks::vectorized
