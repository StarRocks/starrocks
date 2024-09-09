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

#include "storage/lake/delta_writer.h"

#include <bthread/bthread.h>
#include <fmt/format.h>

#include <memory>
#include <utility>

#include "column/chunk.h"
#include "column/column.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/pk_tablet_writer.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/memtable.h"
#include "storage/memtable_flush_executor.h"
#include "storage/memtable_sink.h"
#include "storage/primary_key_encoder.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

using TxnLogPtr = DeltaWriter::TxnLogPtr;

class TabletWriterSink : public MemTableSink {
public:
    explicit TabletWriterSink(TabletWriter* w) : _writer(w) {}

    ~TabletWriterSink() override = default;

    DISALLOW_COPY_AND_MOVE(TabletWriterSink);

    Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment = nullptr) override {
        RETURN_IF_ERROR(_writer->write(chunk, segment));
        return _writer->flush(segment);
    }

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                    starrocks::SegmentPB* segment = nullptr) override {
        RETURN_IF_ERROR(_writer->flush_del_file(deletes));
        RETURN_IF_ERROR(_writer->write(upserts, segment));
        return _writer->flush(segment);
    }

private:
    TabletWriter* _writer;
};

/// DeltaWriterImpl

class DeltaWriterImpl {
public:
    explicit DeltaWriterImpl(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                             const std::vector<SlotDescriptor*>* slots, std::string merge_condition,
                             bool miss_auto_increment_column, int64_t table_id, int64_t immutable_tablet_size,
                             MemTracker* mem_tracker, int64_t max_buffer_size, int64_t schema_id,
                             const PartialUpdateMode& partial_update_mode,
                             const std::map<string, string>* column_to_expr_value)
            : _tablet_manager(tablet_manager),
              _tablet_id(tablet_id),
              _txn_id(txn_id),
              _table_id(table_id),
              _partition_id(partition_id),
              _schema_id(schema_id),
              _mem_tracker(mem_tracker),
              _slots(slots),
              _max_buffer_size(max_buffer_size > 0 ? max_buffer_size : config::write_buffer_size),
              _immutable_tablet_size(immutable_tablet_size),
              _merge_condition(std::move(merge_condition)),
              _miss_auto_increment_column(miss_auto_increment_column),
              _partial_update_mode(partial_update_mode),
              _column_to_expr_value(column_to_expr_value) {}

    ~DeltaWriterImpl() = default;

    DISALLOW_COPY_AND_MOVE(DeltaWriterImpl);

    Status open();

    Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size);

    StatusOr<TxnLogPtr> finish_with_txnlog(DeltaWriterFinishMode mode);

    Status finish();

    void close();

    [[nodiscard]] int64_t partition_id() const { return _partition_id; }

    [[nodiscard]] int64_t tablet_id() const { return _tablet_id; }

    [[nodiscard]] int64_t txn_id() const { return _txn_id; }

    [[nodiscard]] MemTracker* mem_tracker() { return _mem_tracker; }

    Status flush();

    Status flush_async();

    int64_t queueing_memtable_num() const;

    std::vector<FileInfo> files() const;

    int64_t data_size() const;

    int64_t num_rows() const;

    bool is_immutable() const;

    Status check_immutable();

    int64_t last_write_ts() const;

private:
    Status reset_memtable();

    Status fill_auto_increment_id(const Chunk& chunk);

    Status init_tablet_schema();

    Status init_write_schema();

    Status build_schema_and_writer();

    Status check_partial_update_with_sort_key(const Chunk& chunk);

    bool is_partial_update();

    TabletManager* _tablet_manager;
    const int64_t _tablet_id;
    const int64_t _txn_id;
    const int64_t _table_id;
    const int64_t _partition_id;
    const int64_t _schema_id;
    MemTracker* const _mem_tracker;

    const std::vector<SlotDescriptor*>* const _slots;

    int64_t _max_buffer_size;

    std::unique_ptr<TabletWriter> _tablet_writer;
    std::unique_ptr<MemTable> _mem_table;
    std::unique_ptr<MemTableSink> _mem_table_sink;
    std::unique_ptr<FlushToken> _flush_token;

    // The full list of columns defined
    std::shared_ptr<const TabletSchema> _tablet_schema;

    // The list of columns to write/update
    // Invariant: _write_schema->num_columns() <= _tablet_schema->num_columns()
    // _write_schema->num_columns() < _tablet_schema->num_columns() means this is a partial update
    std::shared_ptr<const TabletSchema> _write_schema;

    // Subscripts in _tablet_schema for each column in _write_schema
    // Would be empty if the _write_schema is the same as _tablet_schema, otherwise
    // _write_column_ids.size() == _write_schema->num_columns()
    std::vector<int32_t> _write_column_ids;

    // Converted from |_write_schema|.
    // Invariant: _write_schema_for_mem_table.num_fields() >= _write_schema->num_columns()
    // Compared with _write_schema, may contain an extra "op" column, see `MemTable::convert_schema()` for reference.
    Schema _write_schema_for_mem_table;

    // for automatic bucket
    int64_t _immutable_tablet_size = 0;
    std::atomic<bool> _is_immutable = false;

    // for condition update
    std::string _merge_condition;

    // for auto increment
    // true if miss AUTO_INCREMENT column in partial update mode
    bool _miss_auto_increment_column;

    PartialUpdateMode _partial_update_mode;
    bool _partial_schema_with_sort_key_conflict = false;

    int64_t _last_write_ts = 0;

    const std::map<string, string>* _column_to_expr_value = nullptr;
};

bool DeltaWriterImpl::is_immutable() const {
    return _is_immutable.load(std::memory_order_relaxed);
}

Status DeltaWriterImpl::check_immutable() {
    if (_immutable_tablet_size > 0 && !_is_immutable.load(std::memory_order_relaxed)) {
        if (_tablet_manager->in_writing_data_size(_tablet_id) > _immutable_tablet_size) {
            _is_immutable.store(true, std::memory_order_relaxed);
        }
        VLOG(1) << "check delta writer, tablet=" << _tablet_id << ", txn=" << _txn_id
                << ", immutable_tablet_size=" << _immutable_tablet_size
                << ", data_size=" << _tablet_manager->in_writing_data_size(_tablet_id)
                << ", is_immutable=" << _is_immutable.load(std::memory_order_relaxed);
    }
    return Status::OK();
}

int64_t DeltaWriterImpl::last_write_ts() const {
    return _last_write_ts;
}

Status DeltaWriterImpl::build_schema_and_writer() {
    if (_mem_table_sink == nullptr) {
        DCHECK(_tablet_writer == nullptr);
        ASSIGN_OR_RETURN([[maybe_unused]] auto tablet, _tablet_manager->get_tablet(_tablet_id));
        RETURN_IF_ERROR(init_tablet_schema());
        RETURN_IF_ERROR(init_write_schema());
        if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
            _tablet_writer = std::make_unique<HorizontalPkTabletWriter>(_tablet_manager, _tablet_id, _write_schema,
                                                                        _txn_id, nullptr, false /** no compaction**/);
        } else {
            _tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(_tablet_manager, _tablet_id, _write_schema,
                                                                             _txn_id, false);
        }
        RETURN_IF_ERROR(_tablet_writer->open());
        _mem_table_sink = std::make_unique<TabletWriterSink>(_tablet_writer.get());
        _write_schema_for_mem_table = MemTable::convert_schema(_write_schema, _slots);

        DCHECK_LE(_write_schema->num_columns(), _tablet_schema->num_columns());
        DCHECK_GE(_write_schema_for_mem_table.num_fields(), _write_schema->num_columns());
        if (_write_schema->num_columns() < _tablet_schema->num_columns()) {
            DCHECK_EQ(_write_column_ids.size(), _write_schema->num_columns());
        }
    }
    return Status::OK();
}

inline Status DeltaWriterImpl::reset_memtable() {
    RETURN_IF_ERROR(build_schema_and_writer());
    if (_slots != nullptr || !_merge_condition.empty()) {
        _mem_table = std::make_unique<MemTable>(_tablet_id, &_write_schema_for_mem_table, _slots, _mem_table_sink.get(),
                                                _merge_condition, _mem_tracker);
    } else {
        _mem_table = std::make_unique<MemTable>(_tablet_id, &_write_schema_for_mem_table, _mem_table_sink.get(),
                                                _max_buffer_size, _mem_tracker);
    }
    return Status::OK();
}

inline Status DeltaWriterImpl::flush_async() {
    Status st;
    if (_mem_table != nullptr) {
        RETURN_IF_ERROR(_mem_table->finalize());
        if (_miss_auto_increment_column && _mem_table->get_result_chunk() != nullptr) {
            RETURN_IF_ERROR(fill_auto_increment_id(*_mem_table->get_result_chunk()));
        }
        st = _flush_token->submit(std::move(_mem_table), false, [this](std::unique_ptr<SegmentPB> seg, bool eos) {
            if (_immutable_tablet_size > 0 && !_is_immutable.load(std::memory_order_relaxed)) {
                if (seg) {
                    _tablet_manager->add_in_writing_data_size(_tablet_id, seg->data_size());
                }
                if (_tablet_manager->in_writing_data_size(_tablet_id) > _immutable_tablet_size) {
                    _is_immutable.store(true, std::memory_order_relaxed);
                }
                VLOG(1) << "flush memtable, tablet=" << _tablet_id << ", txn=" << _txn_id
                        << " _immutable_tablet_size=" << _immutable_tablet_size << ", segment_size=" << seg->data_size()
                        << ", in_writing_data_size=" << _tablet_manager->in_writing_data_size(_tablet_id)
                        << ", is_immutable=" << _is_immutable.load(std::memory_order_relaxed);
            }
        });
        _mem_table.reset(nullptr);
        _last_write_ts = 0;
    }
    return st;
}

inline Status DeltaWriterImpl::init_tablet_schema() {
    if (_tablet_schema != nullptr) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));
    auto res = tablet.get_schema_by_id(_schema_id);
    if (res.ok()) {
        _tablet_schema = std::move(res).value();
        return Status::OK();
    } else if (res.status().is_not_found()) {
        LOG(WARNING) << "No schema file of id=" << _schema_id << " for tablet=" << _tablet_id;
        // schema file does not exist, fetch tablet schema from tablet metadata
        ASSIGN_OR_RETURN(_tablet_schema, tablet.get_schema());
        return Status::OK();
    } else {
        return res.status();
    }
}

inline Status DeltaWriterImpl::flush() {
    RETURN_IF_ERROR(flush_async());
    return _flush_token->wait();
}

// To developers: Do NOT perform any I/O in this method, because this method may be invoked
// in a bthread.
Status DeltaWriterImpl::open() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    _flush_token = StorageEngine::instance()->lake_memtable_flush_executor()->create_flush_token();
    if (_flush_token == nullptr) {
        return Status::InternalError("fail to create flush token");
    }
    return Status::OK();
}

Status DeltaWriterImpl::check_partial_update_with_sort_key(const Chunk& chunk) {
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && _partial_schema_with_sort_key_conflict) {
        bool ok = true;
        if (_slots != nullptr && _slots->back()->col_name() == "__op") {
            size_t op_column_id = chunk.num_columns() - 1;
            const auto& op_column = chunk.get_column_by_index(op_column_id);
            auto* ops = reinterpret_cast<const uint8_t*>(op_column->raw_data());
            ok = !std::any_of(ops, ops + chunk.num_rows(), [](auto op) { return op == TOpType::UPSERT; });
        } else {
            ok = false;
        }
        if (!ok) {
            string msg;
            if (_partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
                msg = "partial update on table with sort key must provide all sort key columns";
            } else {
                msg = "column mode partial update on table with sort key cannot update sort key column";
            }
            LOG(WARNING) << msg;
            return Status::NotSupported(msg);
        }
    }
    return Status::OK();
}

Status DeltaWriterImpl::write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size) {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);

    if (_mem_table == nullptr) {
        // When loading memory usage is larger than hard limit, we will reject new loading task.
        if (!config::enable_new_load_on_memory_limit_exceeded &&
            is_tracker_hit_hard_limit(GlobalEnv::GetInstance()->load_mem_tracker(),
                                      config::load_process_max_memory_hard_limit_ratio)) {
            return Status::MemoryLimitExceeded(
                    "memory limit exceeded, please reduce load frequency or increase config "
                    "`load_process_max_memory_hard_limit_ratio` or add more BE nodes");
        }
        RETURN_IF_ERROR(reset_memtable());
    }
    RETURN_IF_ERROR(check_partial_update_with_sort_key(chunk));
    _last_write_ts = butil::gettimeofday_s();
    Status st;
    auto res = _mem_table->insert(chunk, indexes, 0, indexes_size);
    if (!res.ok()) {
        return res.status();
    }
    auto full = res.value();
    if (_mem_tracker->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to memory limit exceeded";
        st = flush();
    } else if (_mem_tracker->parent() && _mem_tracker->parent()->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to parent memory limit exceeded";
        st = flush();
    } else if (full) {
        st = flush_async();
    }
    return st;
}

Status DeltaWriterImpl::init_write_schema() {
    if (_tablet_schema == nullptr) {
        return Status::InternalError("init_write_schema() must be invoked after init_tablet_schema()");
    }
    // default value of _write_schema is _tablet_schema
    _write_schema = _tablet_schema;
    if (_slots == nullptr) {
        return Status::OK();
    }
    const auto has_op_column = (this->_slots->size() > 0 && this->_slots->back()->col_name() == "__op");
    const auto write_columns = has_op_column ? _slots->size() - 1 : _slots->size();

    // maybe partial update, change to partial tablet schema
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && write_columns < _tablet_schema->num_columns()) {
        _write_column_ids.reserve(write_columns);
        for (auto i = 0; i < write_columns; ++i) {
            const auto& slot_col_name = (*_slots)[i]->col_name();
            int32_t index = _tablet_schema->field_index(slot_col_name);
            if (index < 0) {
                return Status::InvalidArgument(strings::Substitute("Invalid column name: $0", slot_col_name));
            }
            _write_column_ids.push_back(index);
        }
        auto sort_key_idxes = _tablet_schema->sort_key_idxes();
        std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
        _partial_schema_with_sort_key_conflict = starrocks::DeltaWriter::is_partial_update_with_sort_key_conflict(
                _partial_update_mode, _write_column_ids, sort_key_idxes, _tablet_schema->num_key_columns());
        _write_schema = TabletSchema::create(_tablet_schema, _write_column_ids);
    }

    auto sort_key_idxes = _write_schema->sort_key_idxes();
    std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
    bool auto_increment_in_sort_key = false;
    for (auto& idx : sort_key_idxes) {
        auto& col = _write_schema->column(idx);
        if (col.is_auto_increment()) {
            auto_increment_in_sort_key = true;
            break;
        }
    }

    if (auto_increment_in_sort_key && _miss_auto_increment_column) {
        LOG(WARNING) << "auto increment column in sort key do not support partial update";
        return Status::NotSupported("auto increment column in sort key do not support partial update");
    }
    return Status::OK();
}

bool DeltaWriterImpl::is_partial_update() {
    return _write_schema->num_columns() < _tablet_schema->num_columns();
}

Status DeltaWriterImpl::finish() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    RETURN_IF_ERROR(build_schema_and_writer());
    RETURN_IF_ERROR(flush());
    RETURN_IF_ERROR(_tablet_writer->finish());
    return Status::OK();
}

StatusOr<TxnLogPtr> DeltaWriterImpl::finish_with_txnlog(DeltaWriterFinishMode mode) {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    RETURN_IF_ERROR(finish());

    if (UNLIKELY(_txn_id < 0)) {
        return Status::InvalidArgument(fmt::format("negative txn id: {}", _txn_id));
    }

    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_id);
    txn_log->set_txn_id(_txn_id);
    txn_log->set_partition_id(_partition_id);
    auto op_write = txn_log->mutable_op_write();

    for (auto& f : _tablet_writer->files()) {
        if (is_segment(f.path)) {
            op_write->mutable_rowset()->add_segments(std::move(f.path));
            op_write->mutable_rowset()->add_segment_size(f.size.value());
            op_write->mutable_rowset()->add_segment_encryption_metas(f.encryption_meta);
        } else if (is_del(f.path)) {
            op_write->add_dels(std::move(f.path));
            op_write->add_del_encryption_metas(f.encryption_meta);
        } else {
            return Status::InternalError(fmt::format("unknown file {}", f.path));
        }
    }
    op_write->mutable_rowset()->set_num_rows(_tablet_writer->num_rows());
    op_write->mutable_rowset()->set_data_size(_tablet_writer->data_size());
    op_write->mutable_rowset()->set_overlapped(op_write->rowset().segments_size() > 1);

    if (is_partial_update() && !_merge_condition.empty()) {
        return Status::NotSupported("partial update and condition update at the same time");
    }

    // handle partial update
    bool skip_pk_preload = config::skip_lake_pk_preload;
    RowsetTxnMetaPB* rowset_txn_meta = _tablet_writer->rowset_txn_meta();
    if (rowset_txn_meta != nullptr) {
        if (is_partial_update()) {
            op_write->mutable_txn_meta()->CopyFrom(*rowset_txn_meta);
            for (auto i = 0; i < _write_schema->columns().size(); ++i) {
                const auto& tablet_column = _write_schema->column(i);
                op_write->mutable_txn_meta()->add_partial_update_column_ids(_write_column_ids[i]);
                op_write->mutable_txn_meta()->add_partial_update_column_unique_ids(tablet_column.unique_id());
            }
            if (_partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
                // generate rewrite segment names to avoid gc in rewrite operation
                for (auto i = 0; i < op_write->rowset().segments_size(); i++) {
                    op_write->add_rewrite_segments(gen_segment_filename(_txn_id));
                }
            } else {
                skip_pk_preload = true;
            }
            // handle partial update
            op_write->mutable_txn_meta()->set_partial_update_mode(_partial_update_mode);
            if (_column_to_expr_value != nullptr) {
                for (auto& [name, value] : (*_column_to_expr_value)) {
                    op_write->mutable_txn_meta()->mutable_column_to_expr_value()->insert({name, value});
                }
            }
        }
        // handle condition update
        if (_merge_condition != "") {
            op_write->mutable_txn_meta()->set_merge_condition(_merge_condition);
        }
        // handle auto increment
        if (_miss_auto_increment_column) {
            for (auto i = 0; i < _write_schema->num_columns(); ++i) {
                auto col = _write_schema->column(i);
                if (col.is_auto_increment()) {
                    /*
                        The auto increment id set here is inconsistent with the id in
                        full tablet schema. The id here is indicate the offset id of
                        auto increment column in partial segment file.
                    */
                    op_write->mutable_txn_meta()->set_auto_increment_partial_update_column_id(i);
                    break;
                }
            }

            if (op_write->rewrite_segments_size() == 0) {
                for (auto i = 0; i < op_write->rowset().segments_size(); i++) {
                    op_write->add_rewrite_segments(gen_segment_filename(_txn_id));
                }
            }
        }
    }
    if (mode == kWriteTxnLog) {
        RETURN_IF_ERROR(tablet.put_txn_log(txn_log));
    } else {
        auto cache_key = _tablet_manager->txn_log_location(_tablet_id, _txn_id);
        _tablet_manager->metacache()->cache_txn_log(cache_key, txn_log);
    }
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && !skip_pk_preload) {
        // preload update state here to minimaze the cost when publishing.
        tablet.update_mgr()->preload_update_state(*txn_log, &tablet);
    }
    return txn_log;
}

Status DeltaWriterImpl::fill_auto_increment_id(const Chunk& chunk) {
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));

    // 1. get pk column from chunk
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < _write_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(_write_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }
    auto col = pk_column->clone();

    PrimaryKeyEncoder::encode(pkey_schema, chunk, 0, chunk.num_rows(), col.get());
    std::vector<std::unique_ptr<Column>> upserts;
    upserts.resize(1);
    upserts[0] = std::move(col);

    std::vector<uint64_t> rss_rowid_map(upserts[0]->size(), (uint64_t)((uint32_t)-1) << 32);
    std::vector<std::vector<uint64_t>*> rss_rowids;
    rss_rowids.resize(1);
    rss_rowids[0] = &rss_rowid_map;

    // 2. probe index
    auto metadata = _tablet_manager->get_latest_cached_tablet_metadata(_tablet_id);
    Status st;
    if (metadata != nullptr) {
        st = tablet.update_mgr()->get_rowids_from_pkindex(tablet.id(), metadata->version(), upserts, &rss_rowids, true);
    }

    Filter filter;
    uint32_t gen_num = 0;
    // There are two cases we should allocate full id for this chunk for simplicity:
    // 1. We can not get the tablet meta from cache.
    // 2. fail in seeking index
    if (metadata != nullptr && st.ok()) {
        for (unsigned long v : rss_rowid_map) {
            uint32_t rssid = v >> 32;
            if (rssid == (uint32_t)-1) {
                filter.emplace_back(1);
                ++gen_num;
            } else {
                filter.emplace_back(0);
            }
        }
    } else {
        gen_num = rss_rowid_map.size();
        filter.resize(gen_num, 1);
    }

    // 3. fill the non-existing rows
    std::vector<int64_t> ids(gen_num);
    RETURN_IF_ERROR(StorageEngine::instance()->get_next_increment_id_interval(_table_id, gen_num, ids));

    for (int i = 0; i < _write_schema->num_columns(); i++) {
        const TabletColumn& tablet_column = _write_schema->column(i);
        if (tablet_column.is_auto_increment()) {
            auto& column = chunk.get_column_by_index(i);
            RETURN_IF_ERROR((std::dynamic_pointer_cast<Int64Column>(column))->fill_range(ids, filter));
            break;
        }
    }

    return Status::OK();
}

void DeltaWriterImpl::close() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);

    if (_flush_token != nullptr) {
        auto st = _flush_token->wait();
        LOG_IF(WARNING, !st.ok()) << "flush token error: " << st;
        VLOG(3) << "Tablet_id: " << tablet_id() << ", flush stats: " << _flush_token->get_stats();
    }

    // Destruct variables manually for counting memory usage into |_mem_tracker|
    if (_tablet_writer != nullptr) {
        _tablet_writer->close();
    }
    _tablet_writer.reset();
    _mem_table.reset();
    _mem_table_sink.reset();
    _flush_token.reset();
    _tablet_schema.reset();
    _write_schema.reset();
    _merge_condition.clear();
}

std::vector<FileInfo> DeltaWriterImpl::files() const {
    return (_tablet_writer != nullptr) ? _tablet_writer->files() : std::vector<FileInfo>();
}

int64_t DeltaWriterImpl::data_size() const {
    return (_tablet_writer != nullptr) ? _tablet_writer->data_size() : 0;
}

int64_t DeltaWriterImpl::num_rows() const {
    return (_tablet_writer != nullptr) ? _tablet_writer->num_rows() : 0;
}

int64_t DeltaWriterImpl::queueing_memtable_num() const {
    if (_flush_token != nullptr) {
        return _flush_token->get_stats().queueing_memtable_num;
    } else {
        return 0;
    }
}

//// DeltaWriter

DeltaWriter::~DeltaWriter() {
    delete _impl;
}

Status DeltaWriter::open() {
    return _impl->open();
}

Status DeltaWriter::write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size) {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::write() in a bthread";
    return _impl->write(chunk, indexes, indexes_size);
}

StatusOr<TxnLogPtr> DeltaWriter::finish_with_txnlog(DeltaWriterFinishMode mode) {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::finish_with_txnlog() in a bthread";
    return _impl->finish_with_txnlog(mode);
}

Status DeltaWriter::finish() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::finish() in a bthread";
    return _impl->finish();
}

void DeltaWriter::close() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::close() in a bthread";
    _impl->close();
}

int64_t DeltaWriter::partition_id() const {
    return _impl->partition_id();
}

int64_t DeltaWriter::tablet_id() const {
    return _impl->tablet_id();
}

int64_t DeltaWriter::txn_id() const {
    return _impl->txn_id();
}

MemTracker* DeltaWriter::mem_tracker() {
    return _impl->mem_tracker();
}

Status DeltaWriter::flush() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::flush() in a bthread";
    return _impl->flush();
}

Status DeltaWriter::flush_async() {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::flush_async() in a bthread";
    return _impl->flush_async();
}

std::vector<FileInfo> DeltaWriter::files() const {
    return _impl->files();
}

const int64_t DeltaWriter::queueing_memtable_num() const {
    return _impl->queueing_memtable_num();
}

int64_t DeltaWriter::data_size() const {
    return _impl->data_size();
}

int64_t DeltaWriter::num_rows() const {
    return _impl->num_rows();
}

bool DeltaWriter::is_immutable() const {
    return _impl->is_immutable();
}

Status DeltaWriter::check_immutable() {
    return _impl->check_immutable();
}

int64_t DeltaWriter::last_write_ts() const {
    return _impl->last_write_ts();
}

ThreadPool* DeltaWriter::io_threads() {
    if (UNLIKELY(StorageEngine::instance() == nullptr)) {
        return nullptr;
    }
    if (UNLIKELY(StorageEngine::instance()->lake_memtable_flush_executor() == nullptr)) {
        return nullptr;
    }
    return StorageEngine::instance()->lake_memtable_flush_executor()->get_thread_pool();
}

StatusOr<DeltaWriterBuilder::DeltaWriterPtr> DeltaWriterBuilder::build() {
    if (UNLIKELY(_tablet_mgr == nullptr)) {
        return Status::InvalidArgument("tablet_manager not set");
    }
    if (UNLIKELY(_tablet_id == 0)) {
        return Status::InvalidArgument("tablet_id not set");
    }
    if (UNLIKELY(_txn_id == 0)) {
        return Status::InvalidArgument("txn_id not set");
    }
    if (UNLIKELY(_mem_tracker == nullptr)) {
        return Status::InvalidArgument("mem_tracker not set");
    }
    if (UNLIKELY(_max_buffer_size < 0)) {
        return Status::InvalidArgument(fmt::format("invalid max_buffer_size: {}", _max_buffer_size));
    }
    if (UNLIKELY(_miss_auto_increment_column && _table_id == 0)) {
        return Status::InvalidArgument("must set table_id when miss_auto_increment_column is true");
    }
    if (UNLIKELY(_schema_id == 0)) {
        return Status::InvalidArgument("schema_id not set");
    }
    auto impl = new DeltaWriterImpl(_tablet_mgr, _tablet_id, _txn_id, _partition_id, _slots, _merge_condition,
                                    _miss_auto_increment_column, _table_id, _immutable_tablet_size, _mem_tracker,
                                    _max_buffer_size, _schema_id, _partial_update_mode, _column_to_expr_value);
    return std::make_unique<DeltaWriter>(impl);
}

} // namespace starrocks::lake
