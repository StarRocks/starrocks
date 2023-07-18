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

#include <memory>
#include <utility>

#include "column/chunk.h"
#include "column/column.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/memtable.h"
#include "storage/memtable_flush_executor.h"
#include "storage/memtable_sink.h"
#include "storage/primary_key_encoder.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

using Chunk = starrocks::Chunk;
using Column = starrocks::Column;
using MemTable = starrocks::MemTable;
using MemTableSink = starrocks::MemTableSink;

class TabletWriterSink : public MemTableSink {
public:
    explicit TabletWriterSink(TabletWriter* w) : _writer(w) {}

    ~TabletWriterSink() override = default;

    DISALLOW_COPY_AND_MOVE(TabletWriterSink);

    Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment = nullptr) override {
        RETURN_IF_ERROR(_writer->write(chunk));
        return _writer->flush();
    }

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                    starrocks::SegmentPB* segment = nullptr) override {
        RETURN_IF_ERROR(_writer->flush_del_file(deletes));
        RETURN_IF_ERROR(_writer->write(upserts));
        return _writer->flush();
    }

private:
    TabletWriter* _writer;
};

/// DeltaWriterImpl

class DeltaWriterImpl {
public:
    explicit DeltaWriterImpl(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                             const std::vector<SlotDescriptor*>* slots, MemTracker* mem_tracker)
            : _tablet_manager(tablet_manager),
              _tablet_id(tablet_id),
              _txn_id(txn_id),
              _partition_id(partition_id),
              _mem_tracker(mem_tracker),
              _slots(slots),
              _schema_initialized(false),
              _miss_auto_increment_column(false) {}

    explicit DeltaWriterImpl(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                             const std::vector<SlotDescriptor*>* slots, std::string merge_condition,
                             MemTracker* mem_tracker)
            : _tablet_manager(tablet_manager),
              _tablet_id(tablet_id),
              _txn_id(txn_id),
              _partition_id(partition_id),
              _mem_tracker(mem_tracker),
              _slots(slots),
              _schema_initialized(false),
              _merge_condition(std::move(merge_condition)),
              _miss_auto_increment_column(false) {}

    explicit DeltaWriterImpl(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t partition_id,
                             const std::vector<SlotDescriptor*>* slots, std::string merge_condition,
                             bool miss_auto_increment_column, int64_t table_id, MemTracker* mem_tracker)
            : _tablet_manager(tablet_manager),
              _tablet_id(tablet_id),
              _txn_id(txn_id),
              _partition_id(partition_id),
              _mem_tracker(mem_tracker),
              _slots(slots),
              _schema_initialized(false),
              _merge_condition(std::move(merge_condition)),
              _miss_auto_increment_column(miss_auto_increment_column),
              _table_id(table_id) {}

    explicit DeltaWriterImpl(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id, int64_t max_buffer_size,
                             MemTracker* mem_tracker)
            : _tablet_manager(tablet_manager),
              _tablet_id(tablet_id),
              _txn_id(txn_id),
              _partition_id(-1),
              _mem_tracker(mem_tracker),
              _slots(nullptr),
              _max_buffer_size(max_buffer_size),
              _schema_initialized(false),
              _miss_auto_increment_column(false) {}

    ~DeltaWriterImpl() = default;

    DISALLOW_COPY_AND_MOVE(DeltaWriterImpl);

    [[nodiscard]] Status open();

    [[nodiscard]] Status write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size);

    [[nodiscard]] Status finish(DeltaWriter::FinishMode mode);

    void close();

    [[nodiscard]] int64_t partition_id() const { return _partition_id; }

    [[nodiscard]] int64_t tablet_id() const { return _tablet_id; }

    [[nodiscard]] int64_t txn_id() const { return _txn_id; }

    [[nodiscard]] MemTracker* mem_tracker() { return _mem_tracker; }

    [[nodiscard]] Status flush();

    [[nodiscard]] Status flush_async();

    std::vector<std::string> files() const;

    int64_t data_size() const;

    int64_t num_rows() const;

    Status handle_partial_update();

    Status build_schema_and_writer();

    void TEST_set_partial_update(std::shared_ptr<const TabletSchema> tschema,
                                 const std::vector<int32_t>& referenced_column_ids);

    void TEST_set_miss_auto_increment_column();

private:
    Status reset_memtable();

    Status _fill_auto_increment_id(const Chunk& chunk);

    TabletManager* _tablet_manager;
    const int64_t _tablet_id;
    const int64_t _txn_id;
    const int64_t _partition_id;
    MemTracker* const _mem_tracker;

    // for load
    const std::vector<SlotDescriptor*>* const _slots;

    // for schema change
    int64_t _max_buffer_size = config::write_buffer_size;

    std::unique_ptr<TabletWriter> _tablet_writer;
    std::unique_ptr<MemTable> _mem_table;
    std::unique_ptr<MemTableSink> _mem_table_sink;
    std::unique_ptr<FlushToken> _flush_token;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    Schema _vectorized_schema;
    bool _schema_initialized;

    // for partial update
    std::shared_ptr<const TabletSchema> _partial_update_tablet_schema;
    std::vector<int32_t> _referenced_column_ids;

    // for condition update
    std::string _merge_condition;

    // for auto increment
    bool _miss_auto_increment_column; // true if miss AUTO_INCREMENT column
                                      // in partial update mode
    int64_t _table_id;
};

void DeltaWriterImpl::TEST_set_partial_update(std::shared_ptr<const TabletSchema> tschema,
                                              const std::vector<int32_t>& referenced_column_ids) {
    _partial_update_tablet_schema = std::move(tschema);
    _referenced_column_ids = referenced_column_ids;
    (void)build_schema_and_writer();
    // recover _tablet_schema with partial update schema
    _tablet_schema = _partial_update_tablet_schema;
}

void DeltaWriterImpl::TEST_set_miss_auto_increment_column() {
    _miss_auto_increment_column = true;
}

Status DeltaWriterImpl::build_schema_and_writer() {
    if (_mem_table_sink == nullptr) {
        DCHECK(_tablet_writer == nullptr);
        ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));
        ASSIGN_OR_RETURN(_tablet_schema, tablet.get_schema());
        RETURN_IF_ERROR(handle_partial_update());
        ASSIGN_OR_RETURN(_tablet_writer, tablet.new_writer(kHorizontal, _txn_id));
        if (_partial_update_tablet_schema != nullptr) {
            _tablet_writer->set_tablet_schema(_partial_update_tablet_schema);
        }
        RETURN_IF_ERROR(_tablet_writer->open());
        _mem_table_sink = std::make_unique<TabletWriterSink>(_tablet_writer.get());
    }
    return Status::OK();
}

inline Status DeltaWriterImpl::reset_memtable() {
    RETURN_IF_ERROR(build_schema_and_writer());
    if (!_schema_initialized) {
        _vectorized_schema = MemTable::convert_schema(_tablet_schema.get(), _slots);
        _schema_initialized = true;
    }
    if (_slots != nullptr || !_merge_condition.empty()) {
        _mem_table = std::make_unique<MemTable>(_tablet_id, &_vectorized_schema, _slots, _mem_table_sink.get(),
                                                _merge_condition, _mem_tracker);
    } else {
        _mem_table = std::make_unique<MemTable>(_tablet_id, &_vectorized_schema, _mem_table_sink.get(),
                                                _max_buffer_size, _mem_tracker);
    }
    return Status::OK();
}

inline Status DeltaWriterImpl::flush_async() {
    Status st;
    if (_mem_table != nullptr) {
        RETURN_IF_ERROR(_mem_table->finalize());
        if (_miss_auto_increment_column && _mem_table->get_result_chunk() != nullptr) {
            RETURN_IF_ERROR(_fill_auto_increment_id(*_mem_table->get_result_chunk()));
        }
        st = _flush_token->submit(std::move(_mem_table));
        _mem_table.reset(nullptr);
    }
    return st;
}

inline Status DeltaWriterImpl::flush() {
    RETURN_IF_ERROR(flush_async());
    return _flush_token->wait();
}

// To developers: Do NOT perform any I/O in this method, because this method may be invoked
// in a bthread.
Status DeltaWriterImpl::open() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    _flush_token = StorageEngine::instance()->memtable_flush_executor()->create_flush_token();
    if (_flush_token == nullptr) {
        return Status::InternalError("fail to create flush token");
    }
    return Status::OK();
}

Status DeltaWriterImpl::write(const Chunk& chunk, const uint32_t* indexes, uint32_t indexes_size) {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);

    if (_mem_table == nullptr) {
        RETURN_IF_ERROR(reset_memtable());
    }
    Status st;
    bool full = _mem_table->insert(chunk, indexes, 0, indexes_size);
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

Status DeltaWriterImpl::handle_partial_update() {
    if (_slots == nullptr) return Status::OK();
    const std::size_t partial_cols_num = [this]() {
        if (this->_slots->size() > 0 && this->_slots->back()->col_name() == "__op") {
            return this->_slots->size() - 1;
        } else {
            return this->_slots->size();
        }
    }();
    // maybe partial update, change to partial tablet schema
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && partial_cols_num < _tablet_schema->num_columns()) {
        _referenced_column_ids.reserve(partial_cols_num);
        for (auto i = 0; i < partial_cols_num; ++i) {
            const auto& slot_col_name = (*_slots)[i]->col_name();
            int32_t index = _tablet_schema->field_index(slot_col_name);
            if (index < 0) {
                return Status::InvalidArgument(strings::Substitute("Invalid column name: $0", slot_col_name));
            }
            _referenced_column_ids.push_back(index);
        }
        _partial_update_tablet_schema = TabletSchema::create(*_tablet_schema, _referenced_column_ids);
        auto sort_key_idxes = _tablet_schema->sort_key_idxes();
        std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
        if (!std::includes(_referenced_column_ids.begin(), _referenced_column_ids.end(), sort_key_idxes.begin(),
                           sort_key_idxes.end())) {
            LOG(WARNING) << "table with sort key do not support partial update";
            return Status::NotSupported("table with sort key do not support partial update");
        }
        _tablet_schema = _partial_update_tablet_schema;
    }

    auto sort_key_idxes = _tablet_schema->sort_key_idxes();
    std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
    bool auto_increment_in_sort_key = false;
    for (auto& idx : sort_key_idxes) {
        auto& col = _tablet_schema->column(idx);
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

Status DeltaWriterImpl::finish(DeltaWriter::FinishMode mode) {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    RETURN_IF_ERROR(build_schema_and_writer());
    RETURN_IF_ERROR(flush());
    RETURN_IF_ERROR(_tablet_writer->finish());

    if (mode == DeltaWriter::kDontWriteTxnLog) {
        return Status::OK();
    }

    if (UNLIKELY(_txn_id < 0)) {
        return Status::InvalidArgument(fmt::format("negative txn id: {}", _txn_id));
    }

    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_id);
    txn_log->set_txn_id(_txn_id);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : _tablet_writer->files()) {
        if (is_segment(f)) {
            op_write->mutable_rowset()->add_segments(std::move(f));
        } else if (is_del(f)) {
            op_write->add_dels(std::move(f));
        } else {
            return Status::InternalError(fmt::format("unknown file {}", f));
        }
    }
    op_write->mutable_rowset()->set_num_rows(_tablet_writer->num_rows());
    op_write->mutable_rowset()->set_data_size(_tablet_writer->data_size());
    op_write->mutable_rowset()->set_overlapped(op_write->rowset().segments_size() > 1);
    // not support handle partial update and condition update at the same time
    if (_partial_update_tablet_schema != nullptr && _merge_condition != "") {
        return Status::NotSupported("partial update and condition update at the same time");
    }
    // handle partial update
    RowsetTxnMetaPB* rowset_txn_meta = _tablet_writer->rowset_txn_meta();
    if (rowset_txn_meta != nullptr) {
        if (_partial_update_tablet_schema != nullptr) {
            op_write->mutable_txn_meta()->CopyFrom(*rowset_txn_meta);
            for (auto i = 0; i < _partial_update_tablet_schema->columns().size(); ++i) {
                const auto& tablet_column = _partial_update_tablet_schema->column(i);
                op_write->mutable_txn_meta()->add_partial_update_column_ids(_referenced_column_ids[i]);
                op_write->mutable_txn_meta()->add_partial_update_column_unique_ids(tablet_column.unique_id());
            }
            // generate rewrite segment names to avoid gc in rewrite operation
            for (auto i = 0; i < op_write->rowset().segments_size(); i++) {
                op_write->add_rewrite_segments(gen_segment_filename(_txn_id));
            }
        }
        // handle condition update
        if (_merge_condition != "") {
            op_write->mutable_txn_meta()->set_merge_condition(_merge_condition);
        }
        // handle auto increment
        if (_miss_auto_increment_column) {
            for (auto i = 0; i < _tablet_schema->num_columns(); ++i) {
                auto col = _tablet_schema->column(i);
                if (col.is_auto_increment()) {
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
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        // preload update state here to minimaze the cost when publishing.
        tablet.update_mgr()->preload_update_state(*txn_log, &tablet);
    }
    RETURN_IF_ERROR(tablet.put_txn_log(std::move(txn_log)));
    return Status::OK();
}

Status DeltaWriterImpl::_fill_auto_increment_id(const Chunk& chunk) {
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(_tablet_id));

    // 1. get pk column from chunk
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(*_tablet_schema, pk_columns);
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
        st = tablet.update_mgr()->get_rowids_from_pkindex(&tablet, metadata->version(), upserts, &rss_rowids);
    }

    std::vector<uint8_t> filter;
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

    for (int i = 0; i < _vectorized_schema.num_fields(); i++) {
        const TabletColumn& tablet_column = _tablet_schema->column(i);
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
        (void)_flush_token->wait();
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
    _partial_update_tablet_schema.reset();
    _merge_condition.clear();
}

std::vector<std::string> DeltaWriterImpl::files() const {
    return (_tablet_writer != nullptr) ? _tablet_writer->files() : std::vector<std::string>();
}

int64_t DeltaWriterImpl::data_size() const {
    return (_tablet_writer != nullptr) ? _tablet_writer->data_size() : 0;
}

int64_t DeltaWriterImpl::num_rows() const {
    return (_tablet_writer != nullptr) ? _tablet_writer->num_rows() : 0;
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

Status DeltaWriter::finish(FinishMode mode) {
    DCHECK_EQ(0, bthread_self()) << "Should not invoke DeltaWriter::finish() in a bthread";
    return _impl->finish(mode);
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

std::vector<std::string> DeltaWriter::files() const {
    return _impl->files();
}

int64_t DeltaWriter::data_size() const {
    return _impl->data_size();
}

int64_t DeltaWriter::num_rows() const {
    return _impl->num_rows();
}

void DeltaWriter::TEST_set_partial_update(std::shared_ptr<const TabletSchema> tschema,
                                          const std::vector<int32_t>& referenced_column_ids) {
    _impl->TEST_set_partial_update(std::move(tschema), referenced_column_ids);
}

void DeltaWriter::TEST_set_miss_auto_increment_column() {
    _impl->TEST_set_miss_auto_increment_column();
}

std::unique_ptr<DeltaWriter> DeltaWriter::create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id,
                                                 int64_t partition_id, const std::vector<SlotDescriptor*>* slots,
                                                 MemTracker* mem_tracker) {
    return std::make_unique<DeltaWriter>(
            new DeltaWriterImpl(tablet_manager, tablet_id, txn_id, partition_id, slots, mem_tracker));
}

std::unique_ptr<DeltaWriter> DeltaWriter::create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id,
                                                 int64_t partition_id, const std::vector<SlotDescriptor*>* slots,
                                                 const std::string& merge_condition, MemTracker* mem_tracker) {
    return std::make_unique<DeltaWriter>(
            new DeltaWriterImpl(tablet_manager, tablet_id, txn_id, partition_id, slots, merge_condition, mem_tracker));
}

std::unique_ptr<DeltaWriter> DeltaWriter::create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id,
                                                 int64_t partition_id, const std::vector<SlotDescriptor*>* slots,
                                                 const std::string& merge_condition, bool miss_auto_increment_column,
                                                 int64_t table_id, MemTracker* mem_tracker) {
    return std::make_unique<DeltaWriter>(new DeltaWriterImpl(tablet_manager, tablet_id, txn_id, partition_id, slots,
                                                             merge_condition, miss_auto_increment_column, table_id,
                                                             mem_tracker));
}

std::unique_ptr<DeltaWriter> DeltaWriter::create(TabletManager* tablet_manager, int64_t tablet_id, int64_t txn_id,
                                                 int64_t max_buffer_size, MemTracker* mem_tracker) {
    return std::make_unique<DeltaWriter>(
            new DeltaWriterImpl(tablet_manager, tablet_id, txn_id, max_buffer_size, mem_tracker));
}

ThreadPool* DeltaWriter::io_threads() {
    if (UNLIKELY(StorageEngine::instance() == nullptr)) {
        return nullptr;
    }
    if (UNLIKELY(StorageEngine::instance()->memtable_flush_executor() == nullptr)) {
        return nullptr;
    }
    return StorageEngine::instance()->memtable_flush_executor()->get_thread_pool();
}

} // namespace starrocks::lake
