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

#include "storage/lake/schema_change.h"

#include <memory>

#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/join_path.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/schema_change_utils.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader_params.h"

namespace starrocks::lake {

class SchemaChange {
public:
    explicit SchemaChange(TabletManager* tablet_manager, int64_t txn_id)
            : _tablet_manager(tablet_manager), _txn_id(txn_id) {}

    virtual ~SchemaChange() = default;

    virtual Status init() = 0;
    virtual Status process(RowsetPtr rowset, RowsetMetadata* new_rowset_metadata) = 0;

protected:
    TabletManager* _tablet_manager;
    int64_t _txn_id;
};

class LinkedSchemaChange final : public SchemaChange {
public:
    explicit LinkedSchemaChange(TabletManager* tablet_manager, int64_t txn_id) : SchemaChange(tablet_manager, txn_id) {}
    ~LinkedSchemaChange() override = default;

    DISALLOW_COPY_AND_MOVE(LinkedSchemaChange);

    Status init() override { return Status::OK(); }
    Status process(RowsetPtr rowset, RowsetMetadata* new_rowset_metadata) override;
};

class ConvertedSchemaChange : public SchemaChange {
public:
    explicit ConvertedSchemaChange(TabletManager* tablet_manager, int64_t txn_id, Tablet* base_tablet,
                                   Tablet* new_tablet, int64_t version, ChunkChanger* chunk_changer)
            : SchemaChange(tablet_manager, txn_id),
              _base_tablet(base_tablet),
              _new_tablet(new_tablet),
              _version(version),
              _chunk_changer(chunk_changer) {
        CHECK(_base_tablet != nullptr);
        CHECK(_new_tablet != nullptr);
        CHECK(_chunk_changer != nullptr);
    }

    ~ConvertedSchemaChange() override = default;

    Status init() override;

protected:
    Tablet* _base_tablet = nullptr;
    Tablet* _new_tablet = nullptr;
    int64_t _version = 0;
    ChunkChanger* _chunk_changer = nullptr;

    TabletReaderParams _read_params;
    std::shared_ptr<const TabletSchema> _new_tablet_schema;
    Schema _base_schema;
    Schema _new_schema;
    ChunkPtr _base_chunk;
    ChunkPtr _new_chunk;
    std::vector<size_t> _char_field_indexes;
    std::unique_ptr<MemPool> _mem_pool;
    int64_t _next_rowset_id = 1; // Same as the value used in `lake::TabletManager::create_tablet()`
};

class DirectSchemaChange final : public ConvertedSchemaChange {
public:
    explicit DirectSchemaChange(TabletManager* tablet_manager, int64_t txn_id, Tablet* base_tablet, Tablet* new_tablet,
                                int64_t version, ChunkChanger* chunk_changer)
            : ConvertedSchemaChange(tablet_manager, txn_id, base_tablet, new_tablet, version, chunk_changer) {}

    ~DirectSchemaChange() override = default;

    DISALLOW_COPY_AND_MOVE(DirectSchemaChange);

    Status process(RowsetPtr rowset, RowsetMetadata* new_rowset_metadata) override;
};

class SortedSchemaChange final : public ConvertedSchemaChange {
public:
    explicit SortedSchemaChange(TabletManager* tablet_manager, int64_t txn_id, Tablet* base_tablet, Tablet* new_tablet,
                                int64_t version, ChunkChanger* chunk_changer, size_t memory_limitation)
            : ConvertedSchemaChange(tablet_manager, txn_id, base_tablet, new_tablet, version, chunk_changer),
              _memory_limitation(memory_limitation) {}

    ~SortedSchemaChange() override = default;

    DISALLOW_COPY_AND_MOVE(SortedSchemaChange);

    Status init() override;
    Status process(RowsetPtr rowset, RowsetMetadata* new_rowset_metadata) override;

private:
    size_t _memory_limitation = 0;
    size_t _max_buffer_size = 0;
    std::unique_ptr<std::vector<uint32_t>> _selective;
};

Status LinkedSchemaChange::process(RowsetPtr rowset, RowsetMetadata* new_rowset_metadata) {
    new_rowset_metadata->CopyFrom(rowset->metadata());
    return Status::OK();
}

Status ConvertedSchemaChange::init() {
    _read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    _read_params.skip_aggregation = false;
    _read_params.chunk_size = config::vector_chunk_size;
    _read_params.use_page_cache = false;
    _read_params.fill_data_cache = false;

    ASSIGN_OR_RETURN(auto base_tablet_schema, _base_tablet->get_schema());
    _base_schema = ChunkHelper::convert_schema(base_tablet_schema, _chunk_changer->get_selected_column_indexes());
    ASSIGN_OR_RETURN(_new_tablet_schema, _new_tablet->get_schema());
    _new_schema = ChunkHelper::convert_schema(_new_tablet_schema);

    _base_chunk = ChunkHelper::new_chunk(_base_schema, config::vector_chunk_size);
    _new_chunk = ChunkHelper::new_chunk(_new_schema, config::vector_chunk_size);

    _char_field_indexes = ChunkHelper::get_char_field_indexes(_new_schema);
    _mem_pool = std::make_unique<MemPool>();
    return Status::OK();
}

Status DirectSchemaChange::process(RowsetPtr rowset, RowsetMetadata* new_rowset_metadata) {
    // create reader
    auto reader = std::make_unique<TabletReader>(*_base_tablet, _version, _base_schema, std::vector<RowsetPtr>{rowset});
    RETURN_IF_ERROR(reader->prepare());
    RETURN_IF_ERROR(reader->open(_read_params));

    // create writer
    ASSIGN_OR_RETURN(auto writer, _new_tablet->new_writer(kHorizontal, _txn_id));
    RETURN_IF_ERROR(writer->open());
    DeferOp defer([&]() { writer->close(); });

    // convert
    while (true) {
        if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
            return Status::InternalError("bg_worker_stopped");
        }
#ifndef BE_TEST
        RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("DirectSchemaChange"));
#endif

        _base_chunk->reset();
        _new_chunk->reset();
        _mem_pool->clear();

        if (auto st = reader->get_next(_base_chunk.get()); st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        }

        if (!_chunk_changer->change_chunk_v2(_base_chunk, _new_chunk, _base_schema, _new_schema, _mem_pool.get())) {
            return Status::InternalError("failed to convert chunk data");
        }

        ChunkHelper::padding_char_columns(_char_field_indexes, _new_schema, _new_tablet_schema, _new_chunk.get());
        RETURN_IF_ERROR(writer->write(*_new_chunk));
    }

    RETURN_IF_ERROR(writer->finish());

    // update new rowset meta
    for (auto& f : writer->files()) {
        new_rowset_metadata->add_segments(std::move(f));
    }
    new_rowset_metadata->set_id(_next_rowset_id);
    new_rowset_metadata->set_num_rows(writer->num_rows());
    new_rowset_metadata->set_data_size(writer->data_size());
    new_rowset_metadata->set_overlapped(rowset->is_overlapped());
    _next_rowset_id += std::max(1, new_rowset_metadata->segments_size());
    return Status::OK();
}

Status SortedSchemaChange::init() {
    RETURN_IF_ERROR(ConvertedSchemaChange::init());

    // memtable max buffer size set default 80% of memory limit so that it will do _merge() if reach limit
    // set max memtable size to 4G since some column has limit size, it will make invalid data
    _max_buffer_size = std::min<size_t>(
            4294967296, static_cast<size_t>(_memory_limitation * config::memory_ratio_for_sorting_schema_change));

    _selective = std::make_unique<std::vector<uint32_t>>();
    _selective->resize(config::vector_chunk_size);
    for (uint32_t i = 0; i < config::vector_chunk_size; i++) {
        (*_selective)[i] = i;
    }
    return Status::OK();
}

Status SortedSchemaChange::process(RowsetPtr rowset, RowsetMetadata* new_rowset_metadata) {
    // create reader
    auto reader = std::make_unique<TabletReader>(*_base_tablet, _version, _base_schema, std::vector<RowsetPtr>{rowset});
    RETURN_IF_ERROR(reader->prepare());
    RETURN_IF_ERROR(reader->open(_read_params));

    // create writer
    ASSIGN_OR_RETURN(auto writer, DeltaWriterBuilder()
                                          .set_tablet_manager(_tablet_manager)
                                          .set_tablet_id(_new_tablet->id())
                                          .set_txn_id(_txn_id)
                                          .set_max_buffer_size(_max_buffer_size)
                                          .set_mem_tracker(CurrentThread::mem_tracker())
                                          .set_index_id(_new_tablet_schema->id()) // TODO: pass tablet schema directly
                                          .build());
    RETURN_IF_ERROR(writer->open());
    DeferOp defer([&]() { writer->close(); });

    // convert
    while (true) {
        if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
            return Status::InternalError("bg_worker_stopped");
        }
#ifndef BE_TEST
        auto cur_usage = CurrentThread::mem_tracker()->consumption();
        // we check memory usage exceeds 90% since tablet reader use some memory
        // it will return fail if memory is exhausted
        if (cur_usage > CurrentThread::mem_tracker()->limit() * 0.9) {
            RETURN_IF_ERROR_WITH_WARN(writer->flush(), "failed to flush writer.");
            VLOG(1) << "SortedSchemaChange memory usage: " << cur_usage << " after writer flush "
                    << CurrentThread::mem_tracker()->consumption();
        }
#endif

        _base_chunk->reset();
        _new_chunk->reset();
        _mem_pool->clear();

        if (auto st = reader->get_next(_base_chunk.get()); st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        }

        if (!_chunk_changer->change_chunk_v2(_base_chunk, _new_chunk, _base_schema, _new_schema, _mem_pool.get())) {
            return Status::InternalError("failed to convert chunk data");
        }

        ChunkHelper::padding_char_columns(_char_field_indexes, _new_schema, _new_tablet_schema, _new_chunk.get());
        RETURN_IF_ERROR(writer->write(*_new_chunk, _selective->data(), _new_chunk->num_rows()));
    }

    RETURN_IF_ERROR(writer->finish(DeltaWriter::kDontWriteTxnLog));

    // update new rowset meta
    for (auto& f : writer->files()) {
        new_rowset_metadata->add_segments(std::move(f));
    }
    new_rowset_metadata->set_id(_next_rowset_id);
    new_rowset_metadata->set_num_rows(writer->num_rows());
    new_rowset_metadata->set_data_size(writer->data_size());
    // TODO: support writer final merge
    new_rowset_metadata->set_overlapped(true);
    _next_rowset_id += std::max(1, new_rowset_metadata->segments_size());
    return Status::OK();
}

Status SchemaChangeHandler::process_alter_tablet(const TAlterTabletReqV2& request) {
    LOG(INFO) << "begin to alter tablet. base tablet: " << request.base_tablet_id
              << ", new tablet: " << request.new_tablet_id << ", alter version: " << request.alter_version;

    MonotonicStopWatch timer;
    timer.start();
    Status status = do_process_alter_tablet(request);
    LOG(INFO) << "finish alter tablet. status: " << status.to_string()
              << ", duration: " << timer.elapsed_time() / 1000000 << " ms"
              << ", peak_mem_usage: " << CurrentThread::mem_tracker()->peak_consumption() << " bytes";
    return status;
}

Status SchemaChangeHandler::do_process_alter_tablet(const TAlterTabletReqV2& request) {
    // get base tablet and new tablet
    auto alter_version = request.alter_version;
    ASSIGN_OR_RETURN(auto base_tablet, _tablet_manager->get_tablet(request.base_tablet_id));
    ASSIGN_OR_RETURN(auto new_tablet, _tablet_manager->get_tablet(request.new_tablet_id));
    ASSIGN_OR_RETURN(auto base_schema, base_tablet.get_schema());
    ASSIGN_OR_RETURN(auto new_schema, new_tablet.get_schema());
    ASSIGN_OR_RETURN(auto has_delete_predicates, base_tablet.has_delete_predicates(alter_version));

    // parse request and create schema change params
    SchemaChangeParams sc_params;
    sc_params.base_tablet = &base_tablet;
    sc_params.new_tablet = &new_tablet;
    sc_params.chunk_changer = std::make_unique<ChunkChanger>(new_schema);
    sc_params.version = alter_version;
    sc_params.txn_id = request.txn_id;

    SchemaChangeUtils::init_materialized_params(request, &sc_params.materialized_params_map);
    RETURN_IF_ERROR(SchemaChangeUtils::parse_request(base_schema, new_schema, sc_params.chunk_changer.get(),
                                                     sc_params.materialized_params_map, has_delete_predicates,
                                                     &sc_params.sc_sorting, &sc_params.sc_directly, nullptr));

    // create txn log
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(new_tablet.id());
    txn_log->set_txn_id(request.txn_id);
    auto op_schema_change = txn_log->mutable_op_schema_change();
    op_schema_change->set_alter_version(alter_version);

    // convert historical rowsets
    RETURN_IF_ERROR(convert_historical_rowsets(sc_params, op_schema_change));

    // write txn log
    RETURN_IF_ERROR(new_tablet.put_txn_log(std::move(txn_log)));
    return Status::OK();
}

Status SchemaChangeHandler::process_update_tablet_meta(const TUpdateTabletMetaInfoReq& request) {
    if (!request.__isset.txn_id) {
        LOG(WARNING) << "txn_id not set in request";
        return Status::InternalError("txn_id not set in request");
    }
    int64_t txn_id = request.txn_id;

    for (const auto& tablet_meta_info : request.tabletMetaInfos) {
        RETURN_IF_ERROR(do_process_update_tablet_meta(tablet_meta_info, txn_id));
    }

    return Status::OK();
}

Status SchemaChangeHandler::do_process_update_tablet_meta(const TTabletMetaInfo& tablet_meta_info, int64_t txn_id) {
    if (tablet_meta_info.meta_type != TTabletMetaType::ENABLE_PERSISTENT_INDEX) {
        // Only support ENABLE_PERSISTENT_INDEX for now
        LOG(WARNING) << "not supported update meta type: " << tablet_meta_info.meta_type;
        return Status::InternalError(fmt::format("not supported update meta type: {}", tablet_meta_info.meta_type));
    }

    MonotonicStopWatch timer;
    timer.start();
    LOG(INFO) << "begin to update tablet, tablet: " << tablet_meta_info.tablet_id
              << ", update meta type: " << tablet_meta_info.meta_type;

    auto tablet_id = tablet_meta_info.tablet_id;
    ASSIGN_OR_RETURN(auto tablet, _tablet_manager->get_tablet(tablet_id));

    // create txn log
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(tablet_id);
    txn_log->set_txn_id(txn_id);
    auto op_alter_metadata = txn_log->mutable_op_alter_metadata();

    auto metadata_update_info = op_alter_metadata->add_metadata_update_infos();
    metadata_update_info->set_enable_persistent_index(tablet_meta_info.enable_persistent_index);

    LOG(INFO) << "update lake tablet: " << tablet_id
              << ", enable_persistent_index: " << tablet_meta_info.enable_persistent_index
              << ", cost: " << timer.elapsed_time();

    // write txn log
    RETURN_IF_ERROR(tablet.put_txn_log(std::move(txn_log)));
    return Status::OK();
}

Status SchemaChangeHandler::convert_historical_rowsets(const SchemaChangeParams& sc_params,
                                                       TxnLogPB_OpSchemaChange* op_schema_change) {
    auto base_tablet = sc_params.base_tablet;
    auto new_tablet = sc_params.new_tablet;
    auto alter_version = sc_params.version;
    LOG(INFO) << "begin to convert historical rowsets from base tablet to new tablet. "
              << "base tablet: " << base_tablet->id() << ", new tablet: " << new_tablet->id()
              << ", version: " << alter_version;

    // create schema change procedure
    std::unique_ptr<SchemaChange> sc_procedure;
    auto chunk_changer = sc_params.chunk_changer.get();
    if (sc_params.sc_sorting) {
        LOG(INFO) << "doing sorted schema change for base tablet: " << base_tablet->id();
        size_t memory_limitation =
                static_cast<size_t>(config::memory_limitation_per_thread_for_schema_change) * 1024 * 1024 * 1024;
        sc_procedure = std::make_unique<SortedSchemaChange>(_tablet_manager, sc_params.txn_id, base_tablet, new_tablet,
                                                            alter_version, chunk_changer, memory_limitation);
        op_schema_change->set_linked_segment(false);
    } else {
        // Note: In current implementation, linked schema change may refer to the segments deleted by gc,
        // so disable linked schema change and will support it in the later version.
        LOG(INFO) << "doing direct schema change for base tablet: " << base_tablet->id()
                  << ", params directly: " << sc_params.sc_directly;
        sc_procedure = std::make_unique<DirectSchemaChange>(_tablet_manager, sc_params.txn_id, base_tablet, new_tablet,
                                                            alter_version, chunk_changer);
        op_schema_change->set_linked_segment(false);
    }
    RETURN_IF_ERROR(sc_procedure->init());

    ASSIGN_OR_RETURN(auto base_metadata, base_tablet->get_metadata(alter_version));

    // convert rowsets
    ASSIGN_OR_RETURN(auto rowsets, base_tablet->get_rowsets(*base_metadata));
    for (const auto& rowset : rowsets) {
        auto st = sc_procedure->process(rowset, op_schema_change->add_rowsets());
        if (!st.ok()) {
            std::string err_msg =
                    fmt::format("failed to convert rowset. base tablet: {}, new tablet: {}, index: {}, status: {}",
                                base_tablet->id(), new_tablet->id(), rowset->index(), st.to_string());
            LOG(WARNING) << err_msg;
            return st;
        }
    }

    // no need to copy delete vector file any more
    // new tablet meta can refer existing delete vector file directly
    if (op_schema_change->linked_segment() && base_metadata->has_delvec_meta()) {
        op_schema_change->mutable_delvec_meta()->CopyFrom(base_metadata->delvec_meta());
    }

    LOG(INFO) << "finish convert historical rowsets from base tablet to new tablet. "
              << "base tablet: " << base_tablet->id() << ", new tablet: " << new_tablet->id()
              << ", version: " << alter_version;
    return Status::OK();
}

} // namespace starrocks::lake
