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

#include "storage/lake/compaction_task.h"

#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_compaction_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h" // config::enable_transparent_data_encryption
#include "common/config_storage_fwd.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/delta_column_group.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/filenames.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/primary_key_compaction_policy.h"
#include "storage/lake/rowset.h"
#include "storage/lake/sdcg_overlay_merge.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/utils.h"

namespace starrocks::lake {

CompactionTask::CompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                               CompactionTaskContext* context, std::shared_ptr<const TabletSchema> tablet_schema)
        : _txn_id(context->txn_id),
          _tablet(std::move(tablet)),
          _input_rowsets(std::move(input_rowsets)),
          _mem_tracker(std::make_unique<MemTracker>(MemTrackerType::COMPACTION_TASK, -1,
                                                    "Compaction-" + std::to_string(_tablet.metadata()->id()),
                                                    GlobalEnv::GetInstance()->compaction_mem_tracker())),
          _context(context),
          _tablet_schema(std::move(tablet_schema)) {}

Status CompactionTask::execute_index_major_compaction(TxnLogPB* txn_log) {
    if (_tablet.get_schema()->keys_type() == KeysType::PRIMARY_KEYS) {
        SCOPED_RAW_TIMER(&_context->stats->pk_sst_merge_ns);
        auto metadata = _tablet.metadata();
        if (metadata->enable_persistent_index() &&
            metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE) {
            // For parallel compaction subtasks, skip SST compaction here.
            // SST compaction will be executed once after all subtasks complete,
            // in TabletParallelCompactionManager::get_merged_txn_log.
            // This avoids multiple subtasks competing to compact the same SST files.
            if (_context->subtask_id >= 0) {
                return Status::OK();
            }
            RETURN_IF_ERROR(_tablet.tablet_manager()->update_mgr()->execute_index_major_compaction(metadata, txn_log));
            if (txn_log->has_op_compaction() && !txn_log->op_compaction().input_sstables().empty()) {
                size_t total_input_sstable_file_size = 0;
                for (const auto& input_sstable : txn_log->op_compaction().input_sstables()) {
                    total_input_sstable_file_size += input_sstable.filesize();
                }
                _context->stats->input_file_size += total_input_sstable_file_size;
            }
            return Status::OK();
        }
    }
    return Status::OK();
}

StatusOr<bool> CompactionTask::try_execute_dcg_overlay_merge() {
    // ---- 1. Gate: lake PK tablet with a DCG, single non-overlapped input rowset, deep sparse chain. ----
    if (_tablet_schema->keys_type() != KeysType::PRIMARY_KEYS) {
        return false;
    }
    const auto& metadata = _tablet.metadata();
    if (metadata->dcg_meta().dcgs().empty()) {
        return false;
    }
    if (_input_rowsets.size() != 1) {
        return false;
    }
    const auto& rowset = _input_rowsets[0];
    const RowsetMetadataPB& rowset_meta = rowset->metadata();
    if (rowset_meta.overlapped()) {
        return false;
    }
    const int64_t eval_depth = max_sparse_chain_depth_for_rowset(rowset_meta, metadata->dcg_meta());
    LOG(INFO) << fmt::format(
            "sdcg overlay-merge eval: tablet {} rowset {} segs {} dcgs {} depth {} (trigger {}) compact_version {}",
            _tablet.id(), rowset_meta.id(), rowset_meta.segment_metas_size(), metadata->dcg_meta().dcgs().size(),
            eval_depth, SDCG_COMPACTION_TRIGGER_DEPTH, metadata->version());
    if (eval_depth < SDCG_COMPACTION_TRIGGER_DEPTH) {
        return false;
    }

    // ---- 2. Snapshot version the merge reads at (mirrors OpCompaction.compact_version). ----
    const int64_t compact_version = metadata->version();

    // ---- 3. Load base segments (footer cache + data cache filled; the base column data is NOT read). ----
    LakeIOOptions io_opts;
    io_opts.fill_data_cache = true;
    io_opts.fill_metadata_cache = true;
    ASSIGN_OR_RETURN(auto base_segments, rowset->segments(io_opts));

    // FileSystem for opening the `.spcols` overlay files for read and the merged file for write. The lake
    // metadata root location is the common ancestor of both (segment_location() resolves against it).
    Tablet tablet(_tablet.tablet_manager(), _tablet.id());
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(tablet.metadata_root_location()));

    // ---- 4. Build the txn log skeleton. ----
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet.id());
    txn_log->set_txn_id(_txn_id);
    auto* op = txn_log->mutable_op_dcg_compaction();
    op->set_compact_version(compact_version);

    // ---- 5. Per base segment: collect + merge its sparse overlay layers, write one packed `.spcols`. ----
    for (int pos = 0; pos < rowset_meta.segment_metas_size(); ++pos) {
        const uint32_t rssid = get_rssid(rowset_meta, pos);
        auto dcg_it = metadata->dcg_meta().dcgs().find(rssid);
        if (dcg_it == metadata->dcg_meta().dcgs().end()) {
            LOG(INFO) << fmt::format("sdcg overlay-merge skip seg: tablet {} pos {} rssid {} no-dcg", _tablet.id(), pos,
                                     rssid);
            continue;
        }
        const DeltaColumnGroupVerPB& ver = dcg_it->second;

        // v1 EXCLUSION: inline patches are `.spcols`-equivalent overlays carried in metadata (no file to
        // read+rewrite), so a segment with any inline patch is left unmerged (always correct -- the reader
        // keeps walking the original chain). Encryption IS supported end-to-end: the read path unwraps each
        // layer's per-file encryption_meta, the writer re-encrypts the merged `.spcols` under the current
        // KEK, and the apply side carries the merged file's encryption_meta.
        if (ver.inline_patches_size() > 0) {
            LOG(INFO) << fmt::format("sdcg overlay-merge skip seg: tablet {} rssid {} inline_patches={}",
                                     _tablet.id(), rssid, ver.inline_patches_size());
            continue;
        }
        bool skip_segment = false;
        int sparse_layers = 0;
        for (int i = 0; i < ver.column_files_size(); ++i) {
            // A layer newer than the snapshot raced the merge: we MUST NOT fold it (the apply side only
            // drops versions in merged_versions, all <= compact_version). Leave the whole segment unmerged.
            const int64_t v = (i < ver.versions_size()) ? ver.versions(i) : 0;
            if (v > compact_version) {
                skip_segment = true;
                break;
            }
            // Only plain SPARSE_PERCOL overlays are foldable; a DENSE_COLS layer means a row-complete
            // rewrite already happened -- defer to normal compaction for that segment.
            const DeltaColumnFileKindPB kind = (i < ver.file_kinds_size()) ? ver.file_kinds(i) : DENSE_COLS;
            if (kind != SPARSE_PERCOL) {
                skip_segment = true;
                break;
            }
            // PACKED (flexible / per-row heterogeneous) sparse files carry per-column placeholders gated by
            // a per-column roaring; v1 CN does not fold these. Leave the segment unmerged.
            if (i < ver.column_presence_lists_size() && ver.column_presence_lists(i).entries_size() > 0) {
                skip_segment = true;
                break;
            }
            ++sparse_layers;
        }
        if (skip_segment) {
            LOG(INFO) << fmt::format(
                    "sdcg overlay-merge skip seg: tablet {} rssid {} unfoldable-layer (dense/packed/raced)",
                    _tablet.id(), rssid);
            continue;
        }
        // Need at least 2 layers to collapse; a single sparse layer is already minimal.
        if (sparse_layers < 2) {
            LOG(INFO) << fmt::format("sdcg overlay-merge skip seg: tablet {} rssid {} sparse_layers={}", _tablet.id(),
                                     rssid, sparse_layers);
            continue;
        }

        if (pos >= static_cast<int>(base_segments.size()) || base_segments[pos] == nullptr) {
            LOG(INFO) << fmt::format("sdcg overlay-merge skip seg: tablet {} pos {} base_segments={} null-or-oob",
                                     _tablet.id(), pos, base_segments.size());
            continue;
        }
        SegmentPtr base_segment = base_segments[pos];

        // Load the DeltaColumnGroup view for this segment (gives column_ids() per file, for the read schema
        // built inside new_sparse_dcg_segment). The loader keys on rssid and the snapshot version.
        DeltaColumnGroupList dcgs;
        LakeDeltaColumnGroupLoader loader(metadata);
        RETURN_IF_ERROR(loader.load(TabletSegmentId(_tablet.id(), rssid), compact_version, &dcgs));
        if (dcgs.empty()) {
            continue;
        }
        const auto& dcg = dcgs[0];

        // Read each foldable sparse layer's source_rowid + value columns into an OverlaySparseLayer.
        std::vector<OverlaySparseLayer> layers;
        std::vector<int64_t> merged_versions;
        for (int i = 0; i < ver.column_files_size(); ++i) {
            const DeltaColumnFileKindPB kind = (i < ver.file_kinds_size()) ? ver.file_kinds(i) : DENSE_COLS;
            if (kind != SPARSE_PERCOL) {
                continue; // (excluded above; defensive)
            }
            const int64_t layer_version = (i < ver.versions_size()) ? ver.versions(i) : 0;

            // Open the `.spcols` as a Segment whose read schema = the dcg entry's value columns + a
            // synthetic source_rowid column (uid == kSDCGSourceRowidUid). Pass the base tablet schema so
            // the value columns are resolved by uid.
            ASSIGN_OR_RETURN(auto spcols_seg, base_segment->new_sparse_dcg_segment(*dcg, i, _tablet_schema, io_opts,
                                                                                   /*fill_meta_cache=*/true));
            const int64_t k = static_cast<int64_t>(spcols_seg->num_rows());

            // Open a read file for this `.spcols` (its own physical file), honoring its encryption meta.
            RandomAccessFileOptions ropts;
            if (spcols_seg->encryption_info() != nullptr && !spcols_seg->file_info().encryption_meta.empty()) {
                ASSIGN_OR_RETURN(auto info,
                                 KeyCache::instance().unwrap_encryption_meta(spcols_seg->file_info().encryption_meta));
                ropts.encryption_info = std::move(info);
            }
            ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file_with_bundling(ropts, spcols_seg->file_info()));

            OlapReaderStatistics stats;
            ColumnIteratorOptions iter_opts;
            iter_opts.read_file = read_file.get();
            iter_opts.stats = &stats;
            iter_opts.use_page_cache = false;

            // Read every column of the `.spcols` read schema: the source_rowid column (uid ==
            // kSDCGSourceRowidUid) gives the K ascending base ordinals; the rest are value columns (their
            // schema order is the read schema's, which we mirror 1:1 into column_uids/values).
            const TabletSchema& sp_schema = spcols_seg->tablet_schema();
            std::vector<uint32_t> source_rowids;
            std::vector<int32_t> column_uids;
            std::vector<ColumnPtr> values;
            bool have_source_rowids = false;
            for (size_t c = 0; c < sp_schema.num_columns(); ++c) {
                const TabletColumn& col = sp_schema.column(c);
                ASSIGN_OR_RETURN(auto col_iter, spcols_seg->new_column_iterator(col, nullptr));
                RETURN_IF_ERROR(col_iter->init(iter_opts));
                RETURN_IF_ERROR(col_iter->seek_to_first());
                if (col.unique_id() == static_cast<int32_t>(kSDCGSourceRowidUid)) {
                    auto holder = Int64Column::create();
                    auto remaining = static_cast<size_t>(k);
                    while (remaining > 0) {
                        size_t n = remaining;
                        RETURN_IF_ERROR(col_iter->next_batch(&n, holder.get()));
                        if (n == 0) break;
                        remaining -= n;
                    }
                    if (static_cast<int64_t>(holder->size()) != k) {
                        return Status::Corruption(fmt::format("sdcg merge: source_rowid count {} != K {} in {}",
                                                              holder->size(), k, spcols_seg->file_name()));
                    }
                    source_rowids.resize(k);
                    const auto& data = holder->immutable_data();
                    for (int64_t r = 0; r < k; ++r) {
                        source_rowids[r] = static_cast<uint32_t>(data[r]);
                    }
                    have_source_rowids = true;
                } else {
                    auto value_col = ChunkFactory::column_from_field_type(col.type(), col.is_nullable());
                    auto remaining = static_cast<size_t>(k);
                    while (remaining > 0) {
                        size_t n = remaining;
                        RETURN_IF_ERROR(col_iter->next_batch(&n, value_col.get()));
                        if (n == 0) break;
                        remaining -= n;
                    }
                    if (static_cast<int64_t>(value_col->size()) != k) {
                        return Status::Corruption(fmt::format("sdcg merge: value count {} != K {} (uid {}) in {}",
                                                              value_col->size(), k, col.unique_id(),
                                                              spcols_seg->file_name()));
                    }
                    column_uids.push_back(static_cast<int32_t>(col.unique_id()));
                    values.emplace_back(std::move(value_col));
                }
            }
            if (!have_source_rowids) {
                return Status::Corruption(
                        fmt::format("sdcg merge: `.spcols` {} missing source_rowid column", spcols_seg->file_name()));
            }

            OverlaySparseLayer layer;
            layer.version = layer_version;
            layer.column_uids = std::move(column_uids);
            layer.source_rowids = std::move(source_rowids);
            layer.values = std::move(values);
            layers.push_back(std::move(layer));
            merged_versions.push_back(layer_version);
        }

        if (layers.size() < 2) {
            continue; // nothing to collapse for this segment
        }

        ASSIGN_OR_RETURN(auto merged, merge_overlay_layers(layers));

        // ---- Build the packed sparse schema (source_rowid + value columns in merged.column_uids order). ----
        TabletColumn source_rowid_col(STORAGE_AGGREGATE_NONE, TYPE_BIGINT, /*is_nullable=*/false,
                                      static_cast<int32_t>(kSDCGSourceRowidUid), sizeof(int64_t));
        source_rowid_col.set_name("__sdcg_source_rowid");
        source_rowid_col.set_is_key(false);
        std::vector<TabletColumn> sparse_cols;
        sparse_cols.reserve(1 + merged.column_uids.size());
        sparse_cols.emplace_back(std::move(source_rowid_col));
        for (int32_t uid : merged.column_uids) {
            const int32_t cidx = _tablet_schema->field_index(uid);
            if (cidx < 0) {
                return Status::InternalError(
                        fmt::format("sdcg merge: merged column uid {} not found in tablet schema", uid));
            }
            sparse_cols.emplace_back(_tablet_schema->column(cidx));
        }
        auto sparse_tschema = TabletSchema::copy(*_tablet_schema, sparse_cols);

        // ---- Assemble the packed chunk: column 0 = source_rowid, then value columns in column_uids order. ----
        Columns chunk_columns;
        chunk_columns.reserve(1 + merged.value_columns.size());
        chunk_columns.emplace_back(std::move(merged.source_rowid_column));
        for (auto& vc : merged.value_columns) {
            chunk_columns.emplace_back(std::move(vc));
        }
        auto chunk_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(sparse_tschema));
        auto chunk = std::make_shared<Chunk>(std::move(chunk_columns), chunk_schema);

        // ---- Write ONE packed `.spcols`, mirroring _prepare_sparse_delta_column_group_writer. ----
        const std::string path = tablet.segment_location(gen_spcols_filename(_txn_id));
        WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        SegmentWriterOptions writer_options;
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            wopts.encryption_info = pair.info;
            writer_options.encryption_meta = std::move(pair.encryption_meta);
        }
        ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(wopts, path));
        auto sw = std::make_unique<SegmentWriter>(std::move(wfile), 0, sparse_tschema, writer_options);
        RETURN_IF_ERROR(sw->init(false));
        RETURN_IF_ERROR(sw->append_chunk(*chunk));
        uint64_t fsize = 0, isize = 0, fpos = 0;
        RETURN_IF_ERROR(sw->finalize(&fsize, &isize, &fpos));
        const std::string filename = file_name(sw->segment_path());

        // ---- Emit the DCG compaction entry for this segment. ----
        auto* e = op->add_entries();
        e->set_rssid(rssid);
        e->mutable_merged_file()->set_name(filename);
        e->mutable_merged_file()->set_size(static_cast<int64_t>(fsize));
        if (!sw->encryption_meta().empty()) {
            e->mutable_merged_file()->set_encryption_meta(sw->encryption_meta());
        }
        for (int32_t uid : merged.column_uids) {
            e->add_column_uids(static_cast<uint32_t>(uid));
        }
        e->mutable_presence()->set_min_source_rowid(merged.min_source_rowid);
        e->mutable_presence()->set_max_source_rowid(merged.max_source_rowid);
        e->mutable_presence()->set_row_count(merged.num_rows);
        for (const auto& p : merged.presences) {
            auto* cpe = e->mutable_column_presence()->add_entries();
            cpe->set_column_uid(static_cast<uint32_t>(p.column_uid));
            cpe->set_min_source_rowid(p.min_source_rowid);
            cpe->set_max_source_rowid(p.max_source_rowid);
            cpe->set_count(p.count);
            cpe->set_roaring(p.roaring);
        }
        e->set_source_segment_num_rows(ver.source_segment_num_rows());
        for (int64_t v : merged_versions) {
            e->add_merged_versions(v);
        }

        LOG(INFO) << fmt::format(
                "SDCG overlay merge: tablet {} rssid {} folded {} sparse layers -> {} (K={}, cols={}), txn_id {}",
                _tablet.id(), rssid, layers.size(), filename, merged.num_rows, merged.column_uids.size(), _txn_id);
    }

    // ---- 6. Nothing merged => fall through to normal compaction. Else emit the txn log. ----
    if (op->entries_size() == 0) {
        LOG(INFO) << fmt::format("sdcg overlay-merge: tablet {} produced no entries -> normal compaction",
                                 _tablet.id());
        return false;
    }
    if (_context->skip_write_txnlog) {
        _context->txn_log = txn_log;
    } else {
        RETURN_IF_ERROR(_tablet.tablet_manager()->put_txn_log(txn_log));
    }
    return true;
}

Status CompactionTask::fill_compaction_segment_info(TxnLogPB_OpCompaction* op_compaction, TabletWriter* writer) {
    for (auto& rowset : _input_rowsets) {
        op_compaction->add_input_rowsets(rowset->id());
    }

    // check last rowset whether this is a partial compaction
    if (_tablet_schema->keys_type() != KeysType::PRIMARY_KEYS && _input_rowsets.size() > 0 &&
        _input_rowsets.back()->partial_segments_compaction()) {
        uint64_t uncompacted_num_rows = 0;
        uint64_t uncompacted_data_size = 0;
        RETURN_IF_ERROR(_input_rowsets.back()->add_partial_compaction_segments_info(
                op_compaction, writer, uncompacted_num_rows, uncompacted_data_size));
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows() + uncompacted_num_rows);
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size() + uncompacted_data_size);
        op_compaction->mutable_output_rowset()->set_overlapped(true);
    } else {
        op_compaction->set_new_segment_offset(0);
        for (const auto& file : writer->segments()) {
            uint32_t segment_idx = op_compaction->output_rowset().segment_metas_size();
            file.to_proto(segment_idx, op_compaction->mutable_output_rowset()->add_segment_metas());
        }
        op_compaction->set_new_segment_count(writer->segments().size());
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows());
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size());
        op_compaction->mutable_output_rowset()->set_overlapped(false);
        op_compaction->mutable_output_rowset()->set_next_compaction_offset(0);
        for (auto& sst : writer->ssts()) {
            to_file_meta_pb(sst, op_compaction->add_ssts());
        }
        for (auto& sst_range : writer->sst_ranges()) {
            op_compaction->add_sst_ranges()->CopyFrom(sst_range);
        }
        // Record lcrm file metadata in transaction log if it exists
        // WHY: During parallel pk index execution, mapper files are stored on remote storage
        // (.lcrm extension). Recording metadata in txn log enables:
        // 1. Light publish optimization - skip re-reading compaction data during publish
        // 2. Performance - file size avoids expensive S3/HDFS get_size() calls
        // 3. Lifecycle management - metadata tracked for proper GC cleanup
        // CONSTRAINT: Only applies to remote storage files (.lcrm), not local files (.crm)
        if (is_lcrm(writer->lcrm_file().path)) {
            to_file_meta_pb(writer->lcrm_file(), op_compaction->mutable_lcrm_file());
        }
    }
    // Fresh uid: a compaction output is a new logical rowset and must not dedup
    // with the input rowsets it supersedes (it leaves their split family). Use
    // set_ (always re-mint), not ensure_ (mint-if-absent): the output is derived
    // from inputs, so it must never alias an input's uid even if one is ever
    // copied in. Matches tablet_parallel_compaction_manager's merged output.
    tablet_reshard_helper::set_rowset_uid(op_compaction->mutable_output_rowset());
    return Status::OK();
}

CompactionTask::SstStats CompactionTask::compute_sst_stats(const std::vector<FileInfo>& writer_ssts,
                                                           const TxnLogPB* txn_log) {
    SstStats stats;
    // SST output from eager PK index build
    stats.output_files = static_cast<int32_t>(writer_ssts.size());
    for (const auto& sst : writer_ssts) {
        stats.output_bytes += sst.size.value_or(0);
    }
    // SST input/output from PK index major compaction
    if (txn_log != nullptr && txn_log->has_op_compaction()) {
        const auto& op = txn_log->op_compaction();
        stats.input_files = op.input_sstables_size();
        for (const auto& sst : op.input_sstables()) {
            stats.input_bytes += sst.filesize();
        }
        for (const auto& sst : op.output_sstables()) {
            stats.output_files += 1;
            stats.output_bytes += sst.filesize();
        }
        if (op.has_output_sstable()) {
            stats.output_files += 1;
            stats.output_bytes += op.output_sstable().filesize();
        }
    }
    return stats;
}

void CompactionTask::collect_sst_stats(const TabletWriter* writer, const TxnLogPB* txn_log) {
    auto stats = compute_sst_stats(writer->ssts(), txn_log);
    _sst_input_files = stats.input_files;
    _sst_input_bytes = stats.input_bytes;
    _sst_output_files = stats.output_files;
    _sst_output_bytes = stats.output_bytes;
}

bool CompactionTask::should_enable_pk_index_eager_build(int64_t input_bytes) {
    if (_tablet.get_schema()->keys_type() != KeysType::PRIMARY_KEYS) {
        return false;
    }
    // Eager PK index build only works when all conditions are met:
    // 1. whether use cloud native index
    // 2. whether use light compaction publish
    // 3. whether input_bytes is large enough
    auto metadata = _tablet.metadata();
    bool use_cloud_native_index = metadata->enable_persistent_index() &&
                                  metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE;
    bool use_light_compaction_publish = config::enable_light_pk_compaction_publish &&
                                        StorageEngine::instance()->get_persistent_index_store(_tablet.id()) != nullptr;
    return use_cloud_native_index && use_light_compaction_publish &&
           input_bytes >= config::pk_index_eager_build_threshold_bytes;
}

} // namespace starrocks::lake
