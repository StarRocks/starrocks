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

#include "storage/lake/vector_index_build_task.h"

#include <fmt/format.h>

#include <algorithm>
#include <set>

#include "column/chunk.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/index/vector/vector_index_builder.h"
#include "storage/index/vector/vector_index_builder_factory.h"
#include "storage/index/vector/vector_index_file_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

static constexpr int kDefaultMaxRowsetsPerBatch = 5;
static constexpr int kReadBatchSize = 4096;

VectorIndexBuildTask::VectorIndexBuildTask(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}

Status VectorIndexBuildTask::prepare(const BuildVectorIndexRequest& request) {
    // Reset task state so the same instance is safe to reuse across prepare() calls.
    _work_items.clear();
    _rowset_versions.clear();
    _tablet_schema.reset();
    _built_version = 0;
    _target_version = 0;

    _tablet_id = request.tablet_id();
    int64_t version = request.version();
    int batch_limit =
            request.has_max_rowsets_per_batch() ? request.max_rowsets_per_batch() : kDefaultMaxRowsetsPerBatch;
    if (batch_limit <= 0) {
        batch_limit = kDefaultMaxRowsetsPerBatch;
    }

    ASSIGN_OR_RETURN(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_id, version));
    _tablet_schema = TabletSchema::create(metadata->schema());
    int64_t metadata_bv = metadata->has_vector_index_built_version() ? metadata->vector_index_built_version() : 0;
    int64_t request_bv = request.has_built_version() ? request.built_version() : 0;
    _built_version = std::max(metadata_bv, request_bv);
    _target_version = version;

    LOG(INFO) << "VectorIndexBuildTask::prepare: tablet=" << _tablet_id << " version=" << version
              << " built_version=" << _built_version << " rowsets=" << metadata->rowsets_size();

    // Collect all rowset versions above built_version and identify candidates.
    struct CandidateInfo {
        const RowsetMetadataPB* rowset;
        int64_t version;
    };
    std::vector<CandidateInfo> candidates;

    for (const auto& rowset : metadata->rowsets()) {
        int64_t rv = rowset.has_version() ? rowset.version() : 0;
        if (rv <= _built_version) {
            continue;
        }
        bool has_vi = false;
        for (int i = 0; i < rowset.segment_metas_size(); i++) {
            if (rowset.segment_metas(i).vector_index_ids_size() > 0) {
                has_vi = true;
                break;
            }
        }
        if (has_vi) {
            candidates.push_back({&rowset, rv});
        }
        _rowset_versions.push_back({rv, has_vi, /*processed=*/false});
    }

    std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) { return a.version < b.version; });
    std::sort(_rowset_versions.begin(), _rowset_versions.end(),
              [](const auto& a, const auto& b) { return a.version < b.version; });

    // Take first batch_limit candidates and collect segment work items.
    int processed = 0;
    for (const auto& cand : candidates) {
        if (processed >= batch_limit) break;

        const auto& rowset = *cand.rowset;
        bool has_segment_size = (rowset.segment_size_size() == rowset.segments_size());
        bool has_bundle_offsets = (rowset.bundle_file_offsets_size() == rowset.segments_size());

        for (int seg_idx = 0; seg_idx < rowset.segments_size(); ++seg_idx) {
            if (seg_idx >= rowset.segment_metas_size() || rowset.segment_metas(seg_idx).vector_index_ids_size() == 0) {
                continue;
            }

            const auto& seg_name = rowset.segments(seg_idx);
            std::vector<int64_t> index_ids;
            for (int64_t idx_id : rowset.segment_metas(seg_idx).vector_index_ids()) {
                // Skip indexes whose .vi file already exists (partial retry recovery).
                // This only runs for segments in the current batch (bounded by
                // max_rowsets_per_batch), so S3 HEAD cost is minimal.
                std::string vi_name = gen_vector_index_filename(seg_name, idx_id);
                std::string vi_path = _tablet_mgr->segment_location(_tablet_id, vi_name);
                if (fs::path_exist(vi_path)) {
                    LOG(INFO) << "VectorIndexBuildTask: tablet=" << _tablet_id
                              << " skip existing .vi file: " << vi_name;
                    continue;
                }
                index_ids.push_back(idx_id);
            }
            if (index_ids.empty()) {
                continue;
            }

            std::string seg_path = _tablet_mgr->segment_location(_tablet_id, seg_name);
            FileInfo segment_file_info{.path = seg_path};
            if (has_segment_size) {
                segment_file_info.size = rowset.segment_size(seg_idx);
            }
            if (has_bundle_offsets) {
                segment_file_info.bundle_file_offset = rowset.bundle_file_offsets(seg_idx);
            }
            if (seg_idx < rowset.segment_encryption_metas_size()) {
                segment_file_info.encryption_meta = rowset.segment_encryption_metas(seg_idx);
            }

            _work_items.push_back({cand.version, std::move(segment_file_info), std::move(index_ids)});
        }

        // Mark this version as processed in _rowset_versions.
        for (auto& rv_info : _rowset_versions) {
            if (rv_info.version == cand.version) {
                rv_info.processed = true;
                break;
            }
        }
        processed++;
    }

    LOG(INFO) << "VectorIndexBuildTask::prepare: tablet=" << _tablet_id << " candidates=" << candidates.size()
              << " batch_limit=" << batch_limit << " segments=" << _work_items.size();

    return Status::OK();
}

Status VectorIndexBuildTask::build_one_segment(size_t work_index) {
    DCHECK_LT(work_index, _work_items.size());
    const auto& work = _work_items[work_index];
    LOG(INFO) << "VectorIndexBuildTask: tablet=" << _tablet_id << " building segment=" << work.segment_file_info.path
              << " version=" << work.rowset_version << " index_ids_count=" << work.index_ids.size();
    RETURN_IF_ERROR(build_segment(_tablet_id, work.segment_file_info, work.index_ids, _tablet_schema));
    LOG(INFO) << "VectorIndexBuildTask: tablet=" << _tablet_id << " built segment=" << work.segment_file_info.path
              << " successfully";
    return Status::OK();
}

int64_t VectorIndexBuildTask::compute_built_version(const std::vector<Status>& segment_results) const {
    // Find versions where any segment build failed.
    std::set<int64_t> failed_versions;
    for (size_t i = 0; i < _work_items.size() && i < segment_results.size(); i++) {
        if (!segment_results[i].ok()) {
            failed_versions.insert(_work_items[i].rowset_version);
        }
    }

    // Walk rowset versions in order. Advance watermark through:
    //   - no-vi rowsets (don't need building)
    //   - processed vi rowsets with all segments succeeded
    // Stop at first unprocessed vi rowset or failed vi rowset.
    int64_t watermark = _built_version;
    bool advanced_through_all = true;
    for (const auto& info : _rowset_versions) {
        if (!info.has_vi) {
            watermark = info.version;
        } else if (info.processed && failed_versions.count(info.version) == 0) {
            watermark = info.version;
        } else {
            advanced_through_all = false;
            break;
        }
    }

    if (advanced_through_all && _target_version > watermark) {
        watermark = _target_version;
    }
    return watermark;
}

Status VectorIndexBuildTask::execute(const BuildVectorIndexRequest& request, BuildVectorIndexResponse* response) {
    RETURN_IF_ERROR(prepare(request));

    std::vector<Status> results(_work_items.size());
    for (size_t i = 0; i < _work_items.size(); i++) {
        results[i] = build_one_segment(i);
        if (!results[i].ok()) {
            LOG(WARNING) << "VectorIndexBuildTask: tablet=" << _tablet_id << " segment[" << i
                         << "] failed: " << results[i];
            for (size_t j = i + 1; j < _work_items.size(); j++) {
                results[j] = Status::Cancelled("vector index segment build cancelled after earlier failure");
            }
            break; // Stop on first failure in sequential mode
        }
    }

    // compute_built_version advances through fully-built rowsets and stops at the first
    // rowset version that had a failed segment, so the watermark reflects exactly how
    // far this run got even on partial failure. The Status here only reflects whether
    // the task itself ran; FE infers per-segment failure from new_built_version not
    // reaching its target version and reschedules.
    response->set_new_built_version(compute_built_version(results));
    return Status::OK();
}

Status VectorIndexBuildTask::build_segment(int64_t tablet_id, const FileInfo& segment_file_info,
                                           const std::vector<int64_t>& index_ids,
                                           const TabletSchemaCSPtr& tablet_schema) {
    // Open the segment file (FileInfo carries size/encryption for proper access)
    ASSIGN_OR_RETURN(auto segment_fs, FileSystemFactory::CreateSharedFromString(segment_file_info.path));
    ASSIGN_OR_RETURN(auto segment, Segment::open(segment_fs, segment_file_info, 0, tablet_schema));

    // For each vector index, read column data and build .vi file
    for (int64_t index_id : index_ids) {
        // Find the column with this vector index
        std::shared_ptr<TabletIndex> tablet_index;
        int32_t col_unique_id = -1;
        for (uint32_t i = 0; i < tablet_schema->num_columns(); ++i) {
            const auto& col = tablet_schema->column(i);
            std::shared_ptr<TabletIndex> idx;
            if (tablet_schema->get_indexes_for_column(col.unique_id(), IndexType::VECTOR, idx).ok() && idx &&
                idx->index_id() == index_id) {
                tablet_index = idx;
                col_unique_id = col.unique_id();
                break;
            }
        }
        if (!tablet_index || col_unique_id < 0) {
            return Status::InternalError(fmt::format("vector index {} not found in schema", index_id));
        }

        // Find column index by unique_id
        int col_idx = -1;
        for (uint32_t i = 0; i < tablet_schema->num_columns(); ++i) {
            if (tablet_schema->column(i).unique_id() == col_unique_id) {
                col_idx = static_cast<int>(i);
                break;
            }
        }
        if (col_idx < 0) {
            return Status::InternalError(fmt::format("column with unique_id {} not found", col_unique_id));
        }

        const auto& column = tablet_schema->column(col_idx);

        // Create column iterator with encryption + bundling support
        ASSIGN_OR_RETURN(auto col_iter, segment->new_column_iterator(column, nullptr));
        RandomAccessFileOptions file_opts;
        if (!segment_file_info.encryption_meta.empty()) {
            ASSIGN_OR_RETURN(auto enc_info,
                             KeyCache::instance().unwrap_encryption_meta(segment_file_info.encryption_meta));
            file_opts.encryption_info = std::move(enc_info);
        }
        ASSIGN_OR_RETURN(auto read_file,
                         segment_fs->new_random_access_file_with_bundling(file_opts, segment_file_info));
        ColumnIteratorOptions iter_opts;
        iter_opts.read_file = read_file.get();
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        RETURN_IF_ERROR(col_iter->init(iter_opts));
        RETURN_IF_ERROR(col_iter->seek_to_first());

        // Build vector index
        // Extract segment filename from full path for .vi filename generation
        auto seg_basename = std::string_view(segment_file_info.path);
        if (auto pos = seg_basename.rfind('/'); pos != std::string_view::npos) {
            seg_basename = seg_basename.substr(pos + 1);
        }
        std::string vi_name = gen_vector_index_filename(seg_basename, index_id);
        std::string vi_path = _tablet_mgr->segment_location(tablet_id, vi_name);

        ASSIGN_OR_RETURN(auto builder_type,
                         VectorIndexBuilderFactory::get_index_builder_type_from_config(tablet_index));

#ifdef WITH_TENANN
        ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(vi_path));
        auto file_writer = std::make_unique<VectorIndexFileWriter>(std::move(wfile));
        ASSIGN_OR_RETURN(auto builder,
                         VectorIndexBuilderFactory::create_index_builder(tablet_index, vi_path, builder_type, true,
                                                                         _omp_threads, file_writer.get()));
#else
        ASSIGN_OR_RETURN(auto builder, VectorIndexBuilderFactory::create_index_builder(
                                               tablet_index, vi_path, builder_type, true, _omp_threads));
#endif
        RETURN_IF_ERROR(builder->init());

        // Read column data in batches and add to builder
        auto field = ChunkHelper::convert_field(col_idx, column);
        auto col_ptr = ChunkHelper::column_from_field(field);
        ordinal_t total_rows = col_iter->num_rows();
        ordinal_t rows_read = 0;

        while (rows_read < total_rows) {
            col_ptr->reset_column();
            size_t batch = std::min(static_cast<size_t>(kReadBatchSize), static_cast<size_t>(total_rows - rows_read));
            RETURN_IF_ERROR(col_iter->next_batch(&batch, col_ptr.get()));
            if (batch == 0) break;
            RETURN_IF_ERROR(builder->add(*col_ptr, rows_read));
            rows_read += batch;
        }

        RETURN_IF_ERROR(builder->flush());
        RETURN_IF_ERROR(builder->close());
    }

    return Status::OK();
}

} // namespace starrocks::lake
