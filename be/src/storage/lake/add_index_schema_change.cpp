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

#include "storage/lake/add_index_schema_change.h"

#include <memory>

#include "agent/agent_server.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/nullable_column.h"
#include "column/raw_data_visitor.h"
#include "column/schema.h"
#include "common/config_agent_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/status.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gen_cpp/lake_types.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/index_file_writer.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/segment_task_runner.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/rowset/bloom_filter_index_writer.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_index.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "util/bloom_filter.h"

namespace starrocks::lake {

namespace {

// BinaryColumn / LargeBinaryColumn store string data as a bytes buffer plus
// an offsets array; BitmapIndexWriter expects a Slice* stride, so we
// materialize one ad-hoc for the requested row range.
template <typename BinaryT>
static void fill_slice_buffer(const BinaryT& bin, size_t start_row, size_t run_len, std::vector<Slice>* out) {
    out->resize(run_len);
    for (size_t i = 0; i < run_len; ++i) {
        (*out)[i] = bin.get_slice(start_row + i);
    }
}

// Walk a run of [start_row, start_row+run_len) in `col` and feed it to the
// given Writer (BitmapIndexWriter or BloomFilterIndexWriter; both share the
// add_values/add_nulls signature), replicating the ByteIterator-style
// pattern used by the standard ColumnWriter
// (be/src/storage/rowset/column_writer.cpp:815-840): contiguous NULL runs
// go through add_nulls(); non-null runs go through add_values() at
// type-sized strides. Fixed-length columns read via RawDataVisitor;
// variable-length string columns materialize a local Slice[] because the
// writer expects a Slice-stride array for string CppTypes.
template <typename Writer>
Status feed_index_from_column(Writer* writer, const Column& col, size_t start_row, size_t run_len, size_t type_size) {
    if (run_len == 0) return Status::OK();

    // Binary / LargeBinary: sidestep the raw_data flow entirely; we need a
    // Slice array anchored at start_row.
    std::vector<Slice> slice_buf;
    auto get_pdata = [&](const Column& data_col) -> StatusOr<const uint8_t*> {
        if (auto* bin = dynamic_cast<const BinaryColumn*>(&data_col); bin != nullptr) {
            DCHECK_EQ(type_size, sizeof(Slice));
            fill_slice_buffer(*bin, start_row, run_len, &slice_buf);
            return reinterpret_cast<const uint8_t*>(slice_buf.data());
        }
        if (auto* lbin = dynamic_cast<const LargeBinaryColumn*>(&data_col); lbin != nullptr) {
            DCHECK_EQ(type_size, sizeof(Slice));
            fill_slice_buffer(*lbin, start_row, run_len, &slice_buf);
            return reinterpret_cast<const uint8_t*>(slice_buf.data());
        }
        RawDataVisitor data_visitor;
        RETURN_IF_ERROR(data_col.accept(&data_visitor));
        // For fixed-length columns, offset by start_row here; for Slice path
        // the slice buffer is already anchored at start_row.
        return data_visitor.result() + start_row * type_size;
    };

    if (col.is_nullable()) {
        const auto& nc = down_cast<const NullableColumn&>(col);
        const auto* data_col = nc.data_column().get();
        const uint8_t* null_flags = nc.immutable_null_column_data().data() + start_row;
        ASSIGN_OR_RETURN(const uint8_t* pdata, get_pdata(*data_col));

        // Collapse contiguous runs of null / non-null values to minimize
        // per-call overhead into add_values / add_nulls.
        size_t i = 0;
        while (i < run_len) {
            bool is_null = null_flags[i] != 0;
            size_t j = i + 1;
            while (j < run_len && (null_flags[j] != 0) == is_null) {
                ++j;
            }
            size_t sub = j - i;
            if (is_null) {
                writer->add_nulls(static_cast<uint32_t>(sub));
            } else {
                writer->add_values(pdata + i * type_size, sub);
            }
            i = j;
        }
        return Status::OK();
    }

    ASSIGN_OR_RETURN(const uint8_t* pdata, get_pdata(col));
    writer->add_values(pdata, run_len);
    return Status::OK();
}

} // namespace

AddIndexSchemaChange::AddIndexSchemaChange(TabletManager* tablet_mgr, int64_t txn_id, VersionedTablet base_tablet,
                                           VersionedTablet new_tablet, std::vector<TabletIndexPB> indexes_to_build,
                                           int64_t alter_version)
        : _tablet_mgr(tablet_mgr),
          _txn_id(txn_id),
          _base_tablet(std::move(base_tablet)),
          _new_tablet(std::move(new_tablet)),
          _indexes_to_build(std::move(indexes_to_build)),
          _alter_version(alter_version) {}

AddIndexSchemaChange::~AddIndexSchemaChange() = default;

Status AddIndexSchemaChange::run(TxnLogPB_OpAddIndex* op_add_index) {
    DCHECK(op_add_index != nullptr);
    op_add_index->set_alter_version(_alter_version);
    for (const auto& ix : _indexes_to_build) {
        op_add_index->add_new_indexes()->CopyFrom(ix);
    }

    auto* exec_env = ExecEnv::GetInstance();
    if (exec_env == nullptr || exec_env->agent_server() == nullptr) {
        return Status::InternalError("AddIndexSchemaChange: ExecEnv or agent_server not available");
    }
    auto* pool = exec_env->agent_server()->get_lake_schema_change_thread_pool();
    SegmentTaskRunner runner(pool, config::lake_schema_change_per_tablet_parallelism);

    auto base_metadata = _base_tablet.metadata();
    if (base_metadata == nullptr) {
        return Status::InternalError("AddIndexSchemaChange: base tablet metadata is null");
    }

    for (const auto& rowset : base_metadata->rowsets()) {
        for (int seg_idx = 0; seg_idx < rowset.segments_size(); ++seg_idx) {
            uint32_t rssid = get_rssid(rowset, seg_idx);
            // Capture by value to keep task self-contained; the `rowset`
            // reference would dangle if we captured by reference and the
            // metadata got mutated during the run (e.g. defensive).
            RowsetMetadataPB rowset_copy = rowset;
            Status submit_st = runner.submit(
                    [this, rowset_copy = std::move(rowset_copy), seg_idx, rssid, op_add_index]() -> Status {
                        IndexDeltaGroupEntryPB entry;
                        RETURN_IF_ERROR(build_idg_for_segment(rowset_copy, seg_idx, rssid, &entry));
                        std::lock_guard<std::mutex> lg(_op_mtx);
                        auto* se = op_add_index->add_segment_entries();
                        se->set_segment_id(rssid);
                        se->mutable_entry()->Swap(&entry);
                        return Status::OK();
                    });
            if (!submit_st.ok()) {
                // Submission failure short-circuits: wait for pending tasks
                // to drain, then report the submit error (which takes
                // precedence over any task-level error we might collect).
                (void)runner.wait();
                return submit_st;
            }
        }
    }
    return runner.wait();
}

Status AddIndexSchemaChange::build_idg_for_segment(const RowsetMetadataPB& rowset_meta, uint32_t seg_idx_in_rowset,
                                                   uint32_t rssid, IndexDeltaGroupEntryPB* out_entry) {
    DCHECK(out_entry != nullptr);

    const auto& seg_name = rowset_meta.segments(seg_idx_in_rowset);
    // segments in rowset_meta carry relative filenames; resolve them against
    // the new tablet's segment location (same bucket/dir as the base tablet
    // on lake, since the fast path never copies or rewrites segment data).
    const std::string seg_path = _tablet_mgr->segment_location(_new_tablet.id(), seg_name);

    // 1. Open source Segment (metadata only — footer) to discover column
    //    positions and open column iterators. Reads go through data cache
    //    from this point (fill_data_cache=false to avoid cache pollution by
    //    this one-shot scan, mirroring schema_change.cpp's existing
    //    convention).
    FileInfo seg_fileinfo{.path = seg_path};
    if (seg_idx_in_rowset < rowset_meta.segment_encryption_metas_size()) {
        seg_fileinfo.encryption_meta = rowset_meta.segment_encryption_metas(seg_idx_in_rowset);
    }
    if (seg_idx_in_rowset < rowset_meta.segment_size_size()) {
        seg_fileinfo.size = rowset_meta.segment_size(seg_idx_in_rowset);
    }
    // Bundled rowsets pack multiple logical segments into one physical file;
    // without this offset the RandomAccessFile starts at byte 0 of the bundle
    // and column reads return the wrong bytes (observed as page checksum
    // mismatch on the first column read in the fast path).
    if (seg_idx_in_rowset < rowset_meta.bundle_file_offsets_size()) {
        seg_fileinfo.bundle_file_offset = rowset_meta.bundle_file_offsets(seg_idx_in_rowset);
    }
    size_t footer_size_hint = 16 * 1024;
    LakeIOOptions read_opts{.fill_data_cache = false};
    auto tablet_schema = _new_tablet.get_schema();
    ASSIGN_OR_RETURN(auto segment,
                     _tablet_mgr->load_segment(seg_fileinfo, seg_idx_in_rowset, &footer_size_hint, read_opts,
                                               /*fill_meta_cache*/ true, tablet_schema));

    // 2. Allocate the .idx file. Default WritableFileOptions leave
    //    skip_fill_local_cache=false so writes populate local cache,
    //    mirroring the DCG .cols behaviour — the first query after the
    //    alter hits cache and avoids a cold remote read.
    const std::string idx_filename = gen_idx_filename(_txn_id);
    const std::string idx_path = _tablet_mgr->segment_location(_new_tablet.id(), idx_filename);
    WritableFileOptions wopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta = std::move(pair.encryption_meta);
    }
    ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(wopts, idx_path));
    IndexFileWriter idx_writer(std::move(wfile));

    // 3. For each index to build, locate the column, dispatch to the
    //    per-type builder, and register the resulting meta with the
    //    IndexFileWriter. Unsupported types are a soft failure at this
    //    phase (NotSupported); the caller will abort the txn log and the
    //    .idx file will be garbage-collected as an orphan.
    for (const auto& ix : _indexes_to_build) {
        if (ix.col_unique_id_size() == 0) {
            return Status::InternalError("TabletIndex has no columns");
        }
        // Multi-column indexes (GIN, VECTOR) aren't supported in this
        // initial slice. BITMAP / NGRAMBF / bloom are single-column only.
        if (ix.col_unique_id_size() > 1 && ix.index_type() != IndexType::GIN) {
            return Status::NotSupported("multi-column non-GIN index unsupported");
        }
        int col_uid = ix.col_unique_id(0);
        int32_t col_ordinal = tablet_schema->field_index(col_uid);
        if (col_ordinal < 0) {
            return Status::InternalError(strings::Substitute("column with unique_id $0 not found in schema", col_uid));
        }
        const auto& column = tablet_schema->column(static_cast<size_t>(col_ordinal));

        ColumnIndexMetaPB meta;
        switch (ix.index_type()) {
        case IndexType::BITMAP:
            RETURN_IF_ERROR(build_bitmap_for_column(segment.get(), column, idx_writer.writable_file(), &meta));
            break;
        case IndexType::NGRAMBF:
            // NGRAMBF is built by BloomFilterIndexWriter with use_ngram=true.
            // No dedicated writer class; distinguishing info lives in
            // BloomFilterOptions and the resulting blob ends up in
            // ColumnIndexMetaPB.bloom_filter_index, same as plain bloom.
            RETURN_IF_ERROR(build_bloom_for_column(segment.get(), column, ix.index_type(), ix,
                                                   idx_writer.writable_file(), &meta));
            break;
        case IndexType::BLOOM_FILTER:
            // Plain (non-ngram) bloom filter. Shares the BloomFilterIndexWriter
            // path with NGRAMBF; `build_bloom_for_column` picks use_ngram
            // based on index_type. Triggered by the lake fast path for an
            // ALTER TABLE ... SET ("bloom_filter_columns"="...") that only
            // adds columns to the BF list.
            RETURN_IF_ERROR(build_bloom_for_column(segment.get(), column, ix.index_type(), ix,
                                                   idx_writer.writable_file(), &meta));
            break;
        case IndexType::GIN:
        case IndexType::VECTOR:
        default:
            return Status::NotSupported(strings::Substitute("lake ADD INDEX fast path: index type $0 not yet supported",
                                                            static_cast<int>(ix.index_type())));
        }
        idx_writer.append_column_index(col_uid, ix.index_type(), meta);
    }

    RETURN_IF_ERROR(idx_writer.finalize());

    // 4. Populate the IDG entry that the caller will hang off OpAddIndex.
    for (const auto& ix : _indexes_to_build) {
        auto* k = out_entry->add_keys();
        k->set_col_unique_id(ix.col_unique_id(0));
        k->set_index_type(ix.index_type());
    }
    out_entry->set_index_file(idx_filename);
    out_entry->set_version(_alter_version);
    if (!encryption_meta.empty()) {
        out_entry->set_encryption_meta(encryption_meta);
    }
    out_entry->set_file_size(static_cast<int64_t>(idx_writer.file_size()));
    return Status::OK();
}

Status AddIndexSchemaChange::build_bitmap_for_column(Segment* segment, const TabletColumn& column,
                                                     WritableFile* target_wfile, ColumnIndexMetaPB* out_meta) {
    if (segment == nullptr || target_wfile == nullptr || out_meta == nullptr) {
        return Status::InvalidArgument("build_bitmap_for_column: null argument");
    }
    auto type_info = get_type_info(column);
    std::unique_ptr<BitmapIndexWriter> bitmap_writer;
    RETURN_IF_ERROR(BitmapIndexWriter::create(type_info, &bitmap_writer));

    // Open a dedicated ColumnIterator on the segment. We intentionally do
    // not share iterators across indexes of the same column: each iterator
    // is cheap to open, and sharing would complicate the add_values /
    // add_nulls run accounting when multiple builders consume from the same
    // source.
    ASSIGN_OR_RETURN(auto col_iter, segment->new_column_iterator(column, /*path=*/nullptr));

    // Open a RandomAccessFile over the segment data for the column iterator.
    // _with_bundling() honors FileInfo.bundle_file_offset so reads land at the
    // right offset when the rowset packs multiple segments into one physical
    // object. Also propagate encryption_info from the already-opened Segment
    // so transparent decryption works end-to-end.
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(segment->file_name()));
    RandomAccessFileOptions raf_opts;
    if (auto enc = segment->encryption_info(); enc) {
        raf_opts.encryption_info = *enc;
    }
    ASSIGN_OR_RETURN(auto rfile, fs->new_random_access_file_with_bundling(raf_opts, segment->file_info()));

    OlapReaderStatistics stats;
    ColumnIteratorOptions col_iter_opts;
    col_iter_opts.read_file = rfile.get();
    col_iter_opts.stats = &stats;
    col_iter_opts.use_page_cache = false;
    col_iter_opts.lake_io_opts = {.fill_data_cache = false};
    col_iter_opts.is_nullable = column.is_nullable();
    col_iter_opts.reader_type = READER_ALTER_TABLE;
    RETURN_IF_ERROR(col_iter->init(col_iter_opts));
    RETURN_IF_ERROR(col_iter->seek_to_first());

    // Iterate the column in reasonable-sized batches. Chunk size mirrors
    // the repo-wide vector chunk default. For each batch we translate the
    // Column's layout into add_values / add_nulls calls and the writer
    // accumulates the bitmap in memory. No footer-based page index is
    // referenced while building: the bitmap is built purely from the raw
    // values.
    auto col = ChunkHelper::column_from_field_type(column.type(), column.is_nullable());
    constexpr size_t kBatch = 4096;
    const size_t type_size = type_info->size();
    while (true) {
        col->reset_column();
        size_t n = kBatch;
        Status st = col_iter->next_batch(&n, col.get());
        if (st.is_end_of_file() || n == 0) {
            break;
        }
        if (!st.ok()) return st;
        RETURN_IF_ERROR(feed_index_from_column(bitmap_writer.get(), *col, 0, n, type_size));
    }

    // Write the bitmap blob to the shared target file and emit the
    // ColumnIndexMetaPB that describes its in-file layout. The file offset
    // used by the blob is determined by target_wfile's current append
    // position; subsequent builders for other columns will be appended
    // after this one.
    RETURN_IF_ERROR(bitmap_writer->finish(target_wfile, out_meta));
    return Status::OK();
}

Status AddIndexSchemaChange::build_bloom_for_column(Segment* segment, const TabletColumn& column, IndexType index_type,
                                                    const TabletIndexPB& ix, WritableFile* target_wfile,
                                                    ColumnIndexMetaPB* out_meta) {
    if (segment == nullptr || target_wfile == nullptr || out_meta == nullptr) {
        return Status::InvalidArgument("build_bloom_for_column: null argument");
    }

    // Bloom / NGRAMBF options. `index_properties` on TabletIndexPB is a
    // serialized JSON map produced by TabletIndex::to_schema_pb (FE side).
    // Parse it through TabletIndex so we honor the same key names the
    // existing column-writer path consumes (bloom_filter_fpp, gram_num,
    // case_sensitive). Missing keys fall back to BloomFilterOptions
    // defaults so a minimal TabletIndexPB still produces a valid index.
    BloomFilterOptions bf_opts;
    if (index_type == IndexType::NGRAMBF) {
        bf_opts.use_ngram = true;
    }
    if (ix.has_index_properties() && !ix.index_properties().empty()) {
        TabletIndex tmp;
        if (auto st = tmp.init_from_pb(ix); st.ok()) {
            const auto& props = tmp.index_properties();
            auto get_str = [&](const std::string& key) -> std::string {
                auto it = props.find(key);
                return it == props.end() ? std::string() : it->second;
            };
            // Common spellings used across StarRocks DDL.
            std::string fpp_str = get_str("bloom_filter_fpp");
            if (fpp_str.empty()) fpp_str = get_str("fpp");
            if (!fpp_str.empty()) {
                try {
                    bf_opts.fpp = std::stod(fpp_str);
                } catch (const std::exception&) {
                    // Ignore malformed input; defaults remain in effect.
                }
            }
            std::string gram_str = get_str("gram_num");
            if (!gram_str.empty()) {
                try {
                    bf_opts.gram_num = static_cast<size_t>(std::stoul(gram_str));
                } catch (const std::exception&) {
                    // Ignore.
                }
            }
            std::string cs_str = get_str("case_sensitive");
            if (!cs_str.empty()) {
                bf_opts.case_sensitive = (cs_str == "true" || cs_str == "1");
            }
        }
    }
    if (bf_opts.use_ngram && bf_opts.gram_num == 0) {
        // gram_num is required when use_ngram is true; fall back to a sane
        // default rather than failing builder creation downstream.
        bf_opts.gram_num = 4;
    }

    auto type_info = get_type_info(column);
    std::unique_ptr<BloomFilterIndexWriter> bf_writer;
    RETURN_IF_ERROR(BloomFilterIndexWriter::create(bf_opts, type_info, &bf_writer));

    ASSIGN_OR_RETURN(auto col_iter, segment->new_column_iterator(column, /*path=*/nullptr));
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(segment->file_name()));
    ASSIGN_OR_RETURN(auto rfile, fs->new_random_access_file(segment->file_info()));

    OlapReaderStatistics stats;
    ColumnIteratorOptions col_iter_opts;
    col_iter_opts.read_file = rfile.get();
    col_iter_opts.stats = &stats;
    col_iter_opts.use_page_cache = false;
    col_iter_opts.lake_io_opts = {.fill_data_cache = false};
    col_iter_opts.is_nullable = column.is_nullable();
    col_iter_opts.reader_type = READER_ALTER_TABLE;
    RETURN_IF_ERROR(col_iter->init(col_iter_opts));
    RETURN_IF_ERROR(col_iter->seek_to_first());

    auto col = ChunkHelper::column_from_field_type(column.type(), column.is_nullable());
    constexpr size_t kBatch = 4096;
    const size_t type_size = type_info->size();
    while (true) {
        col->reset_column();
        size_t n = kBatch;
        Status st = col_iter->next_batch(&n, col.get());
        if (st.is_end_of_file() || n == 0) {
            break;
        }
        if (!st.ok()) return st;
        // BloomFilterIndexWriter shares the add_values/add_nulls signature
        // with BitmapIndexWriter; reuse the common feeder so Binary / string
        // columns get the Slice-buffer treatment automatically.
        RETURN_IF_ERROR(feed_index_from_column(bf_writer.get(), *col, 0, n, type_size));
        // BloomFilterIndexWriter accumulates per-page filters; flush at
        // chunk boundaries so memory stays bounded even for large columns.
        RETURN_IF_ERROR(bf_writer->flush());
    }

    RETURN_IF_ERROR(bf_writer->finish(target_wfile, out_meta));
    return Status::OK();
}

} // namespace starrocks::lake
