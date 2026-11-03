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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/segment.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/segment.h"

#include <bvar/bvar.h>
#include <fmt/core.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite.h>

#include <algorithm>
#include <cstring>
#include <memory>

#include "base/failpoint/fail_point.h"
#include "base/hash/crc32c.h"
#include "base/string/slice.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "column/column_access_path.h"
#include "column/flat_json/json_flat_path.h"
#include "column/schema.h"
#include "common/bloom_filter.h"
#include "common/config_rowset_fwd.h"
#include "common/logging.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "platform/key_cache.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_env.h"
#include "segment_iterator.h"
#include "segment_options.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/cast_column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/json_column_iterator.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/scalar_column_iterator.h"
#include "storage/rowset/segment_writer.h" // k_segment_magic_length
#include "storage/storage_metrics.h"
#include "storage/tablet_schema.h"
#include "storage/utils.h"

bvar::Adder<int> g_open_segments;    // NOLINT
bvar::Adder<int> g_open_segments_io; // NOLINT
// How many segments been opened in the last 60 seconds
// NOLINTNEXTLINE
bvar::Window<bvar::Adder<int>> g_open_segments_minute("starrocks", "open_segments_minute", &g_open_segments, 60);
// How many I/O issued to open segment in the last 60 seconds
// NOLINTNEXTLINE
bvar::Window<bvar::Adder<int>> g_open_segments_io_minute("starrocks", "open_segments_io_minute", &g_open_segments_io,
                                                         60);
// How many ColumnReaders segments have built. A segment builds the reader of a column the first time the column is
// used, not when it is opened.
// NOLINTNEXTLINE
bvar::Adder<int64_t> g_segment_column_readers_built("starrocks", "segment_column_readers_built");

namespace starrocks {

#ifdef BE_TEST
bool Segment::_s_allow_batch_update_mode = true;
#endif

using strings::Substitute;

StatusOr<std::shared_ptr<Segment>> Segment::open(std::shared_ptr<FileSystem> fs, FileInfo segment_file_info,
                                                 uint32_t segment_id, std::shared_ptr<const TabletSchema> tablet_schema,
                                                 size_t* footer_length_hint,
                                                 const FooterPointerPB* partial_rowset_footer,
                                                 const LakeIOOptions& lake_io_opts,
                                                 lake::TabletManager* tablet_manager) {
    auto segment = std::make_shared<Segment>(std::move(fs), std::move(segment_file_info), segment_id,
                                             std::move(tablet_schema), tablet_manager);
    RETURN_IF_ERROR(segment->open(footer_length_hint, partial_rowset_footer, lake_io_opts));
    return std::move(segment);
}

// Reads the serialized SegmentFooterPB of |read_file| and verifies its checksum. On success |*footer| views the
// footer bytes, which are owned by |*buf|.
static StatusOr<size_t> read_segment_footer_internal(RandomAccessFile* read_file, std::string* buf,
                                                     std::string_view* footer, size_t* footer_length_hint,
                                                     const FooterPointerPB* partial_rowset_footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    ASSIGN_OR_RETURN(auto file_size, read_file->get_size());

    if (file_size < 12) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: file size $1 < 12", read_file->filename(), file_size));
    }

    size_t hint_size = footer_length_hint ? *footer_length_hint : 4096;
    size_t footer_read_size = std::min<size_t>(hint_size, file_size);

    if (partial_rowset_footer != nullptr) {
        if (file_size < partial_rowset_footer->position() + partial_rowset_footer->size()) {
            return Status::Corruption(
                    strings::Substitute("Bad partial segment file $0: file size $1 < $2", read_file->filename(),
                                        file_size, partial_rowset_footer->position() + partial_rowset_footer->size()));
        }
        footer_read_size = partial_rowset_footer->size();
    }
    std::string& buff = *buf;
    raw::stl_string_resize_uninitialized(&buff, footer_read_size);
    size_t read_pos = partial_rowset_footer ? partial_rowset_footer->position() : file_size - buff.size();

    RETURN_IF_ERROR(read_file->read_at_fully(read_pos, buff.data(), buff.size()));

    const uint32_t footer_length = UNALIGNED_LOAD32(buff.data() + buff.size() - 12);
    const uint32_t checksum = UNALIGNED_LOAD32(buff.data() + buff.size() - 8);
    const uint32_t magic_number = UNALIGNED_LOAD32(buff.data() + buff.size() - 4);

    // validate magic number
    if (magic_number != UNALIGNED_LOAD32(k_segment_magic)) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: magic number not match", read_file->filename()));
    }

    if (footer_length_hint != nullptr && footer_length > *footer_length_hint) {
        *footer_length_hint = footer_length + 128 /* allocate slightly more bytes next time*/;
    }

    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2", read_file->filename(),
                                                      file_size, 12 + footer_length));
    }

    buff.resize(buff.size() - 12); // Remove the last 12 bytes.

    if (footer_length <= buff.size()) {
        g_open_segments << 1;
        g_open_segments_io << 1;

        *footer = std::string_view(buff.data() + buff.size() - footer_length, footer_length);
    } else { // Need read file again.
        g_open_segments << 1;
        g_open_segments_io << 2;

        // |buff| holds the tail of the footer. Read the rest in front of it.
        size_t left_size = footer_length - buff.size();
        std::string whole;
        raw::stl_string_resize_uninitialized(&whole, footer_length);
        RETURN_IF_ERROR(read_file->read_at_fully(file_size - footer_length - 12, whole.data(), left_size));
        memcpy(whole.data() + left_size, buff.data(), buff.size());
        buff = std::move(whole);
        *footer = std::string_view(buff);
    }

    // validate footer PB's checksum
    uint32_t actual_checksum = crc32c::Value(footer->data(), footer->size());
    if (actual_checksum != checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                                    read_file->filename(), actual_checksum, checksum));
    }

    return footer_length + 12;
}

static StatusOr<size_t> read_segment_footer(RandomAccessFile* read_file, std::string* buf, std::string_view* footer,
                                            size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer) {
    auto s = read_segment_footer_internal(read_file, buf, footer, footer_length_hint, partial_rowset_footer);
    if (s.ok()) {
        return s;
    }
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (s.status().is_corruption()) {
        auto drop_status = drop_local_cache_data(read_file->filename());
        if (!drop_status.ok()) {
            return s; // return first read error
        }
        return read_segment_footer_internal(read_file, buf, footer, footer_length_hint, partial_rowset_footer);
    } else {
        return s;
    }
#else
    return s;
#endif
}

StatusOr<size_t> Segment::parse_segment_footer(RandomAccessFile* read_file, SegmentFooterPB* footer,
                                               size_t* footer_length_hint,
                                               const FooterPointerPB* partial_rowset_footer) {
    std::string buf;
    std::string_view footer_bytes;
    ASSIGN_OR_RETURN(auto size,
                     read_segment_footer(read_file, &buf, &footer_bytes, footer_length_hint, partial_rowset_footer));
    if (!footer->ParseFromArray(footer_bytes.data(), footer_bytes.size())) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: failed to parse footer", read_file->filename()));
    }
    return size;
}

// Parses a serialized SegmentFooterPB except its `columns`, which are returned as slices of |footer| in
// |column_metas| instead. Parsing the ColumnMetaPB of every column of a wide table costs more than
// everything else in opening a segment, and most of them may never be read.
static Status parse_footer_without_columns(std::string_view footer, SegmentFooterPB* footer_pb,
                                           std::vector<std::string_view>* column_metas) {
    using google::protobuf::internal::WireFormatLite;
    google::protobuf::io::CodedInputStream input(reinterpret_cast<const uint8_t*>(footer.data()), footer.size());
    std::string others;
    while (true) {
        const int start = input.CurrentPosition();
        const uint32_t tag = input.ReadTag();
        if (tag == 0) {
            // ReadTag() returns 0 at the end of the input, and for an invalid tag.
            if (start != static_cast<int>(footer.size())) {
                return Status::Corruption("invalid tag in segment footer");
            }
            break;
        }
        if (WireFormatLite::GetTagFieldNumber(tag) == SegmentFooterPB::kColumnsFieldNumber &&
            WireFormatLite::GetTagWireType(tag) == WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
            uint32_t length = 0;
            if (!input.ReadVarint32(&length) || length > footer.size() - input.CurrentPosition()) {
                return Status::Corruption("truncated column meta in segment footer");
            }
            column_metas->emplace_back(footer.substr(input.CurrentPosition(), length));
            input.Skip(length);
            continue;
        }
        if (!WireFormatLite::SkipField(&input, tag)) {
            return Status::Corruption("truncated field in segment footer");
        }
        others.append(footer.data() + start, input.CurrentPosition() - start);
    }
    if (!footer_pb->ParseFromString(others)) {
        return Status::Corruption("failed to parse segment footer");
    }
    return Status::OK();
}

// Returns the unique id in a serialized ColumnMetaPB without parsing the rest of it. As when parsing, the last
// occurrence wins and an absent field reads as 0.
static StatusOr<uint32_t> peek_column_unique_id(std::string_view column_meta) {
    using google::protobuf::internal::WireFormatLite;
    google::protobuf::io::CodedInputStream input(reinterpret_cast<const uint8_t*>(column_meta.data()),
                                                 column_meta.size());
    uint32_t unique_id = 0;
    while (true) {
        const int start = input.CurrentPosition();
        const uint32_t tag = input.ReadTag();
        if (tag == 0) {
            if (start != static_cast<int>(column_meta.size())) {
                return Status::Corruption("invalid tag in column meta");
            }
            return unique_id;
        }
        if (WireFormatLite::GetTagFieldNumber(tag) == ColumnMetaPB::kUniqueIdFieldNumber &&
            WireFormatLite::GetTagWireType(tag) == WireFormatLite::WIRETYPE_VARINT) {
            if (!input.ReadVarint32(&unique_id)) {
                return Status::Corruption("truncated unique id in column meta");
            }
        } else if (!WireFormatLite::SkipField(&input, tag)) {
            return Status::Corruption("truncated field in column meta");
        }
    }
}

Status Segment::write_segment_footer(WritableFile* write_file, const SegmentFooterPB& footer) {
    std::string footer_buf;
    if (!footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, footer_buf.size());
    // footer's checksum
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_segment_magic, k_segment_magic_length);

    Slice slices[2] = {footer_buf, fixed_buf};
    return write_file->appendv(&slices[0], 2);
}

Segment::Segment(std::shared_ptr<FileSystem> fs, FileInfo segment_file_info, uint32_t segment_id,
                 TabletSchemaCSPtr tablet_schema, lake::TabletManager* tablet_manager)
        : _fs(std::move(fs)),
          _segment_file_info(std::move(segment_file_info)),
          _tablet_schema(std::move(tablet_schema)),
          _segment_id(segment_id),
          _tablet_manager(tablet_manager) {
    MEM_TRACKER_SAFE_CONSUME(RuntimeEnv::GetInstance()->segment_metadata_mem_tracker(), _basic_info_mem_usage());
}

Segment::~Segment() {
    for (auto& column : _column_readers) {
        delete column.reader.load(std::memory_order_acquire);
    }
    MEM_TRACKER_SAFE_RELEASE(RuntimeEnv::GetInstance()->segment_metadata_mem_tracker(),
                             _basic_info_mem_usage() + _column_meta_mem_usage);
    MEM_TRACKER_SAFE_RELEASE(RuntimeEnv::GetInstance()->short_key_index_mem_tracker(), _short_key_index_mem_usage());
}

// Adds what |file| has read to |stats|, under the fields SegmentIterator::_update_stats fills for the column files.
static void add_read_file_io_stats(const RandomAccessFile& file, OlapReaderStatistics* stats) {
    const auto io = file.get_io_stats_snapshot();
    stats->compressed_bytes_read_local_disk += io.bytes_read_local_disk;
    stats->compressed_bytes_read_remote += io.bytes_read_remote;
    stats->compressed_bytes_read += io.bytes_read_local_disk + io.bytes_read_remote;
    stats->io_count_local_disk += io.io_count_local_disk;
    stats->io_count_remote += io.io_count_remote;
    stats->io_count += io.io_count_local_disk + io.io_count_remote;
    stats->io_ns_read_local_disk += io.io_ns_read_local_disk;
    stats->io_ns_remote += io.io_ns_read_remote;
}

Status Segment::open(size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer,
                     const LakeIOOptions& lake_io_opts, OlapReaderStatistics* stats) {
    if (invoked(_open_once)) {
        return Status::OK();
    }

    auto res = success_once(_open_once,
                            [&] { return _open(footer_length_hint, partial_rowset_footer, lake_io_opts, stats); });
    if (res.status().is_not_found()) {
        StorageMetrics::instance()->segment_file_not_found_total.increment(1);
    }

    // move the cache size update out of the `success_once`,
    // so that the onceflag `_open_once` can be set before the cache_size is updated.
    if (res.ok() && *res) {
        update_cache_size();
    }
    return res.status();
}

Status Segment::_open(size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer,
                      const LakeIOOptions& lake_io_opts, OlapReaderStatistics* stats) {
    std::string footer_buf;
    std::string_view footer_bytes;
    RandomAccessFileOptions opts{.skip_fill_local_cache = !lake_io_opts.fill_data_cache,
                                 .buffer_size = lake_io_opts.buffer_size};

    if (!_segment_file_info.encryption_meta.empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(_segment_file_info.encryption_meta));
        opts.encryption_info = std::move(info);
        _encryption_info = std::make_unique<FileEncryptionInfo>(opts.encryption_info);
    }

    ASSIGN_OR_RETURN(auto read_file, _fs->new_random_access_file_with_bundling(opts, _segment_file_info));
    {
        const int64_t footer_start_ns = stats != nullptr ? MonotonicNanos() : 0;
        DeferOp record_footer_reads([&] {
            if (stats != nullptr) {
                stats->io_ns += MonotonicNanos() - footer_start_ns;
                add_read_file_io_stats(*read_file, stats);
            }
        });
        RETURN_IF_ERROR(read_segment_footer(read_file.get(), &footer_buf, &footer_bytes, footer_length_hint,
                                            partial_rowset_footer));
    }
    if (stats != nullptr) {
        stats->segments_opened++;
    }
    SegmentFooterPB footer;
    std::vector<std::string_view> column_metas;
    if (auto st = parse_footer_without_columns(footer_bytes, &footer, &column_metas); !st.ok()) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: $1", _segment_file_info.path, st.message()));
    }
    RETURN_IF_ERROR(_create_column_readers(column_metas));
    _num_rows = footer.num_rows();
    _short_key_index_page = PagePointer(footer.short_key_index_page());
    _skip_vector_index =
            footer.has_vector_index_storage_type() && footer.vector_index_storage_type() == VECTOR_INDEX_STORAGE_NONE;
    return Status::OK();
}

bool Segment::_use_segment_zone_map_filter(const SegmentReadOptions& read_options) {
    if (read_options.dcg_loader == nullptr) {
        return true;
    }
    SCOPED_RAW_TIMER(&read_options.stats->get_delta_column_group_ns);
    Status st;
    DeltaColumnGroupList dcgs;
    if (read_options.is_primary_keys) {
        TabletSegmentId tsid;
        tsid.tablet_id = read_options.tablet_id;
        tsid.segment_id = read_options.rowset_id + _segment_id;
        st = read_options.dcg_loader->load(tsid, read_options.version, &dcgs);
    } else {
        int64_t tablet_id = read_options.tablet_id;
        RowsetId rowsetid = read_options.rowsetid;
        uint32_t segment_id = _segment_id;
        st = read_options.dcg_loader->load(tablet_id, rowsetid, segment_id, INT64_MAX, &dcgs);
    }

    return st.ok() && dcgs.size() == 0;
}

struct SegmentZoneMapPruner {
    bool operator()(const PredicateColumnNode& node) const {
        const auto* col_pred = node.col_pred();
        const ColumnId column_id = col_pred->column_id();
        const auto& tablet_column = read_options.tablet_schema ? read_options.tablet_schema->column(column_id)
                                                               : parent->_tablet_schema->column(column_id);
        const auto column_unique_id = tablet_column.unique_id();

        const auto* column = parent->_find_column(column_unique_id);
        if (column == nullptr) {
            return false;
        }
        auto reader = parent->_get_or_create_column_reader(*column);
        if (!reader.ok()) {
            // Not pruning is always safe. The scan reads the column next and reports the error then.
            return false;
        }
        return (*reader)->has_zone_map() && !(*reader)->segment_zone_map_filter({col_pred}) &&
               (tablet_column.is_key() || parent->_use_segment_zone_map_filter(read_options));
    }
    bool operator()(const PredicateAndNode& node) const {
        return std::any_of(node.children().begin(), node.children().end(),
                           [this](const auto& child) { return child.visit(*this); });
    }
    bool operator()(const PredicateOrNode& node) const {
        return !node.empty() && std::all_of(node.children().begin(), node.children().end(),
                                            [this](const auto& child) { return child.visit(*this); });
    }

    Segment* parent;
    const SegmentReadOptions& read_options;
};

Status Segment::_prune_by_segment_zone_map(const SegmentReadOptions& read_options) {
    SCOPED_RAW_TIMER(&read_options.stats->segment_zone_map_filter_ns);
    const auto pruned = config::enable_index_segment_level_zonemap_filter &&
                        read_options.pred_tree_for_zone_map.visit(SegmentZoneMapPruner{this, read_options});
    if (pruned) {
        if (read_options.is_first_split_of_segment) {
            read_options.stats->segment_stats_filtered += num_rows();
        }
        return Status::EndOfFile(strings::Substitute("End of file $0, empty iterator", _segment_file_info.path));
    }
    return Status::OK();
}

StatusOr<ChunkIteratorPtr> Segment::_new_iterator(const Schema& schema, const SegmentReadOptions& read_options) {
    DCHECK(read_options.stats != nullptr);

    RETURN_IF_ERROR(_prune_by_segment_zone_map(read_options));
    return new_segment_iterator(shared_from_this(), schema, read_options);
}

StatusOr<ChunkIteratorPtr> Segment::new_iterator(const Schema& schema, const SegmentReadOptions& read_options) {
    if (read_options.stats == nullptr) {
        return Status::InvalidArgument("stats is null pointer");
    }
    return _new_iterator(schema, read_options);
}

StatusOr<ChunkIteratorPtr> Segment::new_reusable_iterator(const Schema& iterator_schema, const Schema& output_schema,
                                                          const SegmentReadOptions& read_options,
                                                          ChunkIteratorPtr* reusable_slot) {
    if (read_options.stats == nullptr) {
        return Status::InvalidArgument("stats is null pointer");
    }
    RETURN_IF_ERROR(_prune_by_segment_zone_map(read_options));
    return new_reusable_segment_iterator(shared_from_this(), iterator_schema, output_schema, read_options,
                                         reusable_slot);
}

Status Segment::new_inverted_index_iterator(uint32_t ucid, InvertedIndexIterator** iter, const SegmentReadOptions& opts,
                                            const IndexReadOptions& index_opt) {
    const auto* column = _find_column(ucid);

    if (column != nullptr) {
        std::shared_ptr<TabletIndex> index_meta;
        RETURN_IF_ERROR(_tablet_schema->get_indexes_for_column(ucid, GIN, index_meta));
        if (index_meta.get() != nullptr) {
            ASSIGN_OR_RETURN(auto reader, _get_or_create_column_reader(*column));
            return reader->new_inverted_index_iterator(index_meta, iter, opts, index_opt);
        }
    }
    return Status::OK();
}

Status Segment::load_index(const LakeIOOptions& lake_io_opts, OlapReaderStatistics* stats) {
    auto res = success_once(_load_index_once, [&] {
        SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);

        Status st = _load_index(lake_io_opts, stats);
        if (st.ok()) {
            const auto index_mem_usage = _short_key_index_mem_usage();
            MEM_TRACKER_SAFE_CONSUME(RuntimeEnv::GetInstance()->short_key_index_mem_tracker(), index_mem_usage);
            _loaded_key_index_mem_usage.store(index_mem_usage, std::memory_order_relaxed);
            update_cache_size();
        } else {
            _reset();
        }
        return st;
    });
    return res.status();
}

StatusOr<std::unique_ptr<RandomAccessFile>> Segment::new_segment_read_file(const LakeIOOptions& lake_io_opts) {
    // Apply the segment file's encryption info + bundling offset (like _open/_load_index) so reads land at
    // the right offset on bundled/encrypted segments. Don't cache into _encryption_info here (OnceFlag owns it).
    RandomAccessFileOptions file_opts{.skip_fill_local_cache = !lake_io_opts.fill_data_cache,
                                      .buffer_size = lake_io_opts.buffer_size};
    if (_encryption_info) {
        file_opts.encryption_info = *_encryption_info;
    } else if (!_segment_file_info.encryption_meta.empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(_segment_file_info.encryption_meta));
        file_opts.encryption_info = std::move(info);
    }
    return _fs->new_random_access_file_with_bundling(file_opts, _segment_file_info);
}

Status Segment::_load_index(const LakeIOOptions& lake_io_opts, OlapReaderStatistics* stats) {
    // read and parse short key index page
    RandomAccessFileOptions file_opts{.skip_fill_local_cache = !lake_io_opts.fill_data_cache,
                                      .buffer_size = lake_io_opts.buffer_size};
    if (_encryption_info) {
        file_opts.encryption_info = *_encryption_info;
    } else if (!_segment_file_info.encryption_meta.empty()) {
        ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(_segment_file_info.encryption_meta));
        file_opts.encryption_info = std::move(info);
        _encryption_info = std::make_unique<FileEncryptionInfo>(file_opts.encryption_info);
    }
    ASSIGN_OR_RETURN(auto read_file, _fs->new_random_access_file_with_bundling(file_opts, _segment_file_info));

    PageReadOptions opts;
    opts.use_page_cache = lake_io_opts.use_page_cache;
    opts.read_file = read_file.get();
    opts.page_pointer = _short_key_index_page;
    opts.codec = nullptr; // short key index page uses NO_COMPRESSION for now
    OlapReaderStatistics tmp_stats;
    opts.stats = stats != nullptr ? stats : &tmp_stats;

    Slice body;
    PageFooterPB footer;
    DeferOp record_index_reads([&] { add_read_file_io_stats(*read_file, opts.stats); });
    RETURN_IF_ERROR(PageIO::read_and_decompress_page(opts, &_sk_index_handle, &body, &footer));

    DCHECK_EQ(footer.type(), SHORT_KEY_PAGE);
    DCHECK(footer.has_short_key_page_footer());

    _sk_index_decoder = std::make_unique<ShortKeyIndexDecoder>();
    return _sk_index_decoder->parse(body, footer.short_key_page_footer());
}

void Segment::_reset() {
    _sk_index_handle.reset();
    _sk_index_decoder.reset();
}

bool Segment::has_loaded_index() const {
    return invoked(_load_index_once);
}

Status Segment::_create_column_readers(const std::vector<std::string_view>& column_metas) {
    std::unordered_map<uint32_t, uint32_t> column_id_to_footer_ordinal;
    RETURN_IF_ERROR(_check_column_unique_id_uniqueness(column_metas, column_id_to_footer_ordinal));

    struct Slot {
        int32_t unique_id;
        std::string_view meta;
    };
    std::vector<Slot> slots;
    size_t metas_size = 0;
    for (uint32_t ordinal = 0, sz = _tablet_schema->num_columns(); ordinal < sz; ++ordinal) {
        const auto& column = _tablet_schema->column(ordinal);
        auto iter = column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == column_id_to_footer_ordinal.end()) {
            continue;
        }
        slots.push_back({column.unique_id(), column_metas[iter->second]});
        metas_size += column_metas[iter->second].size();
    }
    std::sort(slots.begin(), slots.end(), [](const Slot& a, const Slot& b) { return a.unique_id < b.unique_id; });

    // Keep only the metas of the columns this segment can read through its schema. The columns of a
    // historical schema are not all of the footer.
    std::string metas;
    metas.reserve(metas_size);
    std::vector<LazyColumnReader> columns(slots.size());
    for (size_t i = 0; i < slots.size(); ++i) {
        columns[i].unique_id = slots[i].unique_id;
        columns[i].meta_offset = metas.size();
        metas.append(slots[i].meta);
    }
    _column_metas = std::move(metas);
    _column_readers = std::move(columns);
    _column_meta_mem_usage = _column_metas.capacity() + _column_readers.capacity() * sizeof(LazyColumnReader);
    MEM_TRACKER_SAFE_CONSUME(RuntimeEnv::GetInstance()->segment_metadata_mem_tracker(), _column_meta_mem_usage);
    return Status::OK();
}

const Segment::LazyColumnReader* Segment::_find_column(int32_t uid) const {
    auto it = std::lower_bound(_column_readers.begin(), _column_readers.end(), uid,
                               [](const LazyColumnReader& c, int32_t id) { return c.unique_id < id; });
    return (it != _column_readers.end() && it->unique_id == uid) ? &*it : nullptr;
}

std::string_view Segment::_column_meta(const LazyColumnReader& column) const {
    const size_t end = &column + 1 == _column_readers.data() + _column_readers.size() ? _column_metas.size()
                                                                                      : (&column + 1)->meta_offset;
    return std::string_view(_column_metas).substr(column.meta_offset, end - column.meta_offset);
}

StatusOr<ColumnReader*> Segment::_get_or_create_column_reader(const LazyColumnReader& column) const {
    if (auto* reader = column.reader.load(std::memory_order_acquire); reader != nullptr) {
        return reader;
    }
    {
        std::lock_guard l(_column_readers_lock);
        if (auto* reader = column.reader.load(std::memory_order_relaxed); reader != nullptr) {
            return reader;
        }
        // Like the short key index, the reader is shared by every user of this segment and must not be left
        // half-built because the query that happens to build it is over its memory limit.
        SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
        ColumnMetaPB meta;
        const auto serialized_meta = _column_meta(column);
        if (!meta.ParseFromArray(serialized_meta.data(), serialized_meta.size())) {
            return Status::Corruption(strings::Substitute("Bad segment file $0: failed to parse meta of column $1",
                                                          _segment_file_info.path, column.unique_id));
        }
        int32_t field_index = _tablet_schema->field_index(column.unique_id);
        if (field_index < 0) {
            // The unique id index can miss a column of the schema: the hidden row-store column appended by
            // TabletSchema::_build_current_tablet_schema() is not in it.
            for (size_t i = 0, n = _tablet_schema->num_columns(); i < n; ++i) {
                if (_tablet_schema->column(i).unique_id() == column.unique_id) {
                    field_index = static_cast<int32_t>(i);
                    break;
                }
            }
        }
        if (field_index < 0) {
            return Status::InternalError(strings::Substitute("Segment $0 has no schema column with unique id $1",
                                                             _segment_file_info.path, column.unique_id));
        }
        ASSIGN_OR_RETURN(auto reader,
                         ColumnReader::create(&meta, const_cast<Segment*>(this), &_tablet_schema->column(field_index)));
        column.reader.store(reader.release(), std::memory_order_release);
        g_segment_column_readers_built << 1;
    }
    // The reader adds to mem_usage(), which the metacache charges for this segment.
    const_cast<Segment*>(this)->update_cache_size();
    return column.reader.load(std::memory_order_acquire);
}

StatusOr<const ColumnReader*> Segment::column_with_uid(int32_t uid) const {
    const auto* column = _find_column(uid);
    if (column == nullptr) {
        return nullptr;
    }
    return _get_or_create_column_reader(*column);
}

DEFINE_FAIL_POINT(ingest_duplicate_column_unique_id);
Status Segment::_check_column_unique_id_uniqueness(
        const std::vector<std::string_view>& column_metas,
        std::unordered_map<uint32_t, uint32_t>& column_id_to_footer_ordinal) {
    // check uniqueness of column ids in footer
    for (uint32_t ordinal = 0, sz = column_metas.size(); ordinal < sz; ++ordinal) {
        ASSIGN_OR_RETURN(auto unique_id, peek_column_unique_id(column_metas[ordinal]));
        auto [it, ok] = column_id_to_footer_ordinal.emplace(unique_id, ordinal);
        if (UNLIKELY(!ok)) {
            LOG(ERROR) << "Duplicate column id=" << unique_id << " found between column #" << it->second
                       << " and column #" << ordinal << " of segment " << _segment_file_info.path;
            return Status::InternalError("Duplicate column id");
        }
    }

    // check uniqueness of column ids in tablet schema
    std::unordered_map<uint32_t, uint32_t> column_id_to_tablet_schema_ordinal;
    FAIL_POINT_TRIGGER_EXECUTE(ingest_duplicate_column_unique_id,
                               { column_id_to_tablet_schema_ordinal.emplace(1, 2); });

    for (uint32_t ordinal = 0, sz = _tablet_schema->num_columns(); ordinal < sz; ++ordinal) {
        const auto& column = _tablet_schema->column(ordinal);
        auto [it, ok] = column_id_to_tablet_schema_ordinal.emplace(column.unique_id(), ordinal);
        if (UNLIKELY(!ok)) {
            LOG(ERROR) << "Duplicate column id=" << column.unique_id() << " found between column '"
                       << _tablet_schema->column(it->second).name() << "' and column '" << column.name()
                       << "' in tablet schema";
            return Status::InternalError("Duplicate column id found in tablet schema");
        }
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<ColumnIterator>> Segment::new_column_iterator_or_default(const TabletColumn& column,
                                                                                  ColumnAccessPath* path) {
    auto id = column.unique_id();
    if (const auto* lazy_reader = _find_column(id); lazy_reader != nullptr) {
        ASSIGN_OR_RETURN(auto reader, _get_or_create_column_reader(*lazy_reader));
        ASSIGN_OR_RETURN(auto source_iter, reader->new_iterator(path, &column));
        bool need_cast = false;
        if (reader->column_type() != column.type()) {
            need_cast = true;
        } else if (column.type() == TYPE_CHAR) {
            // if the column is a char column, we need to check if the length of the column is the same as
            // the length stored in the segment footer (the original length when the segment was written).
            // Because the char column is padded with 0 to the length of the tablet column and the storage
            // bitmap index and zone map need it.
            // In shared-data mode, segments may be opened with a new tablet schema that has a different
            // CHAR length than what's stored on disk, so we must compare against the segment's stored length.
            int32_t segment_column_length = reader->column_length();
            if (segment_column_length != column.length()) {
                need_cast = true;
            }
        }
        if (!need_cast) {
            return source_iter;
        } else {
            auto nullable = reader->is_nullable();
            auto source_type = TypeDescriptor::from_logical_type(reader->column_type());
            auto target_type = TypeDescriptor::from_logical_type(column.type(), column.precision(), column.scale());
            return std::make_unique<CastColumnIterator>(std::move(source_iter), source_type, target_type, nullable);
        }
    }
    if (column.is_extended()) {
        return _new_extended_column_iterator(column, path);
    }

    if (!column.has_default_value() && !column.is_nullable()) {
        return Status::InternalError(
                fmt::format("invalid nonexistent column({}) without default value.", column.name()));
    } else {
        const TypeInfoPtr& type_info = get_type_info(column);
        auto default_value_iter = std::make_unique<DefaultValueColumnIterator>(
                column.has_default_value(), column.default_value(), column.is_nullable(), type_info, column.length(),
                num_rows(), path);
        return default_value_iter;
    }
}

StatusOr<ColumnIteratorUPtr> Segment::_new_extended_column_iterator(const TabletColumn& column,
                                                                    ColumnAccessPath* path) {
    auto extended_info = column.extended_info();
    RETURN_IF(extended_info == nullptr, Status::InvalidArgument("extended info is null"));
    auto access_path = extended_info->access_path;
    RETURN_IF(access_path == nullptr, Status::InvalidArgument("access path is null for extended column"));

    // Resolve the root column by unique id to be robust across schema changes
    auto full_path = access_path->linear_path();
    auto [col_name, field_name] = JsonFlatPath::split_path(full_path);

    int32_t source_uid = extended_info->source_column_uid;
    int32_t source_index = _tablet_schema->field_index(source_uid);
    if (source_index < 0 || static_cast<size_t>(source_index) >= _tablet_schema->num_columns()) {
        // The root column does not exist in this segment (e.g., added by schema change later).
        // Fall back to column default/nullability.
        const TypeInfoPtr& type_info = get_type_info(column);
        auto default_iter = std::make_unique<DefaultValueColumnIterator>(column.has_default_value(),
                                                                         column.default_value(), column.is_nullable(),
                                                                         type_info, column.length(), num_rows());
        VLOG(2) << "root column '" << col_name << "' not found in segment, return default for path: " << full_path;
        return default_iter;
    }
    RETURN_IF(access_path == nullptr, Status::InvalidArgument("access path is null for extended column"));

    auto source_id = _tablet_schema->column(static_cast<size_t>(source_index)).unique_id();
    auto& leaf_type = access_path->leaf_value_type();
    const auto* lazy_source_reader = _find_column(source_id);
    RETURN_IF(lazy_source_reader == nullptr, Status::RuntimeError(fmt::format("unknown root column: {}", source_id)));
    ASSIGN_OR_RETURN(auto column_reader, _get_or_create_column_reader(*lazy_source_reader));

    // Check if it's a sub-column of FlatJSON
    // case 1: it's not a FlatJSON
    // case 2: it's a FlatJSON, but it's not a flatten column in the FlatJSON
    auto sub_readers = column_reader->sub_readers();
    if (sub_readers) {
        for (auto& sub_reader : *sub_readers) {
            if (field_name == sub_reader->name()) {
                auto source_iter = std::make_unique<ScalarColumnIterator>(sub_reader.get());
                LogicalType reader_type = sub_reader.get()->column_type();
                VLOG(2) << fmt::format(
                        "create extended_column_iterator for full_path={} field={} reader_type={}, expected_type={}",
                        full_path, field_name, reader_type, column.type());

                if (reader_type == column.type()) {
                    return source_iter;
                } else {
                    auto nullable = sub_reader->is_nullable();
                    auto source_type = TypeDescriptor::from_logical_type(reader_type);
                    auto target_type =
                            TypeDescriptor::from_logical_type(column.type(), column.precision(), column.scale());
                    return std::make_unique<CastColumnIterator>(std::move(source_iter), source_type, target_type,
                                                                nullable);
                }
            }
        }
    }

    // case 3: check if this segment contains the specific field
    bool may_contains = column_reader->has_remain_json();
    if (may_contains && column_reader->get_remain_filter() != nullptr) {
        std::vector<std::string> paths = strings::Split(full_path, ".");
        std::string_view leaf = paths.back();
        may_contains = column_reader->get_remain_filter()->test_bytes(leaf.data(), leaf.size());
    }
    // An intermediate node (e.g. "o") whose descendants are stored as flattened sub-columns
    // (e.g. "o.inner") is present in this segment even though no sub-column is named exactly
    // "o". Such a node must be reconstructed by the JsonExtractIterator below, not reported as
    // absent -- otherwise extracting an intermediate JSON object through a json-path-rewrite
    // extended column (e.g. get_json_string(j, '$.o')) wrongly returns NULL.
    bool has_flatten_descendant = false;
    if (sub_readers) {
        const std::string prefix = std::string(field_name) + ".";
        for (auto& sub_reader : *sub_readers) {
            if (std::string_view(sub_reader->name()).starts_with(prefix)) {
                has_flatten_descendant = true;
                break;
            }
        }
    }
    if (column_reader->is_flat_json() && !may_contains && !has_flatten_descendant) {
        // create an iterator always return NULL for fields that don't exist in this segment
        auto default_null_iter = std::make_unique<DefaultValueColumnIterator>(false, "", true, get_type_info(column),
                                                                              column.length(), num_rows());
        VLOG(2) << "json field " << full_path << " not found in segment, return NULL directly";
        return default_null_iter;
    }

    // Build a regular JsonExtractIterator to read it
    ASSIGN_OR_RETURN(auto source_iter, column_reader->new_iterator(path, &column));
    return create_json_extract_iterator(std::move(source_iter), column_reader->is_nullable(), std::string(field_name),
                                        leaf_type.type);
}

StatusOr<std::unique_ptr<ColumnIterator>> Segment::new_column_iterator(const TabletColumn& column,
                                                                       ColumnAccessPath* path) {
    auto id = column.unique_id();
    if (const auto* lazy_reader = _find_column(id); lazy_reader != nullptr) {
        ASSIGN_OR_RETURN(auto reader, _get_or_create_column_reader(*lazy_reader));
        ASSIGN_OR_RETURN(auto source_iter, reader->new_iterator(path, &column));
        if (reader->column_type() == column.type()) {
            return source_iter;
        } else {
            auto nullable = reader->is_nullable();
            auto source_type = TypeDescriptor::from_logical_type(reader->column_type());
            auto target_type = TypeDescriptor::from_logical_type(column.type(), column.precision(), column.scale());
            return std::make_unique<CastColumnIterator>(std::move(source_iter), source_type, target_type, nullable);
        }
    } else {
        return Status::NotFound(fmt::format("{} does not contain column of id {}", _segment_file_info.path, id));
    }
}

Status Segment::new_bitmap_index_iterator(ColumnUID id, const IndexReadOptions& options, BitmapIndexIterator** res) {
    const auto* lazy_reader = _find_column(id);
    if (lazy_reader == nullptr) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto reader, _get_or_create_column_reader(*lazy_reader));
    // Delegate to ColumnReader: it handles both the footer-embedded bitmap
    // (legacy path) and the IDG-backed sidecar (.idx file, lake fast path).
    // Do NOT gate on `has_bitmap_index()` here — that only checks the
    // footer-loaded `_bitmap_index` pointer and would short-circuit before
    // the IDG branch gets a chance. ColumnReader::new_bitmap_index_iterator
    // returns OK with `*res == nullptr` when neither source has a bitmap.
    return reader->new_bitmap_index_iterator(options, res);
}

StatusOr<std::shared_ptr<Segment>> Segment::new_dcg_segment(const DeltaColumnGroup& dcg, uint32_t idx,
                                                            const TabletSchemaCSPtr& read_tablet_schema) {
    std::shared_ptr<TabletSchema> tablet_schema;
    if (read_tablet_schema != nullptr) {
        tablet_schema = TabletSchema::create_with_uid(read_tablet_schema, dcg.column_ids()[idx]);
    } else {
        tablet_schema = TabletSchema::create_with_uid(_tablet_schema.schema(), dcg.column_ids()[idx]);
    }
    ASSIGN_OR_RETURN(auto filepath, dcg.column_file_by_idx(parent_name(_segment_file_info.path), idx));
    FileInfo info{.path = filepath};
    // Supplying the known size lets the segment open path skip a stat/HeadObject. A size of 0 means
    // unknown (old data lacking column_file_sizes), in which case we fall back to discovering it.
    if (idx < dcg.column_file_sizes().size() && dcg.column_file_sizes()[idx] > 0) {
        info.size = dcg.column_file_sizes()[idx];
    }
    if (idx < dcg.encryption_metas().size()) {
        info.encryption_meta = dcg.encryption_metas()[idx];
    }
    return Segment::open(_fs, info, 0, tablet_schema, nullptr);
}

Status Segment::get_short_key_index(std::vector<std::string>* sk_index_values) {
    LakeIOOptions lakeIoOptions{.fill_data_cache = false, .buffer_size = -1};
    RETURN_IF_ERROR(load_index(lakeIoOptions));
    for (size_t i = 0; i < _sk_index_decoder->num_items(); i++) {
        sk_index_values->emplace_back(_sk_index_decoder->key(i).to_string());
    }
    return Status::OK();
}

size_t Segment::_column_index_mem_usage() const {
    size_t size = 0;
    for (const auto& column : _column_readers) {
        if (const auto* reader = column.reader.load(std::memory_order_acquire); reader != nullptr) {
            size += reader->mem_usage();
        }
    }
    return size;
}

void Segment::turn_off_batch_update_cache_size() {
#ifdef BE_TEST
    if (!_s_allow_batch_update_mode) return;
#endif
    if (_batch_on_flags_counter.fetch_sub(1, std::memory_order_relaxed) == 1) {
        if (_dirty_cache_counter.exchange(0) > 0) {
            if (_tablet_manager != nullptr) {
                auto mem_cost = mem_usage();
                // Use FileInfo::cache_key() so this matches the key load_segment registered under;
                // a path-only key would miss for bundled slices (non-zero bundle_file_offset) and
                // their cache entries would never get the post-open memory cost, defeating
                // metacache capacity control.
                _tablet_manager->update_segment_cache_size(_segment_file_info.cache_key(), mem_cost, this);
            }
        }
    }
}

void Segment::update_cache_size() {
    if (_tablet_manager != nullptr) {
        // could be race condition on this `_batch_on_flags_counter` check, but it is ok to be inaccurate in such case.
        if (_batch_on_flags_counter.load(std::memory_order_relaxed) == 0) {
            auto mem_cost = mem_usage();
            _tablet_manager->update_segment_cache_size(_segment_file_info.cache_key(), mem_cost, this);
        } else {
            // under batch mode, only increase the _dirty_cache_counter
            _dirty_cache_counter.fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        // Only used by share-nothing rowsets. The last reader release consumes this flag.
        _lazy_mem_update.store(true, std::memory_order_release);
    }
}

size_t Segment::mem_usage() const {
    if (!invoked(_open_once)) {
        // just report the basic info memory usage if not opened yet
        return _basic_info_mem_usage();
    }
    return _basic_info_mem_usage() + _column_meta_mem_usage +
           _loaded_key_index_mem_usage.load(std::memory_order_relaxed) + _column_index_mem_usage();
}

StatusOr<int64_t> Segment::get_data_size() const {
    if (_segment_file_info.size.has_value()) {
        return _segment_file_info.size.value();
    }
    return _fs->get_file_size(_segment_file_info.path);
}

} // namespace starrocks
