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
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <memory>

#include "column/column_access_path.h"
#include "column/schema.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "segment_iterator.h"
#include "segment_options.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/segment_writer.h" // k_segment_magic_length
#include "storage/tablet_schema.h"
#include "storage/type_utils.h"
#include "storage/utils.h"
#include "util/crc32c.h"
#include "util/slice.h"

bvar::Adder<int> g_open_segments;    // NOLINT
bvar::Adder<int> g_open_segments_io; // NOLINT
// How many segments been opened in the last 60 seconds
// NOLINTNEXTLINE
bvar::Window<bvar::Adder<int>> g_open_segments_minute("starrocks", "open_segments_minute", &g_open_segments, 60);
// How many I/O issued to open segment in the last 60 seconds
// NOLINTNEXTLINE
bvar::Window<bvar::Adder<int>> g_open_segments_io_minute("starrocks", "open_segments_io_minute", &g_open_segments_io,
                                                         60);

namespace starrocks {

using strings::Substitute;

StatusOr<std::shared_ptr<Segment>> Segment::open(std::shared_ptr<FileSystem> fs, FileInfo segment_file_info,
                                                 uint32_t segment_id, std::shared_ptr<const TabletSchema> tablet_schema,
                                                 size_t* footer_length_hint,
                                                 const FooterPointerPB* partial_rowset_footer,
                                                 bool skip_fill_local_cache, lake::TabletManager* tablet_manager) {
    auto segment = std::make_shared<Segment>(std::move(fs), std::move(segment_file_info), segment_id,
                                             std::move(tablet_schema), tablet_manager);
    RETURN_IF_ERROR(segment->open(footer_length_hint, partial_rowset_footer, skip_fill_local_cache));
    return std::move(segment);
}

StatusOr<size_t> Segment::parse_segment_footer(RandomAccessFile* read_file, SegmentFooterPB* footer,
                                               size_t* footer_length_hint,
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
    std::string buff;
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

    uint32_t actual_checksum = 0;
    if (footer_length <= buff.size()) {
        g_open_segments << 1;
        g_open_segments_io << 1;

        std::string_view buf_footer(buff.data() + buff.size() - footer_length, footer_length);
        actual_checksum = crc32c::Value(buf_footer.data(), buf_footer.size());
        if (!footer->ParseFromArray(buf_footer.data(), buf_footer.size())) {
            return Status::Corruption(
                    strings::Substitute("Bad segment file $0: failed to parse footer", read_file->filename()));
        }
    } else { // Need read file again.
        g_open_segments << 1;
        g_open_segments_io << 2;

        int left_size = (int)footer_length - buff.size();
        std::string buff_2;
        raw::stl_string_resize_uninitialized(&buff_2, left_size);
        RETURN_IF_ERROR(read_file->read_at_fully(file_size - footer_length - 12, buff_2.data(), buff_2.size()));
        actual_checksum = crc32c::Extend(actual_checksum, buff_2.data(), buff_2.size());
        actual_checksum = crc32c::Extend(actual_checksum, buff.data(), buff.size());

        ::google::protobuf::io::ArrayInputStream stream1(buff_2.data(), buff_2.size());
        ::google::protobuf::io::ArrayInputStream stream2(buff.data(), buff.size());
        ::google::protobuf::io::ZeroCopyInputStream* streams[2] = {&stream1, &stream2};
        ::google::protobuf::io::ConcatenatingInputStream concatenating_stream(streams, 2);
        if (!footer->ParseFromZeroCopyStream(&concatenating_stream)) {
            return Status::Corruption(
                    strings::Substitute("Bad segment file $0: failed to parse footer", read_file->filename()));
        }
    }

    // validate footer PB's checksum
    if (actual_checksum != checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                                    read_file->filename(), actual_checksum, checksum));
    }

    return footer_length + 12;
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
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->segment_metadata_mem_tracker(), _basic_info_mem_usage());
}

Segment::~Segment() {
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->segment_metadata_mem_tracker(), _basic_info_mem_usage());
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->short_key_index_mem_tracker(), _short_key_index_mem_usage());
}

Status Segment::open(size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer,
                     bool skip_fill_local_cache) {
    if (invoked(_open_once)) {
        return Status::OK();
    }

    auto res = success_once(_open_once,
                            [&] { return _open(footer_length_hint, partial_rowset_footer, skip_fill_local_cache); });

    // move the cache size update out of the `success_once`,
    // so that the onceflag `_open_once` can be set before the cache_size is updated.
    if (res.ok() && *res) {
        update_cache_size();
    }
    return res.status();
}

Status Segment::_open(size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer,
                      bool skip_fill_local_cache) {
    SegmentFooterPB footer;
    RandomAccessFileOptions opts{.skip_fill_local_cache = skip_fill_local_cache};

    ASSIGN_OR_RETURN(auto read_file, _fs->new_random_access_file(opts, _segment_file_info));
    RETURN_IF_ERROR(Segment::parse_segment_footer(read_file.get(), &footer, footer_length_hint, partial_rowset_footer));
    RETURN_IF_ERROR(_create_column_readers(&footer));
    _num_rows = footer.num_rows();
    _short_key_index_page = PagePointer(footer.short_key_index_page());
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

StatusOr<ChunkIteratorPtr> Segment::_new_iterator(const Schema& schema, const SegmentReadOptions& read_options) {
    DCHECK(read_options.stats != nullptr);
    // trying to prune the current segment by segment-level zone map
    for (const auto& pair : read_options.predicates_for_zone_map) {
        ColumnId column_id = pair.first;
        const auto& tablet_column = read_options.tablet_schema ? read_options.tablet_schema->column(column_id)
                                                               : _tablet_schema->column(column_id);
        auto column_unique_id = tablet_column.unique_id();
        if (_column_readers.count(column_unique_id) < 1 || !_column_readers.at(column_unique_id)->has_zone_map()) {
            continue;
        }
        if (!_column_readers.at(column_unique_id)->segment_zone_map_filter(pair.second)) {
            // skip segment zonemap filter when this segment has column files link to it.
            if (tablet_column.is_key() || _use_segment_zone_map_filter(read_options)) {
                read_options.stats->segment_stats_filtered += _column_readers.at(column_unique_id)->num_rows();
                return Status::EndOfFile(
                        strings::Substitute("End of file $0, empty iterator", _segment_file_info.path));
            } else {
                break;
            }
        }
    }
    return new_segment_iterator(shared_from_this(), schema, read_options);
}

StatusOr<ChunkIteratorPtr> Segment::new_iterator(const Schema& schema, const SegmentReadOptions& read_options) {
    if (read_options.stats == nullptr) {
        return Status::InvalidArgument("stats is null pointer");
    }
    return _new_iterator(schema, read_options);
}

Status Segment::load_index(bool skip_fill_local_cache) {
    auto res = success_once(_load_index_once, [&] {
        SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);

        Status st = _load_index(skip_fill_local_cache);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->short_key_index_mem_tracker(),
                                     _short_key_index_mem_usage());
            update_cache_size();
        } else {
            _reset();
        }
        return st;
    });
    return res.status();
}

Status Segment::_load_index(bool skip_fill_local_cache) {
    // read and parse short key index page
    RandomAccessFileOptions file_opts{.skip_fill_local_cache = skip_fill_local_cache};
    ASSIGN_OR_RETURN(auto read_file, _fs->new_random_access_file(file_opts, _segment_file_info));

    PageReadOptions opts;
    opts.use_page_cache = !config::disable_storage_page_cache;
    opts.read_file = read_file.get();
    opts.page_pointer = _short_key_index_page;
    opts.codec = nullptr; // short key index page uses NO_COMPRESSION for now
    OlapReaderStatistics tmp_stats;
    opts.stats = &tmp_stats;

    Slice body;
    PageFooterPB footer;
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

Status Segment::_create_column_readers(SegmentFooterPB* footer) {
    std::unordered_map<uint32_t, uint32_t> column_id_to_footer_ordinal;
    for (uint32_t ordinal = 0, sz = footer->columns().size(); ordinal < sz; ++ordinal) {
        const auto& column_pb = footer->columns(ordinal);
        column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
    }

    for (uint32_t ordinal = 0, sz = _tablet_schema->num_columns(); ordinal < sz; ++ordinal) {
        const auto& column = _tablet_schema->column(ordinal);
        auto iter = column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == column_id_to_footer_ordinal.end()) {
            continue;
        }

        auto res = ColumnReader::create(footer->mutable_columns(iter->second), this);
        if (!res.ok()) {
            return res.status();
        }
        _column_readers.emplace(column.unique_id(), std::move(res).value());
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<ColumnIterator>> Segment::new_column_iterator_or_default(const TabletColumn& column,
                                                                                  ColumnAccessPath* path) {
    auto id = column.unique_id();
    if (_column_readers.contains(id)) {
        return _column_readers.at(id)->new_iterator(path);
    } else if (!column.has_default_value() && !column.is_nullable()) {
        return Status::InternalError(
                fmt::format("invalid nonexistent column({}) without default value.", column.name()));
    } else {
        const TypeInfoPtr& type_info = get_type_info(column);
        auto default_value_iter = std::make_unique<DefaultValueColumnIterator>(
                column.has_default_value(), column.default_value(), column.is_nullable(), type_info, column.length(),
                num_rows());
        ColumnIteratorOptions iter_opts;
        RETURN_IF_ERROR(default_value_iter->init(iter_opts));
        return default_value_iter;
    }
}

StatusOr<std::unique_ptr<ColumnIterator>> Segment::new_column_iterator(ColumnUID id, ColumnAccessPath* path) {
    auto iter = _column_readers.find(id);
    if (iter != _column_readers.end()) {
        return iter->second->new_iterator(path);
    } else {
        return Status::NotFound(fmt::format("{} does not contain column of id {}", _segment_file_info.path, id));
    }
}

Status Segment::new_bitmap_index_iterator(ColumnUID id, const IndexReadOptions& options, BitmapIndexIterator** res) {
    auto iter = _column_readers.find(id);
    if (iter != _column_readers.end() && iter->second->has_bitmap_index()) {
        return iter->second->new_bitmap_index_iterator(options, res);
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<Segment>> Segment::new_dcg_segment(const DeltaColumnGroup& dcg, uint32_t idx,
                                                            const TabletSchemaCSPtr& read_tablet_schema) {
    std::shared_ptr<TabletSchema> tablet_schema;
    if (read_tablet_schema != nullptr) {
        tablet_schema = TabletSchema::create_with_uid(read_tablet_schema, dcg.column_ids()[idx]);
    } else {
        tablet_schema = TabletSchema::create_with_uid(_tablet_schema.schema(), dcg.column_ids()[idx]);
    }
    FileInfo info{.path = dcg.column_files(parent_name(_segment_file_info.path))[idx]};
    return Segment::open(_fs, info, 0, tablet_schema, nullptr);
}

Status Segment::get_short_key_index(std::vector<std::string>* sk_index_values) {
    RETURN_IF_ERROR(load_index(false));
    for (size_t i = 0; i < _sk_index_decoder->num_items(); i++) {
        sk_index_values->emplace_back(_sk_index_decoder->key(i).to_string());
    }
    return Status::OK();
}

size_t Segment::_column_index_mem_usage() const {
    size_t size = 0;
    for (auto& r : _column_readers) {
        auto& reader = r.second;
        size += reader->mem_usage();
    }
    return size;
}

void Segment::update_cache_size() {
    if (_tablet_manager != nullptr) {
        _tablet_manager->update_segment_cache_size(file_name(), reinterpret_cast<intptr_t>(this));
    }
}

size_t Segment::mem_usage() const {
    if (!invoked(_open_once)) {
        // just report the basic info memory usage if not opened yet
        return _basic_info_mem_usage();
    }
    return _basic_info_mem_usage() + _short_key_index_mem_usage() + _column_index_mem_usage();
}
} // namespace starrocks
