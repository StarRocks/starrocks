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
#include "segment_chunk_iterator_adapter.h"
#include "segment_iterator.h"
#include "segment_options.h"
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

StatusOr<std::shared_ptr<Segment>> Segment::open(std::shared_ptr<FileSystem> fs, const std::string& path,
                                                 uint32_t segment_id, const TabletSchema* tablet_schema,
                                                 size_t* footer_length_hint,
                                                 const FooterPointerPB* partial_rowset_footer) {
    auto segment = std::make_shared<Segment>(private_type(0), std::move(fs), path, segment_id, tablet_schema);

    RETURN_IF_ERROR(segment->_open(footer_length_hint, partial_rowset_footer, true));
    return std::move(segment);
}

StatusOr<std::shared_ptr<Segment>> Segment::open(std::shared_ptr<FileSystem> fs, const std::string& path,
                                                 uint32_t segment_id, std::shared_ptr<const TabletSchema> tablet_schema,
                                                 size_t* footer_length_hint,
                                                 const FooterPointerPB* partial_rowset_footer,
                                                 bool skip_fill_local_cache) {
    auto segment =
            std::make_shared<Segment>(private_type(0), std::move(fs), path, segment_id, std::move(tablet_schema));

    RETURN_IF_ERROR(segment->_open(footer_length_hint, partial_rowset_footer, skip_fill_local_cache));
    return std::move(segment);
}

Status Segment::parse_segment_footer(RandomAccessFile* read_file, SegmentFooterPB* footer, size_t* footer_length_hint,
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

    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2", read_file->filename(),
                                                      file_size, 12 + footer_length));
    }

    if (footer_length_hint != nullptr && footer_length > *footer_length_hint) {
        *footer_length_hint = footer_length + 128 /* allocate slightly more bytes next time*/;
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

    return Status::OK();
}

Segment::Segment(const private_type&, std::shared_ptr<FileSystem> fs, std::string path, uint32_t segment_id,
                 const TabletSchema* tablet_schema)
        : _fs(std::move(fs)), _fname(std::move(path)), _tablet_schema(tablet_schema), _segment_id(segment_id) {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->segment_metadata_mem_tracker(), _basic_info_mem_usage());
}

Segment::Segment(const private_type&, std::shared_ptr<FileSystem> fs, std::string path, uint32_t segment_id,
                 std::shared_ptr<const TabletSchema> tablet_schema)
        : _fs(std::move(fs)),
          _fname(std::move(path)),
          _tablet_schema(std::move(tablet_schema)),
          _segment_id(segment_id) {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->segment_metadata_mem_tracker(), _basic_info_mem_usage());
}

Segment::~Segment() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->segment_metadata_mem_tracker(), _basic_info_mem_usage());
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->short_key_index_mem_tracker(), _short_key_index_mem_usage());
}

Status Segment::_open(size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer,
                      bool skip_fill_local_cache) {
    SegmentFooterPB footer;
    RandomAccessFileOptions opts(skip_fill_local_cache);
    ASSIGN_OR_RETURN(auto read_file, _fs->new_random_access_file(opts, _fname));
    RETURN_IF_ERROR(Segment::parse_segment_footer(read_file.get(), &footer, footer_length_hint, partial_rowset_footer));

    RETURN_IF_ERROR(_create_column_readers(&footer));
    _num_rows = footer.num_rows();
    _short_key_index_page = PagePointer(footer.short_key_index_page());
    _prepare_adapter_info();
    return Status::OK();
}

bool Segment::_use_segment_zone_map_filter(const SegmentReadOptions& read_options) {
    if (!read_options.is_primary_keys || read_options.dcg_loader == nullptr) {
        return true;
    }
    SCOPED_RAW_TIMER(&read_options.stats->get_delta_column_group_ns);
    DeltaColumnGroupList dcgs;
    TabletSegmentId tsid;
    tsid.tablet_id = read_options.tablet_id;
    tsid.segment_id = read_options.rowset_id + _segment_id;
    auto st = read_options.dcg_loader->load(tsid, read_options.version, &dcgs);
    return st.ok() && dcgs.size() == 0;
}

StatusOr<ChunkIteratorPtr> Segment::_new_iterator(const Schema& schema, const SegmentReadOptions& read_options) {
    DCHECK(read_options.stats != nullptr);
    // trying to prune the current segment by segment-level zone map
    for (const auto& pair : read_options.predicates_for_zone_map) {
        ColumnId column_id = pair.first;
        if (_column_readers[column_id] == nullptr || !_column_readers[column_id]->has_zone_map()) {
            continue;
        }
        if (!_column_readers[column_id]->segment_zone_map_filter(pair.second)) {
            // skip segment zonemap filter when this segment has column files link to it.
            const TabletColumn& tablet_column = _tablet_schema->column(column_id);
            if (tablet_column.is_key() || _use_segment_zone_map_filter(read_options)) {
                read_options.stats->segment_stats_filtered += _column_readers[column_id]->num_rows();
                return Status::EndOfFile(strings::Substitute("End of file $0, empty iterator", _fname));
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
    // If input schema is not match the actual meta, must convert the read_options according
    // to the actual format. And create an AdaptSegmentIterator to wrap
    if (_needs_chunk_adapter) {
        std::unique_ptr<SegmentChunkIteratorAdapter> adapter(new SegmentChunkIteratorAdapter(
                *_tablet_schema, *_column_storage_types, schema, read_options.chunk_size));
        RETURN_IF_ERROR(adapter->prepare(read_options));

        auto result = _new_iterator(adapter->in_schema(), adapter->in_read_options());
        if (!result.ok()) {
            return result;
        }
        adapter->set_iterator(std::move(result.value()));
        return std::move(adapter);
    } else {
        return _new_iterator(schema, read_options);
    }
}

Status Segment::load_index(bool skip_fill_local_cache) {
    auto res = success_once(_load_index_once, [&] {
        SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);

        Status st = _load_index(skip_fill_local_cache);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->short_key_index_mem_tracker(),
                                     _short_key_index_mem_usage());
        } else {
            _reset();
        }
        return st;
    });
    return res.status();
}

Status Segment::_load_index(bool skip_fill_local_cache) {
    // read and parse short key index page
    RandomAccessFileOptions file_opts(skip_fill_local_cache);
    ASSIGN_OR_RETURN(auto read_file, _fs->new_random_access_file(file_opts, _fname));

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

    _column_readers.resize(_tablet_schema->columns().size());
    for (uint32_t ordinal = 0, sz = _tablet_schema->num_columns(); ordinal < sz; ++ordinal) {
        const auto& column = _tablet_schema->columns()[ordinal];
        auto iter = column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == column_id_to_footer_ordinal.end()) {
            continue;
        }

        auto res = ColumnReader::create(footer->mutable_columns(iter->second), this);
        if (!res.ok()) {
            return res.status();
        }
        _column_readers[ordinal] = std::move(res).value();
    }
    return Status::OK();
}

void Segment::_prepare_adapter_info() {
    ColumnId num_columns = _tablet_schema->num_columns();
    _needs_chunk_adapter = false;
    std::vector<LogicalType> types(num_columns);
    for (ColumnId cid = 0; cid < num_columns; ++cid) {
        LogicalType type;
        if (_column_readers[cid] != nullptr) {
            type = _column_readers[cid]->column_type();
        } else {
            // when the default column is used, column reader will be null.
            // And the type will be same with the tablet schema.
            type = _tablet_schema->column(cid).type();
        }
        types[cid] = type;
        if (TypeUtils::specific_type_of_format_v1(type)) {
            _needs_chunk_adapter = true;
        }
    }
    if (_needs_chunk_adapter) {
        _column_storage_types = std::make_unique<std::vector<LogicalType>>(std::move(types));
    }
}

StatusOr<std::unique_ptr<ColumnIterator>> Segment::new_column_iterator(uint32_t cid, ColumnAccessPath* path) {
    if (_column_readers[cid] == nullptr) {
        const TabletColumn& tablet_column = _tablet_schema->column(cid);
        if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
            return Status::InternalError(
                    fmt::format("invalid nonexistent column({}) without default value.", tablet_column.name()));
        }
        const TypeInfoPtr& type_info = get_type_info(tablet_column);
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(new DefaultValueColumnIterator(
                tablet_column.has_default_value(), tablet_column.default_value(), tablet_column.is_nullable(),
                type_info, tablet_column.length(), num_rows()));
        ColumnIteratorOptions iter_opts;
        RETURN_IF_ERROR(default_value_iter->init(iter_opts));
        return default_value_iter;
    }
    return _column_readers[cid]->new_iterator(path);
}

Status Segment::new_bitmap_index_iterator(uint32_t cid, const IndexReadOptions& options, BitmapIndexIterator** iter) {
    if (_column_readers[cid] != nullptr && _column_readers[cid]->has_bitmap_index()) {
        return _column_readers[cid]->new_bitmap_index_iterator(options, iter);
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<Segment>> Segment::new_dcg_segment(const DeltaColumnGroup& dcg) {
    return Segment::open(_fs, dcg.column_file(parent_name(_fname)), 0,
                         TabletSchema::create_with_uid(*_tablet_schema, dcg.column_ids()), nullptr);
}

Status Segment::get_short_key_index(std::vector<std::string>* sk_index_values) {
    RETURN_IF_ERROR(load_index(false));
    for (size_t i = 0; i < _sk_index_decoder->num_items(); i++) {
        sk_index_values->emplace_back(_sk_index_decoder->key(i).to_string());
    }
    return Status::OK();
}

} // namespace starrocks
