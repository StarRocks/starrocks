// This file is made available under Elastic License 2.0.
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

#include "storage/rowset/segment_v2/segment.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <memory>

#include "column/schema.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "storage/fs/fs_util.h"
#include "storage/rowset/segment_v2/column_reader.h"
#include "storage/rowset/segment_v2/empty_segment_iterator.h"
#include "storage/rowset/segment_v2/page_io.h"
#include "storage/rowset/segment_v2/segment_iterator.h"
#include "storage/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "storage/rowset/vectorized/segment_chunk_iterator_adapter.h"
#include "storage/rowset/vectorized/segment_iterator.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/rowset/vectorized/segment_v2_iterator_adapter.h"
#include "storage/tablet_schema.h"
#include "storage/vectorized/type_utils.h"
#include "util/crc32c.h"
#include "util/slice.h"

namespace starrocks::segment_v2 {

using strings::Substitute;

StatusOr<std::shared_ptr<Segment>> Segment::open(MemTracker* mem_tracker, fs::BlockManager* blk_mgr,
                                                 const std::string& filename, uint32_t segment_id,
                                                 const TabletSchema* tablet_schema, size_t* footer_length_hint) {
    auto segment =
            std::make_shared<Segment>(private_type(0), mem_tracker, blk_mgr, filename, segment_id, tablet_schema);
    RETURN_IF_ERROR(segment->_open(footer_length_hint));
    return std::move(segment);
}

Segment::Segment(const private_type&, MemTracker* mem_tracker, fs::BlockManager* blk_mgr, std::string fname,
                 uint32_t segment_id, const TabletSchema* tablet_schema)
        : _mem_tracker(mem_tracker),
          _block_mgr(blk_mgr),
          _fname(std::move(fname)),
          _segment_id(segment_id),
          _tablet_schema(tablet_schema) {
    _mem_tracker->consume(sizeof(Segment) + _fname.size());
}

Segment::~Segment() = default;

Status Segment::_open(size_t* footer_length_hint) {
    RETURN_IF_ERROR(_parse_footer(footer_length_hint));
    RETURN_IF_ERROR(_create_column_readers());
    _prepare_adapter_info();
    return Status::OK();
}

Status Segment::_new_iterator(const Schema& schema, const StorageReadOptions& read_options,
                              std::unique_ptr<RowwiseIterator>* iter) {
    DCHECK_NOTNULL(read_options.stats);
    // trying to prune the current segment by segment-level zone map
    if (read_options.conditions != nullptr) {
        for (const auto& column_condition : read_options.conditions->columns()) {
            int32_t column_id = column_condition.first;
            if (_column_readers[column_id] == nullptr || !_column_readers[column_id]->has_zone_map()) {
                continue;
            }
            if (!_column_readers[column_id]->match_condition(column_condition.second)) {
                // any condition not satisfied, return.
                *iter = std::make_unique<EmptySegmentIterator>(schema);
                return Status::OK();
            }
        }
    }
    RETURN_IF_ERROR(_load_index());
    *iter = std::make_unique<SegmentIterator>(this->shared_from_this(), schema);
    iter->get()->init(read_options);
    return Status::OK();
}

Status Segment::new_iterator(const Schema& schema, const StorageReadOptions& read_options,
                             std::unique_ptr<RowwiseIterator>* output) {
    DCHECK_NOTNULL(read_options.stats);

    // If input schema is not match the actual meta, must convert the read_options according
    // to the actual format. And create an AdaptSegmentIterator to wrap
    if (_needs_block_adapter) {
        std::unique_ptr<vectorized::SegmentV2IteratorAdapter> adapter(
                new vectorized::SegmentV2IteratorAdapter(*_tablet_schema, _column_storage_types, schema));

        RETURN_IF_ERROR(adapter->init(read_options));

        std::unique_ptr<RowwiseIterator> iter;
        RETURN_IF_ERROR(_new_iterator(adapter->in_schema(), adapter->in_read_options(), &iter));
        adapter->set_iterator(std::move(iter));
        *output = std::move(adapter);
        return Status::OK();
    } else {
        return _new_iterator(schema, read_options, output);
    }
}

StatusOr<ChunkIteratorPtr> Segment::_new_iterator(const vectorized::Schema& schema,
                                                  const vectorized::SegmentReadOptions& read_options) {
    DCHECK_NOTNULL(read_options.stats);
    // trying to prune the current segment by segment-level zone map
    for (const auto& pair : read_options.predicates) {
        ColumnId column_id = pair.first;
        if (_column_readers[column_id] == nullptr || !_column_readers[column_id]->has_zone_map()) {
            continue;
        }
        if (!_column_readers[column_id]->segment_zone_map_filter(pair.second)) {
            return Status::EndOfFile("empty iterator");
        }
    }
    return vectorized::new_segment_iterator(shared_from_this(), schema, read_options);
}

StatusOr<ChunkIteratorPtr> Segment::new_iterator(const vectorized::Schema& schema,
                                                 const vectorized::SegmentReadOptions& read_options) {
    if (read_options.stats == nullptr) {
        return Status::InvalidArgument("stats is null pointer");
    }
    // If input schema is not match the actual meta, must convert the read_options according
    // to the actual format. And create an AdaptSegmentIterator to wrap
    if (_needs_chunk_adapter) {
        std::unique_ptr<vectorized::SegmentChunkIteratorAdapter> adapter(new vectorized::SegmentChunkIteratorAdapter(
                *_tablet_schema, _column_storage_types, schema, read_options.chunk_size));
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

Status Segment::_parse_footer(size_t* footer_length_hint) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::unique_ptr<fs::ReadableBlock> rblock;
    RETURN_IF_ERROR(_block_mgr->open_block(_fname, &rblock));

    uint64_t file_size;
    RETURN_IF_ERROR(rblock->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < 12", _fname, file_size));
    }

    size_t hint_size = footer_length_hint ? *footer_length_hint : 4096;
    size_t footer_read_size = std::min<size_t>(hint_size, file_size);

    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, footer_read_size);

    RETURN_IF_ERROR(rblock->read(file_size - buff.size(), buff));

    const uint32_t footer_length = UNALIGNED_LOAD32(buff.data() + buff.size() - 12);
    const uint32_t checksum = UNALIGNED_LOAD32(buff.data() + buff.size() - 8);
    const uint32_t magic_number = UNALIGNED_LOAD32(buff.data() + buff.size() - 4);

    // validate magic number
    if (magic_number != UNALIGNED_LOAD32(k_segment_magic)) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: magic number not match", _fname));
    }

    if (file_size < 12 + footer_length) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: file size $1 < $2", _fname, file_size, 12 + footer_length));
    }

    if (footer_length_hint != nullptr && footer_length > *footer_length_hint) {
        *footer_length_hint = footer_length + 128 /* allocate slightly more bytes next time*/;
    }

    buff.resize(buff.size() - 12); // Remove the last 12 bytes.

    uint32_t actual_checksum = 0;
    if (footer_length <= buff.size()) {
        std::string_view footer(buff.data() + buff.size() - footer_length, footer_length);
        actual_checksum = crc32c::Value(footer.data(), footer.size());
        if (!_footer.ParseFromArray(footer.data(), footer.size())) {
            return Status::Corruption(strings::Substitute("Bad segment file $0: failed to parse footer", _fname));
        }
    } else { // Need read file again.
        int left_size = (int)footer_length - buff.size();
        std::string buff_2;
        raw::stl_string_resize_uninitialized(&buff_2, left_size);
        RETURN_IF_ERROR(rblock->read(file_size - footer_length - 12, buff_2));
        actual_checksum = crc32c::Extend(actual_checksum, buff_2.data(), buff_2.size());
        actual_checksum = crc32c::Extend(actual_checksum, buff.data(), buff.size());

        ::google::protobuf::io::ArrayInputStream stream1(buff_2.data(), buff_2.size());
        ::google::protobuf::io::ArrayInputStream stream2(buff.data(), buff.size());
        ::google::protobuf::io::ZeroCopyInputStream* streams[2] = {&stream1, &stream2};
        ::google::protobuf::io::ConcatenatingInputStream concatenating_stream(streams, 2);
        if (!_footer.ParseFromZeroCopyStream(&concatenating_stream)) {
            return Status::Corruption(strings::Substitute("Bad segment file $0: failed to parse footer", _fname));
        }
    }

    // validate footer PB's checksum
    if (actual_checksum != checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2", _fname,
                                    actual_checksum, checksum));
    }

    // The memory usage obtained through SpaceUsedLong() is an estimate
    _mem_tracker->consume(static_cast<int64_t>(_footer.SpaceUsedLong()) -
                          static_cast<int64_t>(sizeof(SegmentFooterPB)));
    return Status::OK();
}

Status Segment::_load_index() {
    return _load_index_once.call([this] {
        // read and parse short key index page
        std::unique_ptr<fs::ReadableBlock> rblock;
        RETURN_IF_ERROR(_block_mgr->open_block(_fname, &rblock));

        PageReadOptions opts;
        opts.use_page_cache = !config::disable_storage_page_cache;
        opts.rblock = rblock.get();
        opts.page_pointer = PagePointer(_footer.short_key_index_page());
        opts.codec = nullptr; // short key index page uses NO_COMPRESSION for now
        OlapReaderStatistics tmp_stats;
        opts.stats = &tmp_stats;

        Slice body;
        PageFooterPB footer;
        RETURN_IF_ERROR(PageIO::read_and_decompress_page(opts, &_sk_index_handle, &body, &footer));
        _mem_tracker->consume(_sk_index_handle.mem_usage());

        DCHECK_EQ(footer.type(), SHORT_KEY_PAGE);
        DCHECK(footer.has_short_key_page_footer());

        _sk_index_decoder = std::make_unique<ShortKeyIndexDecoder>();
        RETURN_IF_ERROR(_sk_index_decoder->parse(body, footer.short_key_page_footer()));
        _mem_tracker->consume(_sk_index_decoder->mem_usage());

        return Status::OK();
    });
}

Status Segment::_create_column_readers() {
    std::unordered_map<uint32_t, uint32_t> column_id_to_footer_ordinal;
    for (uint32_t ordinal = 0; ordinal < _footer.columns().size(); ++ordinal) {
        const auto& column_pb = _footer.columns(ordinal);
        column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
    }

    _column_readers.resize(_tablet_schema->columns().size());
    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        const auto& column = _tablet_schema->columns()[ordinal];
        auto iter = column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == column_id_to_footer_ordinal.end()) {
            continue;
        }

        ColumnReaderOptions opts;
        opts.block_mgr = _block_mgr;
        opts.storage_format_version = _footer.version();
        opts.kept_in_memory = _tablet_schema->is_in_memory();
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(_mem_tracker, opts, _footer.columns(iter->second), _footer.num_rows(),
                                             _fname, &reader));
        _column_readers[ordinal] = std::move(reader);
    }
    return Status::OK();
}

void Segment::_prepare_adapter_info() {
    ColumnId num_columns = _tablet_schema->num_columns();
    _needs_block_adapter = false;
    _needs_chunk_adapter = false;
    _column_storage_types.resize(num_columns);
    for (ColumnId cid = 0; cid < num_columns; ++cid) {
        FieldType type;
        if (_column_readers[cid] != nullptr) {
            type = _column_readers[cid]->column_type();
        } else {
            // when the default column is used, column reader will be null.
            // And the type will be same with the tablet schema.
            type = _tablet_schema->column(cid).type();
        }
        _column_storage_types[cid] = type;
        if (TypeUtils::specific_type_of_format_v1(type)) {
            _needs_chunk_adapter = true;
        }
        if (type != _tablet_schema->column(cid).type()) {
            _needs_block_adapter = true;
        }
    }
}

Status Segment::new_column_iterator(uint32_t cid, ColumnIterator** iter) {
    if (_column_readers[cid] == nullptr) {
        const TabletColumn& tablet_column = _tablet_schema->column(cid);
        if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
            return Status::InternalError(
                    Substitute("invalid nonexistent column($0) without default value.", tablet_column.name()));
        }
        const TypeInfoPtr& type_info = get_type_info(tablet_column);
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(new DefaultValueColumnIterator(
                tablet_column.has_default_value(), tablet_column.default_value(), tablet_column.is_nullable(),
                type_info, tablet_column.length(), num_rows()));
        ColumnIteratorOptions iter_opts;
        RETURN_IF_ERROR(default_value_iter->init(iter_opts));
        *iter = default_value_iter.release();
        return Status::OK();
    }
    return _column_readers[cid]->new_iterator(iter);
}

Status Segment::new_bitmap_index_iterator(uint32_t cid, BitmapIndexIterator** iter) {
    if (_column_readers[cid] != nullptr && _column_readers[cid]->has_bitmap_index()) {
        return _column_readers[cid]->new_bitmap_index_iterator(iter);
    }
    return Status::OK();
}

} // namespace starrocks::segment_v2
