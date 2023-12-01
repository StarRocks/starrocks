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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_writer.cpp

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

#include "storage/rowset/column_writer.h"

#include <cstddef>
#include <memory>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "simd/simd.h"
#include "storage/rowset/array_column_writer.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/rowset/bitshuffle_page.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/rowset/bloom_filter_index_writer.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/map_column_writer.h"
#include "storage/rowset/options.h"
#include "storage/rowset/ordinal_page_index.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/struct_column_writer.h"
#include "storage/rowset/zone_map_index.h"
#include "util/compression/block_compression.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"

namespace starrocks {

#define INDEX_ADD_VALUES(index, data, size) \
    do {                                    \
        if (index != nullptr) {             \
            index->add_values(data, size);  \
        }                                   \
    } while (0)

#define INDEX_ADD_NULLS(index, count) \
    do {                              \
        if (index != nullptr) {       \
            index->add_nulls(count);  \
        }                             \
    } while (0)

using strings::Substitute;

class ByteIterator {
public:
    ByteIterator(const uint8_t* bytes, size_t size) : _bytes(bytes), _size(size) {}

    // Returns a pair consisting of the run length and the value of the run.
    std::pair<size_t, uint8_t> next() {
        if (UNLIKELY(_pos == _size)) {
            return std::pair<size_t, uint8_t>{0, 0};
        }
        size_t prev = _pos++;
        while (_pos < _size && _bytes[_pos] == _bytes[prev]) {
            ++_pos;
        }
        return std::pair<size_t, uint8_t>{_pos - prev, _bytes[prev]};
    }

private:
    const uint8_t* _bytes;
    const size_t _size;
    size_t _pos{0};
};

class NullMapRLEBuilder {
public:
    NullMapRLEBuilder() : _bitmap_buf(512), _rle_encoder(&_bitmap_buf, 1) {}

    explicit NullMapRLEBuilder(size_t reserve_bits)
            : _bitmap_buf(BitmapSize(reserve_bits)), _rle_encoder(&_bitmap_buf, 1) {}

    void add_run(bool value, size_t run) {
        _has_null |= value;
        _rle_encoder.Put(value, run);
    }

    // Returns whether the building nullmap contains NULL
    bool has_null() const { return _has_null; }

    OwnedSlice finish() {
        _rle_encoder.Flush();
        return _bitmap_buf.build();
    }

    void reset() {
        _has_null = false;
        _rle_encoder.Clear();
    }

    uint64_t size() { return _bitmap_buf.size(); }

private:
    bool _has_null{false};
    faststring _bitmap_buf;
    RleEncoder<bool> _rle_encoder;
};

class NullFlagsBuilder {
public:
    explicit NullFlagsBuilder(NullEncodingPB null_encoding) : NullFlagsBuilder(32 * 1024, null_encoding) {}

    explicit NullFlagsBuilder(size_t reserve_bits, NullEncodingPB null_encoding)
            : _null_map(reserve_bits), _null_encoding(null_encoding) {}

    void add_null_flags(const uint8_t* flags, size_t count) { _null_map.append(flags, count); }

    ALWAYS_INLINE bool has_null() const { return _has_null; }

    ALWAYS_INLINE void set_has_null(bool has_null) { _has_null = has_null; }

    OwnedSlice finish() {
        if (_null_encoding == NullEncodingPB::BITSHUFFLE_NULL) {
            size_t old_size = _null_map.size();
            _null_map.resize(ALIGN_UP(_null_map.size(), 8u));
            memset(_null_map.data() + old_size, 0, _null_map.size() - old_size);
            _encode_buf.resize(bitshuffle::compress_lz4_bound(_null_map.size(), sizeof(uint8_t), 0));
            int64_t r = bitshuffle::compress_lz4(_null_map.data(), _encode_buf.data(), _null_map.size(),
                                                 sizeof(uint8_t), 0);
            if (r < 0) {
                LOG(ERROR) << "bitshuffle compress failed: " << bitshuffle_error_msg(r);
                return {};
            }
            return _encode_buf.build();
        } else if (_null_encoding == NullEncodingPB::LZ4_NULL) {
            const BlockCompressionCodec* codec = nullptr;
            CompressionTypePB type = CompressionTypePB::LZ4;
            Status status = get_block_compression_codec(type, &codec);
            if (!status.ok()) {
                LOG(ERROR) << "get codec failed, fail to encode null flags";
                return {};
            }
            _encode_buf.resize(codec->max_compressed_len(_null_map.size()));
            Slice origin_slice(_null_map);
            Slice compressed_slice(_encode_buf);
            status = codec->compress(origin_slice, &compressed_slice);
            if (!status.ok()) {
                LOG(ERROR) << "compress null map failed";
                return {};
            }
            // _encode_buf must be resize to compressed slice's size
            _encode_buf.resize(compressed_slice.get_size());
            return _encode_buf.build();
        } else {
            LOG(ERROR) << "invalid null encoding:" << _null_encoding;
            return {};
        }
    }

    void reset() {
        _has_null = false;
        _null_map.clear();
        _encode_buf.clear();
    }

    size_t size() { return _null_map.size(); }

    size_t data_count() const {
        if (!_has_null) {
            return _null_map.size();
        }
        return SIMD::count_zero(_null_map.data(), _null_map.size());
    }

    NullEncodingPB null_encoding() { return _null_encoding; }

private:
    bool _has_null{false};
    faststring _null_map;
    faststring _encode_buf;
    NullEncodingPB _null_encoding;
};

class StringColumnWriter final : public ColumnWriter {
public:
    StringColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                       std::unique_ptr<ScalarColumnWriter> column_writer);

    ~StringColumnWriter() override = default;

    Status init() override { return _scalar_column_writer->init(); };

    Status append(const Column& column) override;

    // Speculate char/varchar encoding and reset encoding
    void speculate_column_and_set_encoding(const Column& column);

    // Speculate char/varchar encoding
    EncodingTypePB speculate_string_encoding(const BinaryColumn& bin_col);

    Status finish_current_page() override { return _scalar_column_writer->finish_current_page(); };

    uint64_t estimate_buffer_size() override { return _scalar_column_writer->estimate_buffer_size(); };

    // finish append data
    Status finish() override;

    Status write_data() override { return _scalar_column_writer->write_data(); };
    Status write_ordinal_index() override { return _scalar_column_writer->write_ordinal_index(); };
    Status write_zone_map() override { return _scalar_column_writer->write_zone_map(); };
    Status write_bitmap_index() override { return _scalar_column_writer->write_bitmap_index(); };
    Status write_bloom_filter_index() override { return _scalar_column_writer->write_bloom_filter_index(); };

    ordinal_t get_next_rowid() const override { return _scalar_column_writer->get_next_rowid(); };

    bool is_global_dict_valid() override { return _scalar_column_writer->is_global_dict_valid(); }

    uint64_t total_mem_footprint() const override { return _scalar_column_writer->total_mem_footprint(); }

    Status check_string_lengths(const Column& column);

private:
    std::unique_ptr<ScalarColumnWriter> _scalar_column_writer;
    bool _is_speculated = false;
    ColumnPtr _buf_column = nullptr;
};


class DictColumnWriter final : public ColumnWriter {
public:
    DictColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                       std::unique_ptr<ScalarColumnWriter> column_writer);

    ~DictColumnWriter() override = default;

    Status init() override { return _scalar_column_writer->init(); };

    Status append(const Column& column) override;

    // Speculate char/varchar encoding and reset encoding
    Status speculate_column_and_set_encoding(const Column& column);

    // Speculate encoding
    template <LogicalType Type>
    inline EncodingTypePB speculate_encoding(const Column& column);

    Status finish_current_page() override { return _scalar_column_writer->finish_current_page(); };

    uint64_t estimate_buffer_size() override { return _scalar_column_writer->estimate_buffer_size(); };

    // finish append data
    Status finish() override;

    Status write_data() override { return _scalar_column_writer->write_data(); };
    Status write_ordinal_index() override { return _scalar_column_writer->write_ordinal_index(); };
    Status write_zone_map() override { return _scalar_column_writer->write_zone_map(); };
    Status write_bitmap_index() override { return _scalar_column_writer->write_bitmap_index(); };
    Status write_bloom_filter_index() override { return _scalar_column_writer->write_bloom_filter_index(); };

    ordinal_t get_next_rowid() const override { return _scalar_column_writer->get_next_rowid(); };

    bool is_global_dict_valid() override { return _scalar_column_writer->is_global_dict_valid(); }

    uint64_t total_mem_footprint() const override { return _scalar_column_writer->total_mem_footprint(); }

private:
    std::unique_ptr<ScalarColumnWriter> _scalar_column_writer;
    bool _is_speculated = false;
    ColumnPtr _buf_column = nullptr;
};

StatusOr<std::unique_ptr<ColumnWriter>> ColumnWriter::create(const ColumnWriterOptions& opts,
                                                             const TabletColumn* column, WritableFile* wfile) {
    TypeInfoPtr type_info = get_type_info(*column);
    DCHECK(type_info.get() != nullptr);
    if (is_string_type(delegate_type(column->type()))) {
        ColumnWriterOptions str_opts = opts;
        str_opts.need_speculate_encoding = true;
        auto column_writer = std::make_unique<ScalarColumnWriter>(str_opts, type_info, wfile);
        return std::make_unique<StringColumnWriter>(str_opts, std::move(type_info), std::move(column_writer));
    } else if (is_default_dict_encoding(column->type())) {
        DCHECK(column->type() != TYPE_VARCHAR);
        DCHECK(column->type() != TYPE_CHAR);
        ColumnWriterOptions dict_opts = opts;
        dict_opts.need_speculate_encoding = true;
        auto column_writer = std::make_unique<ScalarColumnWriter>(dict_opts, type_info, wfile);
        return std::make_unique<DictColumnWriter>(dict_opts, std::move(type_info), std::move(column_writer));
    } else if (is_scalar_field_type(delegate_type(column->type()))) {
        return std::make_unique<ScalarColumnWriter>(opts, std::move(type_info), wfile);
    } else {
        switch (column->type()) {
        case LogicalType::TYPE_ARRAY:
            return create_array_column_writer(opts, std::move(type_info), column, wfile);
        case LogicalType::TYPE_MAP:
            return create_map_column_writer(opts, std::move(type_info), column, wfile);
        case LogicalType::TYPE_STRUCT:
            return create_struct_column_writer(opts, std::move(type_info), column, wfile);
        default:
            return Status::NotSupported("unsupported type for ColumnWriter: " + std::to_string(type_info->type()));
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////

ScalarColumnWriter::ScalarColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info, WritableFile* wfile)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _opts(opts),
          _wfile(wfile),
          _curr_page_format(_opts.page_format),
          _data_size(0) {
    // these opts.meta fields should be set by client
    DCHECK(opts.meta->has_column_id());
    DCHECK(opts.meta->has_unique_id());
    DCHECK(opts.meta->has_type());
    DCHECK(opts.meta->has_length());
    DCHECK(opts.meta->has_encoding());
    DCHECK(opts.meta->has_compression());
    DCHECK(opts.meta->has_is_nullable());
    DCHECK(wfile != nullptr);
}

ScalarColumnWriter::~ScalarColumnWriter() {
    // delete all pages
    Page* page = _pages.head;
    while (page != nullptr) {
        Page* next_page = page->next;
        delete page;
        page = next_page;
    }
}

Status ScalarColumnWriter::init() {
    RETURN_IF_ERROR(get_block_compression_codec(_opts.meta->compression(), &_compress_codec));

    if (!_opts.need_speculate_encoding) {
        auto st = set_encoding(_opts.meta->encoding());
        CHECK(st.ok()) << st;
    }
    // create ordinal builder
    _ordinal_index_builder = std::make_unique<OrdinalIndexWriter>();
    // create null bitmap builder
    if (is_nullable()) {
        _null_map_builder_v1 = std::make_unique<NullMapRLEBuilder>();
        NullEncodingPB default_null_encoding = NullEncodingPB::BITSHUFFLE_NULL;
        if (config::null_encoding == 1) {
            default_null_encoding = NullEncodingPB::LZ4_NULL;
        }
        _null_map_builder_v2 = std::make_unique<NullFlagsBuilder>(default_null_encoding);
    }
    if (_opts.need_zone_map) {
        _has_index_builder = true;
        _zone_map_index_builder = ZoneMapIndexWriter::create(type_info());
    }
    if (_opts.need_bitmap_index) {
        _has_index_builder = true;
        RETURN_IF_ERROR(BitmapIndexWriter::create(_type_info, &_bitmap_index_builder));
    }
    if (_opts.need_bloom_filter) {
        _has_index_builder = true;
        RETURN_IF_ERROR(BloomFilterIndexWriter::create(BloomFilterOptions(), _type_info, &_bloom_filter_index_builder));
    }
    return Status::OK();
}

uint64_t ScalarColumnWriter::estimate_buffer_size() {
    uint64_t size = _data_size;
    // In string type _page_builder in speculating may nullptr
    if (_page_builder != nullptr) {
        size += _page_builder->size();
    }
    if (is_nullable()) {
        size += _null_map_builder_v1->has_null() ? _null_map_builder_v1->size() : 0;
        size += _null_map_builder_v2->has_null() ? _null_map_builder_v2->size() : 0;
    }
    size += _ordinal_index_builder->size();
    if (_zone_map_index_builder != nullptr) {
        size += _zone_map_index_builder->size();
    }
    if (_bitmap_index_builder != nullptr) {
        size += _bitmap_index_builder->size();
    }
    if (_bloom_filter_index_builder != nullptr) {
        size += _bloom_filter_index_builder->size();
    }
    return size;
}

Status ScalarColumnWriter::finish() {
    if (_encoding_info->encoding() == DICT_ENCODING && _opts.global_dict != nullptr) {
        _is_global_dict_valid = _page_builder->is_valid_global_dict(_opts.global_dict);
    } else {
        _is_global_dict_valid = false;
    }
    RETURN_IF_ERROR(finish_current_page());
    _opts.meta->set_num_rows(_next_rowid);
    _opts.meta->set_total_mem_footprint(_total_mem_footprint);
    return Status::OK();
}

Status ScalarColumnWriter::write_data() {
    // dict will be load before data,
    // so write column dict first
    if (_encoding_info->encoding() == DICT_ENCODING) {
        faststring* dict_body = _page_builder->get_dictionary_page();
        if (UNLIKELY(dict_body == nullptr)) {
            return Status::InternalError("dictionary page is nullptr");
        }

        PageFooterPB footer;
        footer.set_type(DICTIONARY_PAGE);
        footer.set_uncompressed_size(dict_body->size());
        if (_encoding_info->type() == TYPE_CHAR || _encoding_info->type() == TYPE_VARCHAR) {
            footer.mutable_dict_page_footer()->set_encoding(PLAIN_ENCODING);
        } else {
            footer.mutable_dict_page_footer()->set_encoding(BIT_SHUFFLE);
        }

        PagePointer dict_pp;
        std::vector<Slice> body{Slice(*dict_body)};
        RETURN_IF_ERROR(PageIO::compress_and_write_page(_compress_codec, _opts.compression_min_space_saving, _wfile,
                                                        body, footer, &dict_pp));
        dict_pp.to_proto(_opts.meta->mutable_dict_page());
    }
    _opts.meta->set_all_dict_encoded(_page_builder->all_dict_encoded());

    Page* page = _pages.head;
    while (page != nullptr) {
        RETURN_IF_ERROR(_write_data_page(page));
        Page* last_page = page;
        page = page->next;
        delete last_page;
        _pages.head = page;
    }
    return Status::OK();
}

// This method should be called when _page_builder is empty
inline Status ScalarColumnWriter::set_encoding(const EncodingTypePB& encoding) {
    if (_encoding_info != nullptr && _encoding_info->encoding() == encoding) {
        return Status::OK();
    }
    if (_page_builder != nullptr && _page_builder->size() != 0) {
        return Status::InternalError("reset encoding failed.");
    }
    PageBuilder* page_builder = nullptr;
    RETURN_IF_ERROR(EncodingInfo::get(type_info()->type(), encoding, &_encoding_info));
    _opts.meta->set_encoding(_encoding_info->encoding());
    PageBuilderOptions opts;
    opts.data_page_size = _opts.data_page_size;
    RETURN_IF_ERROR(_encoding_info->create_page_builder(opts, &page_builder));
    if (page_builder == nullptr) {
        return Status::NotSupported(strings::Substitute("Failed to create page builder for type $0 and encoding $1",
                                                        type_info()->type(), _opts.meta->encoding()));
    }
    // should store more concrete encoding type instead of DEFAULT_ENCODING
    // because the default encoding of a data type can be changed in the future
    DCHECK_NE(_opts.meta->encoding(), DEFAULT_ENCODING);
    _page_builder.reset(page_builder);
    return Status::OK();
}

Status ScalarColumnWriter::write_ordinal_index() {
    return _ordinal_index_builder->finish(_wfile, _opts.meta->add_indexes());
}

Status ScalarColumnWriter::write_zone_map() {
    if (_zone_map_index_builder != nullptr) {
        return _zone_map_index_builder->finish(_wfile, _opts.meta->add_indexes());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_bitmap_index() {
    if (_bitmap_index_builder != nullptr) {
        return _bitmap_index_builder->finish(_wfile, _opts.meta->add_indexes());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_bloom_filter_index() {
    if (_bloom_filter_index_builder != nullptr) {
        return _bloom_filter_index_builder->finish(_wfile, _opts.meta->add_indexes());
    }
    return Status::OK();
}

// write a data page into file and update ordinal index
Status ScalarColumnWriter::_write_data_page(Page* page) {
    PagePointer pp;
    std::vector<Slice> compressed_body;
    for (auto& data : page->data) {
        compressed_body.push_back(data.slice());
    }
    RETURN_IF_ERROR(PageIO::write_page(_wfile, compressed_body, page->footer, &pp));
    _ordinal_index_builder->append_entry(page->footer.data_page_footer().first_ordinal(), pp);
    return Status::OK();
}

Status ScalarColumnWriter::finish_current_page() {
    if (_zone_map_index_builder != nullptr) {
        RETURN_IF_ERROR(_zone_map_index_builder->flush());
    }

    if (_bloom_filter_index_builder != nullptr) {
        RETURN_IF_ERROR(_bloom_filter_index_builder->flush());
    }

    // build data page body : encoded values + [nullmap]
    std::vector<Slice> body;
    faststring* encoded_values = _page_builder->finish();
    body.emplace_back(*encoded_values);

    OwnedSlice nullmap;
    if (is_nullable() && _curr_page_format == 1) {
        if (_null_map_builder_v1->has_null()) {
            nullmap = _null_map_builder_v1->finish();
            body.push_back(nullmap.slice());
        }
    } else if (is_nullable() && (_curr_page_format == 2)) {
        DCHECK_EQ(_page_builder->count(), _null_map_builder_v2->size());
        DCHECK_EQ(_null_map_builder_v2->size(), _next_rowid - _first_rowid);
        if (_null_map_builder_v2->has_null()) {
            nullmap = _null_map_builder_v2->finish();
            if (!nullmap.is_loaded()) {
                return Status::Corruption("encode null flags failed");
            }
            body.push_back(nullmap.slice());
        }
    }

    // prepare data page footer
    std::unique_ptr<Page> page(new Page());
    page->footer.set_type(DATA_PAGE);
    page->footer.set_uncompressed_size(Slice::compute_total_size(body));
    starrocks::DataPageFooterPB* data_page_footer = page->footer.mutable_data_page_footer();
    data_page_footer->set_first_ordinal(_first_rowid);
    data_page_footer->set_num_values(_next_rowid - _first_rowid);
    data_page_footer->set_nullmap_size(nullmap.slice().size);
    data_page_footer->set_format_version(_curr_page_format);
    data_page_footer->set_corresponding_element_ordinal(_element_ordinal);
    if (is_nullable() && _curr_page_format >= 2) {
        // for page format v2 or above, use the encoding type of config::null_encoding
        data_page_footer->set_null_encoding(_null_map_builder_v2->null_encoding());
    }
    // trying to compress page body
    faststring compressed_body;
    RETURN_IF_ERROR(
            PageIO::compress_page_body(_compress_codec, _opts.compression_min_space_saving, body, &compressed_body));
    if (compressed_body.size() == 0) {
        // page body is uncompressed
        double space_saving =
                1.0 - static_cast<double>(encoded_values->size()) / static_cast<double>(encoded_values->capacity());
        // when the page is first compressed by bitshuffle, the compression effect of lz4 is not obvious.
        // Then the compressed page (may be much larger then the actual size,
        // e.g. the page is 6K, but the compressed page allocated is 256K),
        // is swaped to the encoded_values for opt the memory allocation.
        // In this scenario, the page is all 256K, bug actual data size is 6K.
        // So, we should shrink the page to the right size.
        if (space_saving >= _opts.compression_min_space_saving) {
            encoded_values->shrink_to_fit();
        }

        page->data.emplace_back(encoded_values->build());
        page->data.emplace_back(std::move(nullmap));
        // Move the ownership of the internal storage of |compressed_body| to |encoded_values|,
        // in order to reduce the internal memory allocations/deallocations of |_page_builder|.
        encoded_values->swap(compressed_body);
    } else {
        // page body is compressed
        page->data.emplace_back(compressed_body.build());
    }

    _push_back_page(page.release());

    if (is_nullable()) {
        size_t num_data = (_curr_page_format == 1) ? _page_builder->count() : _null_map_builder_v2->data_count();
        size_t num_null = data_page_footer->num_values() - num_data;
        // If more than 80% of the current page is NULL records, using format 1 for the next page,
        // otherwise using format 2.
        _curr_page_format = (num_null > 4 * num_data) ? 1 : 2;
    }
    if (is_nullable()) {
        _null_map_builder_v1->reset();
        _null_map_builder_v2->reset();
    }
    _page_builder->reset();
    _first_rowid = _next_rowid;

    return Status::OK();
}

Status ScalarColumnWriter::append(const Column& column) {
    _total_mem_footprint += column.byte_size();

    const uint8_t* ptr = column.raw_data();
    const uint8_t* null =
            is_nullable() ? down_cast<const NullableColumn*>(&column)->null_column()->raw_data() : nullptr;
    return append(ptr, null, column.size(), column.has_null());
}

Status ScalarColumnWriter::append_array_offsets(const Column& column) {
    _total_mem_footprint += column.byte_size();

    // Write offset column, it's only used in ArrayColumn
    // [1, 2, 3], [4, 5, 6]
    // In memory, it will be transformed by actual offset(0, 3, 6)
    // In disk, offset is stored as length array(3, 3)
    auto& offsets = down_cast<const UInt32Column&>(column);
    auto& data = offsets.get_data();

    std::vector<uint32_t> array_size;
    raw::make_room(&array_size, offsets.size() - 1);

    for (size_t i = 0; i < offsets.size() - 1; ++i) {
        array_size[i] = data[i + 1] - data[i];
    }

    const auto* raw_data = reinterpret_cast<const uint8_t*>(array_size.data());
    const size_t field_size = type_info()->size();
    size_t remaining = array_size.size();
    size_t offset_ordinal = 0;
    while (remaining > 0) {
        bool page_full = false;
        size_t num_written = 0;
        num_written = _page_builder->add(raw_data, remaining);
        page_full = num_written < remaining;

        _next_rowid += num_written;
        raw_data += field_size * num_written;
        _previous_ordinal += data[offset_ordinal + num_written] - data[offset_ordinal];
        offset_ordinal += num_written;
        if (page_full) {
            RETURN_IF_ERROR(finish_current_page());
            _element_ordinal = _previous_ordinal;
        }
        remaining -= num_written;
    }
    return Status::OK();
}

Status ScalarColumnWriter::append(const uint8_t* data, const uint8_t* null_flags, size_t count, bool has_null) {
    const size_t field_size = type_info()->size();
    size_t remaining = count;
    while (remaining > 0) {
        bool page_full = false;
        bool has_null_in_page = false;
        size_t num_written = 0;
        if (_curr_page_format == 2) {
            num_written = _page_builder->add(data, remaining);
            page_full = num_written < remaining;
            if (_null_map_builder_v2 != nullptr) {
                _null_map_builder_v2->add_null_flags(null_flags, num_written);
                // The input data may be split into multiple pages, so |has_null| is true does
                // not mean the current page has null, |null_flags| must be checked.
                has_null_in_page = has_null && (nullptr != memchr(null_flags, 1, num_written));
                has_null_in_page |= _null_map_builder_v2->has_null();
                _null_map_builder_v2->set_has_null(has_null_in_page);
            }
        } else if (!has_null) {
            num_written = _page_builder->add(data, remaining);
            page_full = num_written < remaining;
            if (_null_map_builder_v1 != nullptr) {
                _null_map_builder_v1->add_run(false, num_written);
            }
        } else {
            const uint8_t* ptr = data;
            ByteIterator iter(null_flags, std::min(remaining, _opts.data_page_size / field_size));
            for (auto pair = iter.next(); pair.first > 0 && !page_full; pair = iter.next()) {
                auto [run, is_null] = pair;
                size_t num_add = run;
                if (!is_null) {
                    num_add = _page_builder->add(ptr, run);
                    _null_map_builder_v1->add_run(false, run);
                } else {
                    _null_map_builder_v1->add_run(true, run);
                    has_null_in_page = true;
                }
                page_full = num_add < run;
                num_written += num_add;
                ptr += field_size * num_add;
            }
        }

        if (_has_index_builder & has_null_in_page) {
            const uint8_t* pdata = data;
            ByteIterator iter(null_flags, num_written);
            for (auto pair = iter.next(); pair.first > 0; pair = iter.next()) {
                auto [run, is_null] = pair;
                if (is_null) {
                    INDEX_ADD_NULLS(_zone_map_index_builder, run);
                    INDEX_ADD_NULLS(_bitmap_index_builder, run);
                    INDEX_ADD_NULLS(_bloom_filter_index_builder, run);
                } else {
                    INDEX_ADD_VALUES(_zone_map_index_builder, pdata, run);
                    INDEX_ADD_VALUES(_bitmap_index_builder, pdata, run);
                    INDEX_ADD_VALUES(_bloom_filter_index_builder, pdata, run);
                }
                pdata += type_info()->size() * run;
            }
        } else {
            INDEX_ADD_VALUES(_zone_map_index_builder, data, num_written);
            INDEX_ADD_VALUES(_bitmap_index_builder, data, num_written);
            INDEX_ADD_VALUES(_bloom_filter_index_builder, data, num_written);
        }

        _next_rowid += num_written;
        data += field_size * num_written;
        null_flags += num_written;
        if (page_full) {
            RETURN_IF_ERROR(finish_current_page());
        }
        remaining -= num_written;
    }
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

StringColumnWriter::StringColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                       std::unique_ptr<ScalarColumnWriter> column_writer)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _scalar_column_writer(std::move(column_writer)) {}

Status StringColumnWriter::append(const Column& column) {
    if (config::enable_check_string_lengths) {
        RETURN_IF_ERROR(check_string_lengths(column));
    }
    if (_is_speculated) {
        return _scalar_column_writer->append(column);
    }

    if (_buf_column == nullptr) {
        // First column size is greater than speculate size or byte size large than UINT32_MAX.
        // Because if columns' byte size than UINT32_MAX, that will cause BinaryColumn<uint32_t> overflow
        if (column.size() >= config::dictionary_speculate_min_chunk_size || column.byte_size() >= UINT32_MAX) {
            _is_speculated = true;
            speculate_column_and_set_encoding(column);
            return _scalar_column_writer->append(column);
        } else {
            _buf_column = column.clone_empty();
            _buf_column->append(column, 0, column.size());
            return Status::OK();
        }
    }
    if (column.size() + _buf_column->size() >= config::dictionary_speculate_min_chunk_size ||
        column.byte_size() + _buf_column->byte_size() >= UINT32_MAX) {
        // If it is predicted that _buf_column will exceed the limit after append column,
        // skip append column
        _is_speculated = true;
        speculate_column_and_set_encoding(*_buf_column);
        RETURN_IF_ERROR(_scalar_column_writer->append(*_buf_column));
        _buf_column.reset();
        RETURN_IF_ERROR(_scalar_column_writer->append(column));
    } else {
        _buf_column->append(column, 0, column.size());
    }
    return Status::OK();
}

inline void StringColumnWriter::speculate_column_and_set_encoding(const Column& column) {
    Status st;
    if (column.is_nullable()) {
        const auto& data_col = down_cast<const NullableColumn&>(column).data_column();
        const auto& bin_col = down_cast<BinaryColumn&>(*data_col);
        const auto detect_encoding = speculate_string_encoding(bin_col);
        st = _scalar_column_writer->set_encoding(detect_encoding);
    } else if (column.is_binary()) {
        const auto& bin_col = down_cast<const BinaryColumn&>(column);
        auto detect_encoding = speculate_string_encoding(bin_col);
        st = _scalar_column_writer->set_encoding(detect_encoding);
    }
    CHECK(st.ok()) << st;
}

inline EncodingTypePB StringColumnWriter::speculate_string_encoding(const BinaryColumn& bin_col) {
    const size_t dictionary_min_rowcount = 256;

    auto row_count = bin_col.size();
    auto ratio = config::dictionary_encoding_ratio;
    auto max_card = static_cast<size_t>(static_cast<double>(row_count) * ratio);

    if (row_count > dictionary_min_rowcount) {
        phmap::flat_hash_set<size_t> hash_set;
        for (size_t i = 0; i < row_count; i++) {
            size_t hash = SliceHash()(bin_col.get_slice(i));
            hash_set.insert(hash);
            if (hash_set.size() > max_card) {
                return PLAIN_ENCODING;
            }
        }
    }

    return DICT_ENCODING;
}

Status StringColumnWriter::finish() {
    if (_is_speculated) {
        return _scalar_column_writer->finish();
    }

    _is_speculated = true;
    if (_buf_column != nullptr) {
        speculate_column_and_set_encoding(*_buf_column);
        Status st = _scalar_column_writer->append(*_buf_column);
        _buf_column.reset();
        if (!st.ok()) {
            return st;
        }
    }

    return _scalar_column_writer->finish();
}

Status StringColumnWriter::check_string_lengths(const Column& column) {
    size_t limit = length();
    auto row_count = column.size();
    const uint8_t* null =
            is_nullable() ? down_cast<const NullableColumn*>(&column)->null_column()->raw_data() : nullptr;
    const BinaryColumn* bin_col;

    if (is_nullable()) {
        const auto& data_col = down_cast<const NullableColumn*>(&column)->data_column();
        bin_col = down_cast<const BinaryColumn*>(data_col.get());
    } else {
        bin_col = down_cast<const BinaryColumn*>(&column);
    }
    for (size_t i = 0; i < row_count; i++) {
        // skip string length check if it is null
        if (null != nullptr && null[i] == starrocks::DATUM_NULL) {
            continue;
        }
        // here we shouldn't use raw_data() api of column to get a vector of slices in advance,
        // because raw_data() will call _build_slices() api, which will create a vector of slices,
        // if there are many StringColumnWriter, each of them will have a vector of slices, which will consume many memory.
        Slice slice = bin_col->get_slice(i);
        if (slice.get_size() > limit) {
            return Status::InvalidArgument(fmt::format("string length({}) > limit({}), string: {}", slice.get_size(),
                                                       limit, slice.to_string()));
        }
    }
    return Status::OK();
}


DictColumnWriter::DictColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                       std::unique_ptr<ScalarColumnWriter> column_writer)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _scalar_column_writer(std::move(column_writer)) {}

Status DictColumnWriter::append(const Column& column) {
    if (_is_speculated) {
        return _scalar_column_writer->append(column);
    }

    if (_buf_column == nullptr) {
        // first column size is greater than speculate size
        if (column.size() >= config::dictionary_speculate_min_chunk_size) {
            _is_speculated = true;
            RETURN_IF_ERROR(speculate_column_and_set_encoding(column));
            return _scalar_column_writer->append(column);
        } else {
            _buf_column = column.clone_empty();
            _buf_column->append(column, 0, column.size());
            return Status::OK();
        }
    }
    _buf_column->append(column, 0, column.size());
    if (_buf_column->size() < config::dictionary_speculate_min_chunk_size) {
        return Status::OK();
    } else {
        _is_speculated = true;
        RETURN_IF_ERROR(speculate_column_and_set_encoding(*_buf_column));
        Status st = _scalar_column_writer->append(*_buf_column);
        _buf_column.reset();
        return st;
    }
}

inline Status DictColumnWriter::speculate_column_and_set_encoding(const Column& column) {
    Status st;
    EncodingTypePB detect_encoding;
    LogicalType logicalType = type_info()->type();
    switch (logicalType) {
        case TYPE_TINYINT:
            detect_encoding = speculate_encoding<TYPE_TINYINT>(column);
            break;
        case TYPE_SMALLINT:
            detect_encoding = speculate_encoding<TYPE_SMALLINT>(column);
            break;
        case TYPE_INT:
            detect_encoding = speculate_encoding<TYPE_INT>(column);
            break;
        case TYPE_BIGINT:
            detect_encoding = speculate_encoding<TYPE_BIGINT>(column);
            break;
        case TYPE_LARGEINT:
            detect_encoding = speculate_encoding<TYPE_LARGEINT>(column);
            break;
        case TYPE_FLOAT:
            detect_encoding = speculate_encoding<TYPE_FLOAT>(column);
            break;
        case TYPE_DOUBLE:
            detect_encoding = speculate_encoding<TYPE_DOUBLE>(column);
            break;
        case TYPE_DATE:
            detect_encoding = speculate_encoding<TYPE_DATE>(column);
            break;
        case TYPE_DATETIME:
            detect_encoding = speculate_encoding<TYPE_DATETIME>(column);
            break;
        case TYPE_DECIMALV2:
            detect_encoding = speculate_encoding<TYPE_DECIMALV2>(column);
            break;
        default:
            return Status::InternalError(strings::Substitute("$0 type should not use dictionary encoding", logicalType));
    }
    st = _scalar_column_writer->set_encoding(detect_encoding);
    CHECK(st.ok()) << st;
    return st;
}

// The detection logic here uses a set to record the distinct values of a sample column. When the number 
// of distinct values exceeds row_count * ratio, dictionary encoding is no longer used.
// Here, row_count is the number of elements in the sample column, and ratio is set by the user.
template <LogicalType Type>
inline EncodingTypePB DictColumnWriter::speculate_encoding(const Column& column) {
    using ColumnType = typename RunTimeTypeTraits<Type>::ColumnType;
    const ColumnType* bin_col;
    if (column.is_nullable()) {
        const auto& data_col = down_cast<const NullableColumn&>(column).data_column();
        bin_col = &down_cast<ColumnType&>(*data_col);
    } else {
        bin_col = &down_cast<const ColumnType&>(column);
    }
    const size_t dictionary_min_rowcount = 256;

    auto row_count = bin_col->size();
    auto ratio = config::dictionary_encoding_ratio_for_non_string_column;
    auto max_card = static_cast<size_t>(static_cast<double>(row_count) * ratio);

    if (row_count > dictionary_min_rowcount) {
        using CppType = typename RunTimeTypeTraits<Type>::CppType;
        phmap::flat_hash_set<CppType> hash_set;
        for (size_t i = 0; i < row_count; i++) {
            CppType value = bin_col->get_data()[i];
            hash_set.insert(value);
            if (hash_set.size() > max_card) {
                return BIT_SHUFFLE;
            }
        }
    }

    return DICT_ENCODING;
}

Status DictColumnWriter::finish() {
    if (_is_speculated) {
        return _scalar_column_writer->finish();
    }

    _is_speculated = true;
    if (_buf_column != nullptr) {
        RETURN_IF_ERROR(speculate_column_and_set_encoding(*_buf_column));
        Status st = _scalar_column_writer->append(*_buf_column);
        _buf_column.reset();
        if (!st.ok()) {
            return st;
        }
    }

    return _scalar_column_writer->finish();
}

} // namespace starrocks
