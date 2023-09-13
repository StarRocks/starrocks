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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/parsed_page.cpp

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

#include "storage/rowset/parsed_page.h"

#include <fmt/format.h>

#include <memory>

#include "column/nullable_column.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "storage/rowset/binary_dict_page.h"
#include "storage/rowset/bitshuffle_page.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_handle.h"
#include "util/compression/block_compression.h"
#include "util/rle_encoding.h"

namespace starrocks {

namespace {
class ByteIterator {
public:
    ByteIterator(const uint8_t* bytes, size_t size) : _bytes(bytes), _size(size) {}

    size_t next(uint8_t* value) {
        if (UNLIKELY(_pos == _size)) {
            return 0;
        }
        size_t prev = _pos++;
        while (_pos < _size && _bytes[_pos] == _bytes[prev]) {
            ++_pos;
        }
        *value = _bytes[prev];
        return _pos - prev;
    }

private:
    const uint8_t* _bytes;
    const size_t _size;
    size_t _pos{0};
};
} // namespace

class ParsedPageV1 : public ParsedPage {
public:
    ~ParsedPageV1() override = default;

    Status seek(ordinal_t offset) override {
        if (_offset_in_page == offset) {
            return Status::OK();
        }
        ordinal_t pos_in_data = offset;
        if (_has_null) {
            ordinal_t offset_in_data = 0;
            ordinal_t skips = offset;

            if (offset > _offset_in_page) {
                // forward, reuse null bitmap
                skips = offset - _offset_in_page;
                offset_in_data = _data_decoder->current_index();
            } else {
                _null_decoder = RleDecoder<bool>((const uint8_t*)_null_bitmap.data, _null_bitmap.size, 1);
            }

            auto skip_nulls = _null_decoder.Skip(skips);
            pos_in_data = offset_in_data + skips - skip_nulls;
        }

        RETURN_IF_ERROR(_data_decoder->seek_to_position_in_page(pos_in_data));
        _offset_in_page = offset;
        return Status::OK();
    }

    Status read(Column* column, size_t* count) override {
        *count = std::min(*count, remaining());
        size_t nrows_to_read = *count;
        if (_has_null) {
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _null_decoder.GetNextRun(&is_null, nrows_to_read);
                size_t expect = this_run;
                if (!is_null) {
                    RETURN_IF_ERROR(_data_decoder->next_batch(&this_run, column));
                    DCHECK_EQ(expect, this_run);
                } else {
                    CHECK(column->append_nulls(this_run));
                }

                nrows_to_read -= this_run;
                _offset_in_page += this_run;
            }
        } else {
            RETURN_IF_ERROR(_data_decoder->next_batch(&nrows_to_read, column));
            DCHECK_EQ(nrows_to_read, *count);
            _offset_in_page += nrows_to_read;
        }
        return Status::OK();
    }

    Status read(Column* column, const SparseRange<>& range) override {
        DCHECK_LE(range.span_size(), remaining());
        if (_has_null) {
            SparseRangeIterator<> iter = range.new_iterator();
            size_t to_read = range.span_size();
            while (to_read > 0) {
                Range<> r = iter.next(to_read);
                RETURN_IF_ERROR(seek(r.begin()));
                size_t n = r.span_size();
                RETURN_IF_ERROR(read(column, &n));
                to_read -= r.span_size();
            }
        } else {
            RETURN_IF_ERROR(_data_decoder->next_batch(range, column));
            _offset_in_page = range.end();
        }
        return Status::OK();
    }

    Status read_dict_codes(Column* column, size_t* count) override {
        *count = std::min(*count, remaining());
        size_t nrows_to_read = *count;
        if (_has_null) {
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _null_decoder.GetNextRun(&is_null, nrows_to_read);
                size_t expect = this_run;
                if (!is_null) {
                    RETURN_IF_ERROR(_data_decoder->next_dict_codes(&this_run, column));
                    DCHECK_EQ(expect, this_run);
                } else {
                    // cannot use `append_nulls` here, since it will fill the code column with default
                    // values, 0 normally, which can not be distinguished from others.
                    const int32_t invalid_code = -1;
                    const size_t idx = column->size();
                    for (size_t i = 0; i < this_run; i++) {
                        size_t nappend = column->append_numbers(&invalid_code, sizeof(invalid_code));
                        bool ok = column->set_null(idx + i);
                        DCHECK_EQ(1u, nappend);
                        DCHECK(ok);
                    }
                }
                nrows_to_read -= this_run;
                _offset_in_page += this_run;
            }
        } else {
            RETURN_IF_ERROR(_data_decoder->next_dict_codes(&nrows_to_read, column));
            DCHECK_EQ(nrows_to_read, *count);
            _offset_in_page += nrows_to_read;
        }
        return Status::OK();
    }

    Status read_dict_codes(Column* column, const SparseRange<>& range) override {
        DCHECK_LE(range.span_size(), remaining());
        if (_has_null) {
            size_t to_read = range.span_size();
            SparseRangeIterator<> iter = range.new_iterator();
            while (to_read > 0) {
                Range<> r = iter.next(to_read);
                RETURN_IF_ERROR(seek(r.begin()));
                size_t n = r.span_size();
                RETURN_IF_ERROR(read_dict_codes(column, &n));
                to_read -= r.span_size();
            }
        } else {
            RETURN_IF_ERROR(_data_decoder->next_dict_codes(range, column));
            _offset_in_page = range.end();
        }

        return Status::OK();
    }

private:
    friend Status parse_page_v1(std::unique_ptr<ParsedPage>* result, PageHandle handle, const Slice& body,
                                const DataPageFooterPB& footer, const EncodingInfo* encoding,
                                const PagePointer& page_pointer, uint32_t page_index);

    bool _has_null = false;
    Slice _null_bitmap;
    RleDecoder<bool> _null_decoder;
    PageHandle _page_handle;
};

class ParsedPageV2 : public ParsedPage {
public:
    Status seek(ordinal_t offset) override {
        DCHECK_EQ(_offset_in_page, _data_decoder->current_index());
        RETURN_IF_ERROR(_data_decoder->seek_to_position_in_page(offset));
        _offset_in_page = offset;
        return Status::OK();
    }

    Status read(Column* column, size_t* count) override {
        DCHECK_EQ(_offset_in_page, _data_decoder->current_index());
        if (_null_flags.size() == 0) {
            RETURN_IF_ERROR(_data_decoder->next_batch(count, column));
        } else {
            auto nc = down_cast<NullableColumn*>(column);
            RETURN_IF_ERROR(_data_decoder->next_batch(count, nc->data_column().get()));
            nc->null_column()->append_numbers(_null_flags.data() + _offset_in_page, *count);
            nc->update_has_null();
        }
        _offset_in_page += *count;
        return Status::OK();
    }

    Status read(Column* column, const SparseRange<>& range) override {
        DCHECK_EQ(_offset_in_page, range.begin());
        DCHECK_EQ(_offset_in_page, _data_decoder->current_index());
        if (_null_flags.size() == 0) {
            RETURN_IF_ERROR(_data_decoder->next_batch(range, column));
            _offset_in_page = range.end();
        } else {
            auto nc = down_cast<NullableColumn*>(column);
            RETURN_IF_ERROR(_data_decoder->next_batch(range, nc->data_column().get()));
            SparseRangeIterator<> iter = range.new_iterator();
            size_t size = range.span_size();
            while (iter.has_more()) {
                _offset_in_page = iter.begin();
                Range<> r = iter.next(size);
                nc->null_column()->append_numbers(_null_flags.data() + _offset_in_page, r.span_size());
                _offset_in_page += r.span_size();
                size -= r.span_size();
            }
            nc->update_has_null();
        }
        return Status::OK();
    }

    Status read_dict_codes(Column* column, size_t* count) override {
        if (_null_flags.size() == 0) {
            RETURN_IF_ERROR(_data_decoder->next_dict_codes(count, column));
        } else {
            auto nc = down_cast<NullableColumn*>(column);
            RETURN_IF_ERROR(_data_decoder->next_dict_codes(count, nc->data_column().get()));
            (void)nc->null_column()->append_numbers(_null_flags.data() + _offset_in_page, *count);
            nc->update_has_null();
        }
        _offset_in_page += *count;
        return Status::OK();
    }

    Status read_dict_codes(Column* column, const SparseRange<>& range) override {
        DCHECK_EQ(_offset_in_page, range.begin());
        DCHECK_EQ(_offset_in_page, _data_decoder->current_index());

        if (_null_flags.size() == 0) {
            RETURN_IF_ERROR(_data_decoder->next_dict_codes(range, column));
            _offset_in_page = range.end();
        } else {
            auto nc = down_cast<NullableColumn*>(column);
            RETURN_IF_ERROR(_data_decoder->next_dict_codes(range, nc->data_column().get()));
            SparseRangeIterator<> iter = range.new_iterator();
            size_t size = range.span_size();
            while (iter.has_more()) {
                _offset_in_page = iter.begin();
                Range<> r = iter.next(size);
                nc->null_column()->append_numbers(_null_flags.data() + _offset_in_page, r.span_size());
                _offset_in_page += r.span_size();
            }
            nc->update_has_null();
        }

        return Status::OK();
    }

private:
    friend Status parse_page_v2(std::unique_ptr<ParsedPage>* result, PageHandle handle, const Slice& body,
                                const DataPageFooterPB& footer, const EncodingInfo* encoding,
                                const PagePointer& page_pointer, uint32_t page_index);

    faststring _null_flags;
    PageHandle _page_handle;
};

Status parse_page_v1(std::unique_ptr<ParsedPage>* result, PageHandle handle, const Slice& body,
                     const DataPageFooterPB& footer, const EncodingInfo* encoding, const PagePointer& page_pointer,
                     uint32_t page_index) {
    auto page = std::make_unique<ParsedPageV1>();
    page->_page_handle = std::move(handle);

    auto null_size = footer.nullmap_size();
    page->_has_null = null_size > 0;
    if (page->_has_null) {
        page->_null_bitmap = Slice(body.data + body.size - null_size, null_size);
        page->_null_decoder = RleDecoder<bool>((const uint8_t*)page->_null_bitmap.data, null_size, 1);
    }

    Slice data_slice(body.data, body.size - null_size);
    PageDecoder* decoder = nullptr;
    RETURN_IF_ERROR(encoding->create_page_decoder(data_slice, &decoder));
    page->_data_decoder.reset(decoder);
    RETURN_IF_ERROR(page->_data_decoder->init());

    page->_first_ordinal = footer.first_ordinal();
    page->_num_rows = footer.num_values();
    page->_page_pointer = page_pointer;
    page->_page_index = page_index;
    page->_corresponding_element_ordinal = footer.corresponding_element_ordinal();

    *result = std::move(page);
    return Status::OK();
}

Status parse_page_v2(std::unique_ptr<ParsedPage>* result, PageHandle handle, const Slice& body,
                     const DataPageFooterPB& footer, const EncodingInfo* encoding, const PagePointer& page_pointer,
                     uint32_t page_index) {
    auto page = std::make_unique<ParsedPageV2>();
    page->_page_handle = std::move(handle);

    auto null_size = footer.nullmap_size();
    if (null_size > 0) {
        Slice null_flags(body.data + body.size - null_size, null_size);
        // The historical segment file do not null_encoding field in page footer, it use BITSHUFFLE to encode null flags.
        // For compatibility, if has_null_encoding returns false, set null_encoding to BITSHUFFLE_NULL
        // for new segments, footer.null_encoding() will be set by config::null_encoding
        NullEncodingPB null_encoding =
                footer.has_null_encoding() ? footer.null_encoding() : NullEncodingPB::BITSHUFFLE_NULL;
        if (null_encoding == NullEncodingPB::BITSHUFFLE_NULL) {
            // bitshuffle format null flags
            size_t elements = footer.num_values();
            size_t elements_pad = ALIGN_UP(elements, 8u);
            page->_null_flags.resize(elements_pad * sizeof(uint8_t));
            int64_t r = bitshuffle::decompress_lz4(null_flags.data, page->_null_flags.data(), elements_pad,
                                                   sizeof(uint8_t), 0);
            if (r < 0) {
                return Status::Corruption("bitshuffle decompress failed: " + bitshuffle_error_msg(r));
            }
            page->_null_flags.resize(elements);
        } else if (null_encoding == NullEncodingPB::LZ4_NULL) {
            // decompress null flags by lz4
            size_t elements = footer.num_values();
            page->_null_flags.resize(elements * sizeof(uint8_t));
            const BlockCompressionCodec* codec = nullptr;
            CompressionTypePB type = CompressionTypePB::LZ4;
            RETURN_IF_ERROR(get_block_compression_codec(type, &codec));
            Slice decompressed_slice(page->_null_flags.data(), elements);
            RETURN_IF_ERROR(codec->decompress(null_flags, &decompressed_slice));
        } else {
            return Status::Corruption(fmt::format("invalid null encoding: {}", null_encoding));
        }
    }

    Slice data_slice(body.data, body.size - null_size);
    PageDecoder* decoder = nullptr;
    RETURN_IF_ERROR(encoding->create_page_decoder(data_slice, &decoder));
    page->_data_decoder.reset(decoder);
    RETURN_IF_ERROR(page->_data_decoder->init());

    page->_first_ordinal = footer.first_ordinal();
    page->_num_rows = footer.num_values();
    page->_page_pointer = page_pointer;
    page->_page_index = page_index;
    page->_corresponding_element_ordinal = footer.corresponding_element_ordinal();

    *result = std::move(page);
    return Status::OK();
}

Status parse_page(std::unique_ptr<ParsedPage>* result, PageHandle handle, const Slice& body,
                  const DataPageFooterPB& footer, const EncodingInfo* encoding, const PagePointer& page_pointer,
                  uint32_t page_index) {
    uint32_t version = footer.has_format_version() ? footer.format_version() : 1;
    if (version == 1) {
        return parse_page_v1(result, std::move(handle), body, footer, encoding, page_pointer, page_index);
    }
    if (version == 2) {
        return parse_page_v2(result, std::move(handle), body, footer, encoding, page_pointer, page_index);
    }
    return Status::InternalError(strings::Substitute("Unknown page format version $0", version));
}

} // namespace starrocks
