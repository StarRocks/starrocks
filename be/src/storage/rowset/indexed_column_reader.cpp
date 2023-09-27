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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/indexed_column_reader.cpp

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

#include "storage/rowset/indexed_column_reader.h"

#include "fs/fs.h"
#include "gutil/strings/substitute.h" // for Substitute
#include "storage/key_coder.h"
#include "storage/rowset/encoding_info.h" // for EncodingInfo
#include "storage/rowset/page_io.h"

namespace starrocks {

using strings::Substitute;

Status IndexedColumnReader::load(const IndexReadOptions& opts) {
    _type_info = get_type_info((LogicalType)_meta.data_type());
    if (_type_info == nullptr) {
        return Status::NotSupported(strings::Substitute("unsupported type=$0", _meta.data_type()));
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info->type(), _meta.encoding(), &_encoding_info));
    RETURN_IF_ERROR(get_block_compression_codec(_meta.compression(), &_compress_codec));
    _validx_key_coder = get_key_coder(_type_info->type());

    // read and parse ordinal index page when exists
    if (_meta.has_ordinal_index_meta()) {
        if (_meta.ordinal_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.ordinal_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(load_index_page(opts, _meta.ordinal_index_meta().root_page(), &_ordinal_index_page_handle,
                                            &_ordinal_index_reader));
            _has_index_page = true;
        }
    }

    // read and parse value index page when exists
    if (_meta.has_value_index_meta()) {
        if (_meta.value_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.value_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(load_index_page(opts, _meta.value_index_meta().root_page(), &_value_index_page_handle,
                                            &_value_index_reader));
            _has_index_page = true;
        }
    }
    _num_values = _meta.num_values();
    return Status::OK();
}

Status IndexedColumnReader::load_index_page(const IndexReadOptions& opts, const PagePointerPB& pp, PageHandle* handle,
                                            IndexPageReader* reader) {
    Slice body;
    PageFooterPB footer;
    RETURN_IF_ERROR(read_page(opts, PagePointer(pp), handle, &body, &footer));
    RETURN_IF_ERROR(reader->parse(body, footer.index_page_footer()));
    return Status::OK();
}

Status IndexedColumnReader::read_page(const IndexReadOptions& opts, const PagePointer& pp, PageHandle* handle,
                                      Slice* body, PageFooterPB* footer) const {
    PageReadOptions page_opts;
    page_opts.read_file = opts.read_file;
    page_opts.page_pointer = pp;
    page_opts.codec = _compress_codec;
    page_opts.stats = opts.stats;
    page_opts.use_page_cache = opts.use_page_cache;
    page_opts.kept_in_memory = opts.kept_in_memory;
    page_opts.encoding_type = _encoding_info->encoding();
    return PageIO::read_and_decompress_page(page_opts, handle, body, footer);
}

Status IndexedColumnReader::new_iterator(const IndexReadOptions& opts, std::unique_ptr<IndexedColumnIterator>* iter) {
    iter->reset(new IndexedColumnIterator(this, opts));
    return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
IndexedColumnIterator::IndexedColumnIterator(const IndexedColumnReader* reader, const IndexReadOptions& opts)
        : _reader(reader),
          _opts(opts),
          _ordinal_iter(&reader->_ordinal_index_reader),
          _value_iter(&reader->_value_index_reader) {}

Status IndexedColumnIterator::_read_data_page(const PagePointer& pp) {
    PageHandle handle;
    Slice body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_reader->read_page(_opts, pp, &handle, &body, &footer));
    // parse data page
    // note that page_index is not used in IndexedColumnIterator, so we pass 0
    return parse_page(&_data_page, std::move(handle), body, footer.data_page_footer(), _reader->encoding_info(), pp, 0);
}

Status IndexedColumnIterator::seek_to_ordinal(ordinal_t idx) {
    DCHECK(idx >= 0 && idx <= _reader->num_values());

    if (!_reader->support_ordinal_seek()) {
        return Status::NotSupported("no ordinal index");
    }

    // it's ok to seek past the last value
    if (idx == _reader->num_values()) {
        _current_ordinal = idx;
        _seeked = true;
        return Status::OK();
    }

    if (_data_page == nullptr || !_data_page->contains(idx)) {
        // need to read the data page containing row at idx
        if (_reader->_has_index_page) {
            std::string key;
            KeyCoderTraits<TYPE_UNSIGNED_BIGINT>::full_encode_ascending(&idx, &key);
            RETURN_IF_ERROR(_ordinal_iter.seek_at_or_before(key));
            RETURN_IF_ERROR(_read_data_page(_ordinal_iter.current_page_pointer()));
            _current_iter = &_ordinal_iter;
        } else {
            RETURN_IF_ERROR(_read_data_page(_reader->_sole_data_page));
        }
    }

    ordinal_t offset_in_page = idx - _data_page->first_ordinal();
    RETURN_IF_ERROR(_data_page->seek(offset_in_page));
    _current_ordinal = idx;
    _seeked = true;
    return Status::OK();
}

Status IndexedColumnIterator::seek_at_or_after(const void* key, bool* exact_match) {
    if (!_reader->support_value_seek()) {
        return Status::NotSupported("no value index");
    }

    if (_reader->num_values() == 0) {
        return Status::NotFound("value index is empty ");
    }

    bool load_data_page = false;
    PagePointer data_page_pp;
    if (_reader->_has_index_page) {
        // seek index to determine the data page to seek
        std::string encoded_key;
        _reader->_validx_key_coder->full_encode_ascending(key, &encoded_key);
        RETURN_IF_ERROR(_value_iter.seek_at_or_after(encoded_key));
        data_page_pp = _value_iter.current_page_pointer();
        _current_iter = &_value_iter;
        if (_data_page == nullptr || _data_page->page_pointer() != data_page_pp) {
            // load when it's not the same with the current
            load_data_page = true;
        }
    } else if (!_data_page) {
        // no index page, load data page for the first time
        load_data_page = true;
        data_page_pp = PagePointer(_reader->_sole_data_page);
    }

    if (load_data_page) {
        RETURN_IF_ERROR(_read_data_page(data_page_pp));
    }

    // seek inside data page
    RETURN_IF_ERROR(_data_page->data_decoder()->seek_at_or_after_value(key, exact_match));
    RETURN_IF_ERROR(_data_page->seek(_data_page->data_decoder()->current_index()));
    _current_ordinal = _data_page->first_ordinal() + _data_page->offset();
    DCHECK(_data_page->contains(_current_ordinal));
    _seeked = true;
    return Status::OK();
}

Status IndexedColumnIterator::next_batch(size_t* n, Column* column) {
    DCHECK(_seeked);
    if (_current_ordinal == _reader->num_values()) {
        *n = 0;
        return Status::OK();
    }

    size_t remaining = *n;
    while (remaining > 0) {
        if (_data_page->remaining() == 0) {
            // trying to read next data page
            if (!_reader->_has_index_page) {
                break; // no more data page
            }
            bool has_next = _current_iter->move_next();
            if (!has_next) {
                break; // no more data page
            }
            RETURN_IF_ERROR(_read_data_page(_current_iter->current_page_pointer()));
        }

        size_t rows_to_read = std::min(_data_page->remaining(), remaining);
        size_t rows_read = rows_to_read;
        RETURN_IF_ERROR(_data_page->read(column, &rows_read));
        DCHECK(rows_to_read == rows_read);
        _current_ordinal += rows_read;
        remaining -= rows_read;
    }
    *n -= remaining;
    _seeked = false;
    return Status::OK();
}

} // namespace starrocks
