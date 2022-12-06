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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/indexed_column_writer.cpp

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

#include "storage/rowset/indexed_column_writer.h"

#include <memory>
#include <string>
#include <utility>

#include "common/logging.h"
#include "fs/fs.h"
#include "storage/key_coder.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/index_page.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/page_pointer.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/compression/block_compression.h"

namespace starrocks {

IndexedColumnWriter::IndexedColumnWriter(const IndexedColumnWriterOptions& options, TypeInfoPtr typeinfo,
                                         WritableFile* wfile)
        : _options(options),
          _typeinfo(std::move(typeinfo)),
          _wfile(wfile),
          _num_values(0),
          _num_data_pages(0),
          _validx_key_coder(nullptr),
          _compress_codec(nullptr) {
    _first_value.resize(_typeinfo->size());
}

IndexedColumnWriter::~IndexedColumnWriter() = default;

Status IndexedColumnWriter::init() {
    const EncodingInfo* encoding_info;
    RETURN_IF_ERROR(EncodingInfo::get(_typeinfo->type(), _options.encoding, &encoding_info));
    _options.encoding = encoding_info->encoding();
    // should store more concrete encoding type instead of DEFAULT_ENCODING
    // because the default encoding of a data type can be changed in the future
    DCHECK_NE(_options.encoding, DEFAULT_ENCODING);

    PageBuilder* data_page_builder;
    RETURN_IF_ERROR(encoding_info->create_page_builder(PageBuilderOptions(), &data_page_builder));
    _data_page_builder.reset(data_page_builder);

    if (_options.write_ordinal_index) {
        _ordinal_index_builder = std::make_unique<IndexPageBuilder>(_options.index_page_size, true);
    }
    if (_options.write_value_index) {
        _value_index_builder = std::make_unique<IndexPageBuilder>(_options.index_page_size, true);
        _validx_key_coder = get_key_coder(_typeinfo->type());
    }

    if (_options.compression != NO_COMPRESSION) {
        RETURN_IF_ERROR(get_block_compression_codec(_options.compression, &_compress_codec));
    }
    return Status::OK();
}

Status IndexedColumnWriter::add(const void* value) {
    if (_options.write_value_index && _data_page_builder->count() == 0) {
        // remember page's first value because it's used to build value index
        _typeinfo->deep_copy(_first_value.data(), value, &_mem_pool);
    }
    (void)_data_page_builder->add(reinterpret_cast<const uint8_t*>(value), 1);
    _num_values++;
    if (_data_page_builder->is_page_full()) {
        RETURN_IF_ERROR(_finish_current_data_page());
    }
    return Status::OK();
}

Status IndexedColumnWriter::_finish_current_data_page() {
    auto num_values_in_page = _data_page_builder->count();
    if (num_values_in_page == 0) {
        return Status::OK();
    }
    ordinal_t first_ordinal = _num_values - num_values_in_page;

    // IndexedColumn doesn't have NULLs, thus data page body only contains encoded values
    faststring* page_body = _data_page_builder->finish();

    PageFooterPB footer;
    footer.set_type(DATA_PAGE);
    footer.set_uncompressed_size(page_body->size());
    footer.mutable_data_page_footer()->set_first_ordinal(first_ordinal);
    footer.mutable_data_page_footer()->set_num_values(num_values_in_page);
    footer.mutable_data_page_footer()->set_nullmap_size(0);

    RETURN_IF_ERROR(PageIO::compress_and_write_page(_compress_codec, _options.compression_min_space_saving, _wfile,
                                                    {*page_body}, footer, &_last_data_page));
    _num_data_pages++;

    if (_options.write_ordinal_index) {
        std::string key;
        KeyCoderTraits<TYPE_UNSIGNED_BIGINT>::full_encode_ascending(&first_ordinal, &key);
        _ordinal_index_builder->add(key, _last_data_page);
    }

    if (_options.write_value_index) {
        std::string key;
        _validx_key_coder->full_encode_ascending(_first_value.data(), &key);
        // TODO short separate key optimize
        _value_index_builder->add(key, _last_data_page);
        // TODO record last key in short separate key optimize
    }
    _data_page_builder->reset();
    return Status::OK();
}

Status IndexedColumnWriter::finish(IndexedColumnMetaPB* meta) {
    RETURN_IF_ERROR(_finish_current_data_page());
    if (_options.write_ordinal_index) {
        RETURN_IF_ERROR(_flush_index(_ordinal_index_builder.get(), meta->mutable_ordinal_index_meta()));
    }
    if (_options.write_value_index) {
        RETURN_IF_ERROR(_flush_index(_value_index_builder.get(), meta->mutable_value_index_meta()));
    }
    meta->set_data_type(_typeinfo->type());
    meta->set_encoding(_options.encoding);
    meta->set_num_values(_num_values);
    meta->set_compression(_options.compression);
    return Status::OK();
}

Status IndexedColumnWriter::_flush_index(IndexPageBuilder* index_builder, BTreeMetaPB* meta) {
    if (_num_data_pages <= 1) {
        meta->set_is_root_data_page(true);
        _last_data_page.to_proto(meta->mutable_root_page());
    } else {
        OwnedSlice page_body;
        PageFooterPB page_footer;
        index_builder->finish(&page_body, &page_footer);

        PagePointer pp;
        RETURN_IF_ERROR(PageIO::compress_and_write_page(_compress_codec, _options.compression_min_space_saving, _wfile,
                                                        {page_body.slice()}, page_footer, &pp));

        meta->set_is_root_data_page(false);
        pp.to_proto(meta->mutable_root_page());
    }
    return Status::OK();
}

} // namespace starrocks
