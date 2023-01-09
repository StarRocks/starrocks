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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/indexed_column_reader.h

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

#pragma once

#include <memory>
#include <utility>

#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "storage/rowset/common.h"
#include "storage/rowset/index_page.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/page_pointer.h"
#include "storage/rowset/parsed_page.h"
#include "util/compression/block_compression.h"
#include "util/slice.h"

namespace starrocks {

class KeyCoder;
class TypeInfo;

class EncodingInfo;
class IndexedColumnReader;

class IndexedColumnIterator {
    friend class IndexedColumnReader;

public:
    // Seek to the given ordinal entry. Entry 0 is the first entry.
    // Return NotFound if provided seek point is past the end.
    // Return NotSupported for column without ordinal index.
    Status seek_to_ordinal(ordinal_t idx);

    // Seek the index to the given key, or to the index entry immediately
    // before it. Then seek the data block to the value matching value or to
    // the value immediately after it.
    //
    // Sets *exact_match to indicate whether the seek found the exact
    // key requested.
    //
    // Return NotFound if the given key is greater than all keys in this column.
    // Return NotSupported for column without value index.
    Status seek_at_or_after(const void* key, bool* exact_match);

    // Get the ordinal index that the iterator is currently pointed to.
    ordinal_t get_current_ordinal() const {
        DCHECK(_seeked);
        return _current_ordinal;
    }

    Status next_batch(size_t* n, Column* column);

private:
    IndexedColumnIterator(const IndexedColumnReader* reader, std::unique_ptr<RandomAccessFile> read_file);

    Status _read_data_page(const PagePointer& pp);

    const IndexedColumnReader* _reader = nullptr;
    std::unique_ptr<RandomAccessFile> _read_file;
    // iterator for ordinal index page
    IndexPageIterator _ordinal_iter;
    // iterator for value index page
    IndexPageIterator _value_iter;

    bool _seeked = false;
    // current in-use index iterator, could be `&_ordinal_iter` or `&_value_iter` or null
    IndexPageIterator* _current_iter = nullptr;
    // seeked data page, containing value at `_current_ordinal`
    std::unique_ptr<ParsedPage> _data_page;
    // next_batch() will read from this position
    ordinal_t _current_ordinal = 0;
    // open file handle
};

// thread-safe reader for IndexedColumn (see comments of `IndexedColumnWriter` to understand what IndexedColumn is)
class IndexedColumnReader {
    friend class IndexedColumnIterator;

public:
    // Does *NOT* take the ownership of |fs|.
    IndexedColumnReader(FileSystem* fs, std::string file_name, IndexedColumnMetaPB meta)
            : _fs(fs), _file_name(std::move(file_name)), _meta(std::move(meta)){};

    Status load(bool use_page_cache, bool kept_in_memory);

    Status new_iterator(std::unique_ptr<IndexedColumnIterator>* iter);

    int64_t num_values() const { return _num_values; }
    const EncodingInfo* encoding_info() const { return _encoding_info; }
    const TypeInfoPtr& type_info() const { return _type_info; }
    bool support_ordinal_seek() const { return _meta.has_ordinal_index_meta(); }
    bool support_value_seek() const { return _meta.has_value_index_meta(); }

    size_t mem_usage() const {
        size_t size = sizeof(IndexedColumnReader) - sizeof(IndexedColumnMetaPB);
        size += _meta.SpaceUsedLong() + _ordinal_index_reader.mem_usage() + _value_index_reader.mem_usage();
        return size;
    }

    bool use_page_cache() const { return _use_page_cache; }
    bool kept_in_memory() const { return _kept_in_memory; }

private:
    Status load_index_page(RandomAccessFile* read_file, const PagePointerPB& pp, PageHandle* handle,
                           IndexPageReader* reader);

    // read a page specified by `pp' from `file' into `handle'
    Status read_page(RandomAccessFile* read_file, const PagePointer& pp, PageHandle* handle, Slice* body,
                     PageFooterPB* footer) const;

    FileSystem* _fs;
    std::string _file_name;
    IndexedColumnMetaPB _meta;

    bool _use_page_cache = true;
    bool _kept_in_memory = false;
    int64_t _num_values = 0;
    // whether this column contains any index page.
    // could be false when the column contains only one data page.
    bool _has_index_page = false;
    // valid only when the column contains only one data page
    PagePointer _sole_data_page;
    IndexPageReader _ordinal_index_reader;
    IndexPageReader _value_index_reader;
    PageHandle _ordinal_index_page_handle;
    PageHandle _value_index_page_handle;

    TypeInfoPtr _type_info = nullptr;
    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr;
    const KeyCoder* _validx_key_coder = nullptr;
};

} // namespace starrocks
