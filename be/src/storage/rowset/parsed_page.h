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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/parsed_page.h

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

#include "storage/range.h"
#include "storage/rowset/common.h" // ordinal_t
#include "storage/rowset/page_decoder.h"
#include "storage/rowset/page_pointer.h"

namespace starrocks {
class Slice;
class Status;
class Column;
class DataPageFooterPB;
class EncodingInfo;
class PageHandle;
class PagePointer;

class ParsedPage {
public:
    ParsedPage() = default;

    virtual ~ParsedPage() = default;

    uint32_t page_index() const { return _page_index; }

    const PagePointer& page_pointer() const { return _page_pointer; }

    ordinal_t first_ordinal() const { return _first_ordinal; }

    uint64_t num_rows() const { return _num_rows; }

    ordinal_t offset() const { return _offset_in_page; }

    ordinal_t corresponding_element_ordinal() const { return _corresponding_element_ordinal; }

    bool contains(ordinal_t ord) const {
        static_assert(std::is_unsigned_v<ordinal_t>);
        // Branch-avoiding version of following check:
        //  return ord >= first_ordinal && ord < num_rows
        return ord - _first_ordinal < _num_rows;
    }

    PageDecoder* data_decoder() { return _data_decoder.get(); }

    // Number of records after current offset in this page.
    size_t remaining() const { return _num_rows - _offset_in_page; }

    // Return the encoding type of this page.
    EncodingTypePB encoding_type() const { return _data_decoder->encoding_type(); }

    // Set the page offset indicator to the specified position |offset|.
    // The |offset| is relative to first_ordinal(), and it should less than num_rows().
    virtual Status seek(ordinal_t offset) = 0;

    // Attempts to read up to |*count| records from this page into the |column|.
    // On success, `Status::OK` is returned, and the number of records read will be updated to
    // |count|, the page offset is advanced by this number too. This number is the minimum value
    // of |*count| and |remaining()|.
    // On error, the value of |*count| is undefined.
    virtual Status read(Column* column, size_t* count) = 0;

    virtual Status read(Column* column, const SparseRange& range) {
        return Status::NotSupported("Read by range Not Support");
    }

    // prerequisite: encoding_type() is `DICT_ENCODING`.
    // Attempts to read up to |*count| dictionary codes from this page into the |column|.
    // On success, `Status::OK` is returned, and the number of codes read will be updated to
    // |count|, the page offset is advanced by this number too. This number is the minimum value
    // of |*count| and |remaining()|.
    // For NULL records, the dictionary codes depended on the page format. If the page format is v1,
    // the dictionary codes of NULL records are all -1; If the page format is v2, the dictionary
    // codes are arbitrary, but it's guaranteed that those codes are within the valid range of
    // dictionary page, i.e, using these codes to lookup the dictionary page is safe.
    // On error, the value of |*count| is undefined.
    virtual Status read_dict_codes(Column* column, size_t* count) = 0;

    virtual Status read_dict_codes(Column* column, const SparseRange& range) = 0;

protected:
    uint32_t _page_index{0};
    uint64_t _num_rows{0};
    ordinal_t _first_ordinal{0};
    ordinal_t _offset_in_page{0};

    // ArrayColumn is made up of (offset, element)
    // On disk, it will change to (array_size, element)
    // Every page will record the corresponding element ordinal.
    ordinal_t _corresponding_element_ordinal = 0;
    std::unique_ptr<PageDecoder> _data_decoder;
    PagePointer _page_pointer;
};

Status parse_page(std::unique_ptr<ParsedPage>* result, PageHandle handle, const Slice& body,
                  const DataPageFooterPB& footer, const EncodingInfo* encoding, const PagePointer& page_pointer,
                  uint32_t page_index);

} // namespace starrocks
