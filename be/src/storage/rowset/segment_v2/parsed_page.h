// This file is made available under Elastic License 2.0.
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

#include "storage/rowset/segment_v2/common.h" // ordinal_t
#include "storage/rowset/segment_v2/page_decoder.h"
#include "storage/rowset/segment_v2/page_pointer.h"

namespace starrocks {
class ColumnBlockView;
class Slice;
class Status;

namespace vectorized {
class Column;
}

namespace segment_v2 {

class DataPageFooterPB;
class EncodingInfo;
class PageHandle;
class PagePointer;

class ParsedPage {
public:
    ParsedPage() : _page_index(0), _num_rows(0), _first_ordinal(0), _offset_in_page(0) {}

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
    virtual Status read(vectorized::Column* column, size_t* count) = 0;

    // Attempts to read up to |*count| records from this page into the |block|.
    // On success, `Status::OK` is returned, and the number of records read will be updated to
    // |count|, the page offset is advanced by this number too. This number is the minimum value
    // of |*count| and |remaining()|.
    // On error, the value of |*count| is undefined.
    virtual Status read(ColumnBlockView* block, size_t* count) = 0;

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
    virtual Status read_dict_codes(vectorized::Column* column, size_t* count) = 0;

protected:
    uint32_t _page_index;
    uint64_t _num_rows;
    ordinal_t _first_ordinal;
    ordinal_t _offset_in_page;

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

} // namespace segment_v2
} // namespace starrocks
