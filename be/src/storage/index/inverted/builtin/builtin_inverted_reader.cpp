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

#include "storage/index/inverted/builtin/builtin_inverted_reader.h"

#include <fmt/format.h>

#include <boost/locale/encoding_utf.hpp>
#include <memory>

#include "storage/index/index_descriptor.h"
#include "types/logical_type.h"

namespace starrocks {

Status BuiltinInvertedReader::new_iterator(const std::shared_ptr<TabletIndex> index_meta,
                                           InvertedIndexIterator** iterator,
                                           const IndexReadOptions& index_opt) {
    BitmapIndexIterator* iter;
    RETURN_IF_ERROR(_bitmap_index->new_iterator(index_opt, &iter));
    _bitmap_itr.reset(iter);

    *iterator = new InvertedIndexIterator(index_meta, this);
    return Status::OK();
}

Status BuiltinInvertedReader::create(const std::shared_ptr<TabletIndex>& tablet_index,
                                     LogicalType field_type, std::unique_ptr<InvertedReader>* res) {
    if (is_string_type(field_type)) {
        *res = std::make_unique<BuiltinInvertedReader>(tablet_index->index_id());
        return Status::OK();
    } else {
        return Status::InvalidArgument(fmt::format("Not supported type {}", field_type));
    }
}

Status BuiltinInvertedReader::load(const IndexReadOptions& opt, void* meta) {
    if (meta == nullptr) {
        return Status::InvalidArgument("Invalid argument for loading builtin inverted index");
    }
    const BitmapIndexPB bitmap_index_meta = reinterpret_cast<BuiltinInvertedIndexPB*>(meta)->bitmap_index();
    _bitmap_index = std::make_unique<BitmapIndexReader>();

    ASSIGN_OR_RETURN(auto first_load, _bitmap_index->load(opt, bitmap_index_meta));
    if (!first_load) {
        return Status::InternalError("loading builtin inverted index more than once");
    }
    return Status::OK();
}

Status BuiltinInvertedReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                                    const void* query_value, InvertedIndexQueryType query_type,
                                    roaring::Roaring* bit_map) {
    if (query_type != InvertedIndexQueryType::EQUAL_QUERY) {
        return Status::InternalError("Only support EQUAL_QUERY");
    }
    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    auto act_len = strnlen(search_query->data, search_query->size);
    std::string search_str(search_query->data, act_len);
    std::wstring search_wstr = boost::locale::conv::utf_to_utf<TCHAR>(search_str);
    constexpr auto mul = sizeof(TCHAR) / sizeof(char);
    Slice s((char*) search_wstr.data(), search_wstr.size() * mul);

    bool exact_match = false;
    Status st = _bitmap_itr->seek_dictionary(&s, &exact_match);
    if (st.ok() && exact_match) {
        rowid_t ordinal = _bitmap_itr->current_ordinal();
        RETURN_IF_ERROR(_bitmap_itr->read_bitmap(ordinal, bit_map));
    } else if (!st.is_not_found()) {
        return st;
    }
    return Status::OK();
}

Status BuiltinInvertedReader::query_null(OlapReaderStatistics* stats, const std::string& column_name,
                                         roaring::Roaring* bit_map) {
    return Status::InternalError("Unsupported");
}

} // namespace starrocks