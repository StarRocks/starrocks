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

#include "storage/index/inverted/builtin/builtin_inverted_index_iterator.h"

#include <CLucene.h>

#include <memory>

#include "exprs/function_context.h"
#include "exprs/like_predicate.h"
#include "storage/chunk_helper.h"

namespace starrocks {
static std::string get_next_prefix(const Slice& prefix_s) {
    std::string next_prefix = prefix_s.to_string();

    int n = next_prefix.length();
    int i = n - 1;
    while (i >= 0) {
        unsigned char byte_val = static_cast<unsigned char>(next_prefix[i]);
        if (byte_val == 0xFF) {
            next_prefix[i] = static_cast<char>(0x00);
            i--;
        } else {
            next_prefix[i] = static_cast<char>(byte_val + 1);
            break;
        }
    }
    if (i < 0) {
        next_prefix = std::string(1, static_cast<char>(0x01)) + next_prefix;
    }
    return next_prefix;
}

Status BuiltinInvertedIndexIterator::close() {
    if (!_like_context) {
        return Status::OK();
    }
    return LikePredicate::like_close(_like_context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL);
}

Status BuiltinInvertedIndexIterator::_equal_query(const Slice* search_query, roaring::Roaring* bit_map) {
    bool exact_match = false;
    Status st = _bitmap_itr->seek_dictionary(search_query, &exact_match);
    if (st.ok() && exact_match) {
        rowid_t ordinal = _bitmap_itr->current_ordinal();
        RETURN_IF_ERROR(_bitmap_itr->read_bitmap(ordinal, bit_map));
    } else if (!st.ok() && !st.is_not_found()) {
        return st;
    }
    return Status::OK();
}

Status BuiltinInvertedIndexIterator::_wildcard_query(const Slice* search_query, roaring::Roaring* bit_map) {
    auto first_wildcard_pos = search_query->to_string().find('%');
    if (first_wildcard_pos == std::string::npos) {
        return Status::InternalError("invalid wildcard query for builtin inverted index");
    }

    std::pair<rowid_t, std::string> lower = std::make_pair(0, Slice::min_value().to_string());
    std::pair<rowid_t, std::string> upper = std::make_pair(_bitmap_itr->bitmap_nums(), Slice::max_value().to_string());

    if (first_wildcard_pos != 0) {
        Slice prefix_s(search_query->data, first_wildcard_pos);
        std::string next_prefix = std::move(get_next_prefix(prefix_s));
        Slice next_prefix_s(next_prefix);

        auto seek = [&](const Slice& bound) -> StatusOr<std::pair<rowid_t, std::string>> {
            bool exact_match;
            auto st = _bitmap_itr->seek_dictionary(&bound, &exact_match);
            auto cur_ordinal = _bitmap_itr->current_ordinal();
            if (st.is_not_found()) {
                // hit the end of dictionary set
                return std::make_pair(_bitmap_itr->bitmap_nums(), Slice::max_value().to_string());
            } else if (!st.ok()) {
                return st;
            } else {
                auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
                size_t read_num = 1;
                RETURN_IF_ERROR(_bitmap_itr->next_batch_dictionary(&read_num, column.get()));
                Slice s = down_cast<BinaryColumn*>(column.get())->get_data()[0];
                return std::make_pair(cur_ordinal, s.to_string());
            }
        };

        ASSIGN_OR_RETURN(lower, seek(prefix_s));
        ASSIGN_OR_RETURN(upper, seek(next_prefix_s));

        if (lower.first >= upper.first) {
            // prefix not found
            return Status::OK();
        }

        if (first_wildcard_pos == search_query->size - 1) {
            // pure prefix query
            Buffer<rowid_t> hit_rowids(upper.first - lower.first);
            std::iota(hit_rowids.begin(), hit_rowids.end(), lower.first);
            RETURN_IF_ERROR(_bitmap_itr->read_union_bitmap(hit_rowids, bit_map));
            return Status::OK();
        }
    }

    RETURN_IF_ERROR(_init_like_context(*search_query));
    auto predicate = [this](const Column& value_column) -> StatusOr<ColumnPtr> {
        Columns cols;
        cols.push_back(&value_column);
        cols.push_back(this->_like_context->get_constant_column(1));
        return LikePredicate::like(this->_like_context.get(), cols);
    };
    Slice from_value = Slice(lower.second);
    size_t search_size = upper.first - lower.first;
    auto res = _bitmap_itr->seek_dictionary_by_predicate(predicate, from_value, search_size);
    Status st = res.status();
    if (st.ok()) {
        auto hit_rowids = std::move(res.value());
        RETURN_IF_ERROR(_bitmap_itr->read_union_bitmap(hit_rowids, bit_map));
    } else if (!st.ok() && !st.is_not_found()) {
        return st;
    }
    return Status::OK();
}

Status BuiltinInvertedIndexIterator::_init_like_context(const Slice& s) {
    if (_like_context) {
        RETURN_IF_ERROR(LikePredicate::like_close(_like_context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL));
        _like_context.reset(nullptr);
    }

    _like_context = std::make_unique<FunctionContext>();
    auto ptr = BinaryColumn::create();
    ptr->append_datum(Datum(s));
    ptr->get_data();
    Columns cols;
    cols.push_back(nullptr);
    cols.push_back(std::move(ConstColumn::create(std::move(ptr), 1)));
    _like_context->set_constant_columns(cols);
    return LikePredicate::like_prepare(_like_context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL);
}

Status BuiltinInvertedIndexIterator::read_from_inverted_index(const std::string& column_name, const void* query_value,
                                                              InvertedIndexQueryType query_type, roaring::Roaring* bit_map) {
    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        RETURN_IF_ERROR(_equal_query(search_query, bit_map));
        break;
    }
    case InvertedIndexQueryType::MATCH_WILDCARD_QUERY: {
        RETURN_IF_ERROR(_wildcard_query(search_query, bit_map));
        break;
    }
    default:
        return Status::InvalidArgument("do not support query type");
    }
    return Status::OK();
}

Status BuiltinInvertedIndexIterator::read_null(const std::string& column_name, roaring::Roaring* bit_map) {
    return Status::InternalError("Unsupported");
}

} // namespace starrocks
