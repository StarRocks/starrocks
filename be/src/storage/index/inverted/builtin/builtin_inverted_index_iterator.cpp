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
#include "util/runtime_profile.h"

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
    if (_stats == nullptr) {
        return Status::InternalError("stats is null for builtin inverted index");
    }

    const std::string& query = search_query->to_string();
    size_t wildcard_pos = query.find('%');
    if (wildcard_pos == std::string::npos) {
        return Status::InternalError("invalid wildcard query for builtin inverted index");
    }

    if (wildcard_pos == search_query->size - 1) {
        SCOPED_RAW_TIMER(&_stats->gin_prefix_filter_ns);
        // optimize for pure prefix query
        Slice prefix_s(search_query->data, wildcard_pos);
        std::string next_prefix = get_next_prefix(prefix_s);
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

        std::pair<rowid_t, std::string> lower = std::make_pair(0, Slice::min_value().to_string());
        ASSIGN_OR_RETURN(lower, seek(prefix_s));

        std::pair<rowid_t, std::string> upper =
                std::make_pair(_bitmap_itr->bitmap_nums(), Slice::max_value().to_string());
        ASSIGN_OR_RETURN(upper, seek(next_prefix_s));

        if (lower.first >= upper.first) {
            // prefix not found
            return Status::OK();
        }

        Buffer<rowid_t> hit_rowids(upper.first - lower.first);
        std::iota(hit_rowids.begin(), hit_rowids.end(), lower.first);
        RETURN_IF_ERROR(_bitmap_itr->read_union_bitmap(hit_rowids, bit_map));
        return Status::OK();
    }

    bool found_at_least_one = false;
    roaring::Roaring filtered_key_words;
    std::vector<std::string> keywords;

    SCOPED_RAW_TIMER(&_stats->gin_ngram_filter_dict_ns);
    // parse wildcard like '%%key%word%%' into ['key', 'word']
    // use ngram to filter each word
    wildcard_pos = 0;
    size_t last_wildcard_pos = 0;
    while (wildcard_pos < query.size()) {
        while (wildcard_pos < query.size() && query[wildcard_pos] == '%') {
            ++wildcard_pos;
        }
        if (wildcard_pos >= query.size()) {
            break;
        }
        last_wildcard_pos = wildcard_pos;
        wildcard_pos = query.find('%', last_wildcard_pos);

        auto sub_query = wildcard_pos != std::string::npos
                                 ? query.substr(last_wildcard_pos, wildcard_pos - last_wildcard_pos)
                                 : query.substr(last_wildcard_pos);
        keywords.emplace_back(sub_query);

        const Slice sub_query_s(sub_query);
        roaring::Roaring tmp;

        if (const auto st = _bitmap_itr->seek_dict_by_ngram(&sub_query_s, &tmp); !st.ok()) {
            if (st.is_not_found()) {
                // no words match ngram index, just return empty bitmap
                VLOG(10) << "no words match ngram index for gram query " << sub_query;
                return Status::OK();
            }
            return st;
        }

        if (tmp.cardinality() <= 0) {
            // no words match ngram index, just return empty bitmap
            VLOG(10) << "no words match ngram index for gram query " << sub_query;
            return Status::OK();
        }

        if (!found_at_least_one) {
            filtered_key_words = std::move(tmp);
            found_at_least_one = true;
        } else {
            filtered_key_words &= tmp;
        }

        if (filtered_key_words.cardinality() <= 0) {
            // no words match ngram index, just return empty bitmap
            VLOG(10) << "no words match ngram index for gram query " << sub_query;
            return Status::OK();
        }
    }

    if (!found_at_least_one) {
        VLOG(10) << "no words match ngram index for " << query;
        // no words match ngram index, just return empty bitmap
        return Status::OK();
    }

    auto predicate = [&keywords](const Slice* dict) -> bool {
        // just need to make sure keywords is in order.
        const std::string dictionary_str = dict->to_string();
        size_t last_pos = 0;
        for (const auto& keyword : keywords) {
            last_pos = dictionary_str.find(keyword, last_pos);
            if (last_pos == std::string::npos) {
                return false;
            }
            last_pos += keyword.size();
        }
        return true;
    };

    SCOPED_RAW_TIMER(&_stats->gin_filter_dict_ns);
    ASSIGN_OR_RETURN(const auto hit_rowids, _bitmap_itr->filter_dict_by_predicate(&filtered_key_words, predicate));
    return _bitmap_itr->read_union_bitmap(hit_rowids, bit_map);
}

Status BuiltinInvertedIndexIterator::read_from_inverted_index(const std::string& column_name, const void* query_value,
                                                              InvertedIndexQueryType query_type,
                                                              roaring::Roaring* bit_map) {
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
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_QUERY: {
        std::string search_query_str = search_query->to_string();
        std::istringstream iss(search_query_str);
        std::string cur_predicate;
        bool first = true;
        while (iss >> cur_predicate) {
            Slice s(cur_predicate);
            roaring::Roaring roaring;
            if (cur_predicate.find('%') != std::string::npos) {
                RETURN_IF_ERROR(_wildcard_query(&s, &roaring));
            } else {
                RETURN_IF_ERROR(_equal_query(&s, &roaring));
            }

            if (first) {
                *bit_map = std::move(roaring);
                first = false;
            } else if (query_type == InvertedIndexQueryType::MATCH_ALL_QUERY) {
                *bit_map &= roaring;
            } else if (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY) {
                *bit_map |= roaring;
            } else {
                DCHECK(false) << "do not support query type";
            }
            cur_predicate.clear();
        }
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
