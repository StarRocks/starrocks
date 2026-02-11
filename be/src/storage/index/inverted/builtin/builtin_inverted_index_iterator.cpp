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

#include "common/runtime_profile.h"
#include "exprs/function_context.h"
#include "exprs/like_predicate.h"
#include "storage/chunk_helper.h"

namespace starrocks {
std::string get_next_prefix(const Slice& prefix_s) {
    std::string next_prefix = prefix_s.to_string();

    for (int i = next_prefix.length() - 1; i >= 0; i--) {
        if (static_cast<unsigned char>(next_prefix[i]) != 0xFF) {
            next_prefix[i]++;
            next_prefix.resize(i + 1);
            return next_prefix;
        }
    }
    return "";
}

Status BuiltinInvertedIndexIterator::_equal_query(const Slice* search_query, roaring::Roaring* bitmap) {
    bool exact_match = false;
    Status st = _bitmap_itr->seek_dictionary(search_query, &exact_match);
    if (st.ok() && exact_match) {
        rowid_t ordinal = _bitmap_itr->current_ordinal();
        RETURN_IF_ERROR(_bitmap_itr->read_bitmap(ordinal, bitmap));
    } else if (!st.ok() && !st.is_not_found()) {
        return st;
    }
    return Status::OK();
}

Status BuiltinInvertedIndexIterator::_wildcard_query(const Slice* search_query, roaring::Roaring* bitmap) {
    if (_stats == nullptr) {
        return Status::InternalError("stats is null for builtin inverted index");
    }

    int64_t wildcard_pos = search_query->find('%');
    if (wildcard_pos == -1) {
        return Status::InternalError("invalid wildcard query for builtin inverted index");
    }

    if (wildcard_pos == search_query->get_size() - 1 && search_query->get_size() == 1) {
        // It means all the rows are matched.
        bitmap->addRange(0, _segment_rows);
        roaring::Roaring null_map;
        RETURN_IF_ERROR(_bitmap_itr->read_null_bitmap(&null_map));
        *bitmap -= null_map;
        return Status::OK();
    }

    if (wildcard_pos == search_query->get_size() - 1) {
        SCOPED_RAW_TIMER(&_stats->gin_prefix_filter_ns);
        // optimize for pure prefix query
        Slice prefix_s(search_query->data, wildcard_pos);
        std::string next_prefix = get_next_prefix(prefix_s);

        rowid_t lower_rowid = 0;
        rowid_t upper_rowid =
                _bitmap_itr->has_null_bitmap() ? _bitmap_itr->bitmap_nums() - 1 : _bitmap_itr->bitmap_nums();
        std::pair<rowid_t, std::string> lower = std::make_pair(lower_rowid, Slice::min_value().to_string());
        std::pair<rowid_t, std::string> upper = std::make_pair(upper_rowid, Slice::max_value().to_string());

        auto seek = [&](const Slice& bound) -> StatusOr<std::pair<rowid_t, std::string>> {
            bool exact_match;
            auto st = _bitmap_itr->seek_dictionary(&bound, &exact_match);
            auto cur_ordinal = _bitmap_itr->current_ordinal();
            if (st.is_not_found()) {
                // hit the end of dictionary set
                return std::make_pair(upper_rowid, Slice::max_value().to_string());
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
        if (!next_prefix.empty()) {
            Slice next_prefix_s(next_prefix);
            ASSIGN_OR_RETURN(upper, seek(next_prefix_s));
        }

        if (lower.first >= upper.first) {
            // prefix not found
            return Status::OK();
        }

        Buffer<rowid_t> hit_rowids(upper.first - lower.first);
        std::iota(hit_rowids.begin(), hit_rowids.end(), lower.first);
        RETURN_IF_ERROR(_bitmap_itr->read_union_bitmap(hit_rowids, bitmap));
        return Status::OK();
    }

    roaring::Roaring filtered_key_words;
    filtered_key_words.addRange(0, _bitmap_itr->num_dictionaries());

    std::vector<std::pair<Slice, std::vector<size_t>>> keywords;

    _stats->gin_dict_count = _bitmap_itr->num_dictionaries();
    _stats->gin_ngram_dict_count = _bitmap_itr->ngram_bitmap_nums();

    {
        SCOPED_RAW_TIMER(&_stats->gin_ngram_filter_dict_ns);
        // Parse wildcard query like '%%key%word%%' into ['key', 'word'], then use ngram to filter each word.
        // Start from the beginning to search: the left boundary may consist of several '%' characters. Therefore,
        // if the left boundary is '%', move the left boundary to the right. After finding the first non-'%' character,
        // locate the first '%' that appears after this position, and use it as the right boundary. The segment between
        // these two positions is the keyword. Then, set the right boundary as the new left boundary and continue
        // searching for the next right boundary following the same logic.
        int64_t left = 0, right = 0;
        while (0 <= left && left < search_query->get_size()) {
            while (0 <= left && left < search_query->get_size() && (*search_query)[left] == '%') {
                ++left;
            }
            if (left < 0 || left >= search_query->get_size()) {
                break;
            }
            right = left;
            left = search_query->find('%', right);

            auto sub_query = left != -1 ? Slice(search_query->data + right, left - right)
                                        : Slice(search_query->data + right, search_query->get_size() - right);
            keywords.emplace_back(sub_query, sub_query.build_next());

            if (_bitmap_itr->ngram_bitmap_nums() == 0) {
                continue;
            }

            roaring::Roaring tmp;
            if (auto st = _bitmap_itr->seek_dict_by_ngram(&sub_query, &tmp); !st.ok()) {
                if (st.is_not_found()) {
                    // no words match ngram index, just return empty bitmap
                    VLOG(10) << "no words match ngram index for gram query " << sub_query.to_string();
                    return Status::OK();
                }
                return st;
            }

            if (tmp.cardinality() <= 0) {
                // no words match ngram index, just return empty bitmap
                VLOG(10) << "no words match ngram index for gram query " << sub_query.to_string();
                return Status::OK();
            }

            filtered_key_words &= tmp;
            if (filtered_key_words.cardinality() <= 0) {
                // no words match ngram index, just return empty bitmap
                VLOG(10) << "no words match ngram index for query " << search_query->to_string();
                return Status::OK();
            }
        }
        _stats->gin_ngram_dict_filtered = _bitmap_itr->num_dictionaries() - filtered_key_words.cardinality();
    }

    Buffer<rowid_t> hit_rowids;
    {
        SCOPED_RAW_TIMER(&_stats->gin_predicate_filter_dict_ns);
        // Check if pattern starts/ends with '%' to enforce SQL LIKE semantics
        bool pattern_starts_with_wildcard = (*search_query)[0] == '%';
        bool pattern_ends_with_wildcard = (*search_query)[search_query->get_size() - 1] == '%';

        auto predicate = [&keywords, pattern_starts_with_wildcard,
                          pattern_ends_with_wildcard](const Slice* dict) -> bool {
            if (keywords.empty()) {
                return true;
            }

            // Find each keyword in order, checking start/end constraints
            int64_t last_pos = 0;
            for (size_t i = 0; i < keywords.size(); ++i) {
                const auto& [keyword, next_array] = keywords[i];

                if (i == keywords.size() - 1 && !pattern_ends_with_wildcard) {
                    // The last keyword must align to the end. Use the last possible position instead of
                    // the first occurrence to avoid rejecting cases like "a%c" on "acc".
                    if (!dict->ends_with(keyword)) {
                        return false;
                    }
                    int64_t required_pos = dict->get_size() - keyword.get_size();
                    if (required_pos < last_pos) {
                        return false;
                    }
                    last_pos = required_pos + keyword.get_size();
                    continue;
                }

                last_pos = dict->find(keyword, next_array, last_pos);
                if (last_pos == -1) {
                    return false;
                }

                // First keyword must be at start if pattern doesn't start with '%'
                if (i == 0 && !pattern_starts_with_wildcard && last_pos != 0) {
                    return false;
                }

                last_pos += keyword.get_size();
            }
            return true;
        };
        ASSIGN_OR_RETURN(hit_rowids, _bitmap_itr->filter_dict_by_predicate(&filtered_key_words, predicate));
        _stats->gin_predicate_dict_filtered = filtered_key_words.cardinality() - hit_rowids.size();
    }
    return _bitmap_itr->read_union_bitmap(hit_rowids, bitmap);
}

Status BuiltinInvertedIndexIterator::read_from_inverted_index(const std::string& column_name, const void* query_value,
                                                              InvertedIndexQueryType query_type,
                                                              roaring::Roaring* bitmap) {
    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        RETURN_IF_ERROR(_equal_query(search_query, bitmap));
        break;
    }
    case InvertedIndexQueryType::MATCH_WILDCARD_QUERY: {
        RETURN_IF_ERROR(_wildcard_query(search_query, bitmap));
        break;
    }
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_QUERY: {
        std::string search_query_str = search_query->to_string();
        std::istringstream iss(search_query_str);
        std::string cur_predicate;
        bool first = true;
        roaring::Roaring roaring;
        while (iss >> cur_predicate) {
            roaring.clear();
            Slice s(cur_predicate);
            if (cur_predicate.find('%') != std::string::npos) {
                RETURN_IF_ERROR(_wildcard_query(&s, &roaring));
            } else {
                RETURN_IF_ERROR(_equal_query(&s, &roaring));
            }

            if (first) {
                *bitmap = std::move(roaring);
                first = false;
            } else if (query_type == InvertedIndexQueryType::MATCH_ALL_QUERY) {
                *bitmap &= roaring;
            } else if (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY) {
                *bitmap |= roaring;
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

Status BuiltinInvertedIndexIterator::read_null(const std::string& column_name, roaring::Roaring* bitmap) {
    return Status::InternalError("Unsupported");
}

} // namespace starrocks
