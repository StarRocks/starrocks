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

    int64_t wildcard_pos = search_query->find('%');
    if (wildcard_pos == -1) {
        return Status::InternalError("invalid wildcard query for builtin inverted index");
    }

    if (wildcard_pos == search_query->get_size() - 1) {
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

    roaring::Roaring filtered_key_words;
    filtered_key_words.addRange(0, _bitmap_itr->bitmap_nums());

    std::vector<std::pair<Slice, std::vector<size_t>>> keywords;

    _stats->gin_dict_count = _bitmap_itr->bitmap_nums();
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
        _stats->gin_ngram_dict_filtered = _bitmap_itr->bitmap_nums() - filtered_key_words.cardinality();
    }

    Buffer<rowid_t> hit_rowids;
    {
        SCOPED_RAW_TIMER(&_stats->gin_predicate_filter_dict_ns);
        auto predicate = [&keywords](const Slice* dict) -> bool {
            // just need to make sure keywords is in order.
            size_t last_pos = 0;
            for (const auto& [keyword, next_array] : keywords) {
                last_pos = dict->find(keyword, next_array, last_pos);
                if (last_pos == -1) {
                    return false;
                }
                last_pos += keyword.get_size();
            }
            return true;
        };
        ASSIGN_OR_RETURN(hit_rowids, _bitmap_itr->filter_dict_by_predicate(&filtered_key_words, predicate));
        _stats->gin_predicate_dict_filtered = filtered_key_words.cardinality() - hit_rowids.size();
    }
    return _bitmap_itr->read_union_bitmap(hit_rowids, bit_map);
}

Status BuiltinInvertedIndexIterator::_phrase_query(const Slice* search_query, roaring::Roaring* bit_map) const {
    std::istringstream iss(search_query->to_string());

    // row_id -> dict_id -> positions
    phmap::flat_hash_map<rowid_t, phmap::flat_hash_map<rowid_t, roaring::Roaring>> positions;

    roaring::Roaring filtered_rows;
    std::vector<rowid_t> dict_ids;
    std::vector<roaring::Roaring> full_doc_ids;
    filtered_rows.addRange(0, _bitmap_itr->bitmap_nums());

    std::string cur_predicate;
    while (iss >> cur_predicate) {
        LOG(INFO) << "match_phrase: filter for " << cur_predicate;
        Slice s(cur_predicate);

        bool exact_match = true;
        Status st = _bitmap_itr->seek_dictionary(&s, &exact_match);

        if (st.ok() && exact_match) {
            rowid_t ordinal = _bitmap_itr->current_ordinal();
            LOG(INFO) << "match_phrase: found at ordinal: " << ordinal;

            roaring::Roaring doc_ids;
            RETURN_IF_ERROR(_bitmap_itr->read_bitmap(ordinal, &doc_ids));
            if (doc_ids.cardinality() <= 0) {
                bit_map->clear();
                return Status::OK();
            }

            filtered_rows &= doc_ids;
            if (filtered_rows.cardinality() <= 0) {
                bit_map->clear();
                return Status::OK();
            }

            full_doc_ids.emplace_back(doc_ids);
            dict_ids.emplace_back(ordinal);
        } else if (st.is_not_found()) {
            bit_map->clear();
            return Status::OK();
        } else {
            return st;
        }
    }

    std::vector<uint32_t> candidate_row_ids(filtered_rows.cardinality(), 0);
    std::vector<uint64_t> ranks(filtered_rows.cardinality(), 0);
    filtered_rows.toUint32Array(candidate_row_ids.data());

    for (uint32_t i = 0; i < dict_ids.size(); ++i) {
        rowid_t dict_id = dict_ids[i];
        full_doc_ids[i].rank_many(candidate_row_ids.data(), candidate_row_ids.data() + candidate_row_ids.size(),
                                  ranks.data());
        ASSIGN_OR_RETURN(auto ranked_positions, _bitmap_itr->read_positions(dict_id, ranks));
        for (uint32_t j = 0; j < candidate_row_ids.size(); ++j) {
            rowid_t row_id = candidate_row_ids[j];
            positions[row_id][dict_id] = ranked_positions[j];
        }
    }

    for (const rowid_t& row : candidate_row_ids) {
        LOG(INFO) << "match_phrase: final processing row: " << row;
        for (auto dict_to_position_list = positions.at(row); const rowid_t start : dict_to_position_list[dict_ids[0]]) {
            LOG(INFO) << "match_phrase: start position: " << start;
            bool found = true;
            for (size_t offset = 1; offset < dict_ids.size(); ++offset) {
                if (const auto& position_list = dict_to_position_list.at(dict_ids[offset]);
                    !position_list.contains(start + offset)) {
                    found = false;
                    break;
                }
            }
            if (found) {
                LOG(INFO) << "match_phrase: found row: " << row;
                bit_map->add(row);
                break;
            }
        }
    }
    return Status::OK();
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
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
        RETURN_IF_ERROR(_phrase_query(search_query, bit_map));
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
