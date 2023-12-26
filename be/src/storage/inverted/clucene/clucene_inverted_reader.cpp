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

#include "storage/inverted/clucene/clucene_inverted_reader.h"

#include <fmt/format.h>

#include <memory>

#include "match_operator.h"
#include "storage/inverted/index_descriptor.hpp"
#include "types/logical_type.h"
#include "util/faststring.h"

namespace starrocks {

Status CLuceneInvertedReader::new_iterator(const std::shared_ptr<TabletIndex> index_meta,
                                           InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, this);
    return Status::OK();
}

Status CLuceneInvertedReader::create(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index,
                                     LogicalType field_type, std::unique_ptr<InvertedReader>* res) {
    if (is_string_type(field_type)) {
        InvertedIndexParserType parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(tablet_index->common_properties()));
        // Only support full text search for now
        *res = std::make_unique<FullTextCLuceneInvertedReader>(path, tablet_index->index_id(), parser_type);
        return Status::OK();
    } else {
        return Status::InvalidArgument(fmt::format("Not supported type {}", field_type));
    }
}

Status FullTextCLuceneInvertedReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                                            const void* query_value, InvertedIndexQueryType query_type,
                                            roaring::Roaring* bit_map) {
    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    auto act_len = strnlen(search_query->data, search_query->size);
    std::string search_str(search_query->data, act_len);
    DLOG(INFO) << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << search_str;
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());

    if (!indexExists(_index_path)) {
        LOG(WARNING) << "inverted index path: " << _index_path << " not exist.";
        return Status::InvertedIndexFileNotFound(fmt::format("Not exists index_file {}", _index_path.c_str()));
    }

    std::unique_ptr<MatchOperator> match_operator;

    auto* directory = lucene::store::FSDirectory::getDirectory(_index_path.c_str());
    lucene::search::IndexSearcher index_searcher(directory);

    switch (query_type) {
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::EQUAL_QUERY:
        match_operator =
                std::make_unique<MatchTermOperator>(&index_searcher, nullptr, column_name_ws.c_str(), search_str);
        break;
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        // in phrase query
        match_operator = std::make_unique<MatchPhraseOperator>(&index_searcher, nullptr, column_name_ws.c_str(),
                                                               search_str, 0, _parser_type);
        break;
    case InvertedIndexQueryType::LESS_THAN_QUERY:
        match_operator = std::make_unique<MatchLessThanOperator>(&index_searcher, nullptr, column_name_ws.c_str(),
                                                                 search_str, false);
        break;
    case InvertedIndexQueryType::LESS_EQUAL_QUERY:
        match_operator = std::make_unique<MatchLessThanOperator>(&index_searcher, nullptr, column_name_ws.c_str(),
                                                                 search_str, true);
        break;
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
        match_operator = std::make_unique<MatchGreatThanOperator>(&index_searcher, nullptr, column_name_ws.c_str(),
                                                                  search_str, false);
        break;
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY:
        match_operator = std::make_unique<MatchGreatThanOperator>(&index_searcher, nullptr, column_name_ws.c_str(),
                                                                  search_str, true);
        break;
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        match_operator =
                std::make_unique<MatchWildcardOperator>(&index_searcher, nullptr, column_name_ws.c_str(), search_str);
        break;
    default:
        return Status::InvertedIndexInvalidParams("Unknown query type");
    }

    _CLDECDELETE(directory)

    roaring::Roaring result;
    try {
        RETURN_IF_ERROR(match_operator->match(&result));
    } catch (CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::InternalError(fmt::format("CLuceneError occured, error msg: {}", e.what()));
    }
    bit_map->swap(result);
    return Status::OK();
}

Status FullTextCLuceneInvertedReader::query_null(OlapReaderStatistics* stats, const std::string& column_name,
                                                 roaring::Roaring* bit_map) {
    lucene::store::IndexInput* null_bitmap_in = nullptr;
    lucene::store::FSDirectory* dir = nullptr;
    try {
        // try to get query bitmap result from cache and return immediately on cache hit
        dir = lucene::store::FSDirectory::getDirectory(_index_path.c_str());

        // ownership of null_bitmap and its deletion will be transfered to cache
        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        auto null_bitmap_file_name = IndexDescriptor::get_temporary_null_bitmap_file_name();
        if (dir->fileExists(null_bitmap_file_name.c_str())) {
            CLuceneError err;
            if (!dir->openInput(null_bitmap_file_name.c_str(), null_bitmap_in, err)) {
                throw err;
            }

            size_t null_bitmap_size = null_bitmap_in->length();
            faststring buf;
            buf.resize(null_bitmap_size);
            null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(buf.data()), null_bitmap_size);
            *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap->runOptimize();
            CLOSE_INPUT(null_bitmap_in);
        }

        bit_map->swap(*null_bitmap);

        CLOSE_INPUT(dir)
    } catch (CLuceneError& e) {
        if (null_bitmap_in) {
            FINALLY_CLOSE_INPUT(null_bitmap_in)
        }
        if (dir) {
            FINALLY_CLOSE_INPUT(dir)
        }
        LOG(WARNING) << "Inverted index read null bitmap error occurred: " << e.what();
        return Status::InvertedIndexFileNotFound(
                fmt::format("Inverted index read null bitmap error occurred: ", e.what()));
    }

    return Status::OK();
}

} // namespace starrocks