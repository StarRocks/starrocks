//
// Created by jijundu_new on 2023/10/4.
//

#include "clucene_inverted_reader.h"

#include <fmt/format.h>

#include <filesystem>
#include <memory>

#include "storage/rowset/index_descriptor.h"

namespace starrocks {
class StringCLuceneInvertedReader;
class BKDIndexReader;

Status CLuceneInvertedReader::new_iterator(const std::shared_ptr<TabletIndex> index_meta,
                                           InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, this);
    return Status::OK();
}

static std::string enum_to_string(InvertedIndexQueryType query_type) {
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
        return "EQUAL_QUERY";
    case InvertedIndexQueryType::LESS_THAN_QUERY:
        return "EQUAL_QUERY";
    case InvertedIndexQueryType::LESS_EQUAL_QUERY:
        return "EQUAL_QUERY";
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
        return "EQUAL_QUERY";
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY:
        return "EQUAL_QUERY";
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        return "EQUAL_QUERY";
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
        return "EQUAL_QUERY";
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        return "EQUAL_QUERY";
    default:
        return "UNKNOWN_QUERY";
    }
}

Status CLuceneInvertedReader::create(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index,
                                     LogicalType field_type, std::unique_ptr<InvertedReader>* res) {
    if (is_string_type(field_type)) {
        InvertedIndexParserType parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(tablet_index->common_properties()));
        // Tokenize situation
        if (parser_type != InvertedIndexParserType::PARSER_NONE) {
            *res = std::make_unique<FullTextCLuceneInvertedReader>(path, tablet_index->index_id(), parser_type);
        } else {
            // None tokenizer
            *res = std::make_unique<StringCLuceneInvertedReader>(path, tablet_index->index_id(), parser_type);
        }
        return Status::OK();
    } else if (is_numeric_type(field_type)) {
        *res = std::make_unique<BKDIndexReader>(path, tablet_index->index_id(), field_type);
        return Status::OK();
    } else {
        return Status::InvalidArgument(fmt::format("Not supported type {}", field_type));
    }
}

Status StringCLuceneInvertedReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                                          const void* query_value, InvertedIndexQueryType query_type,
                                          roaring::Roaring* bit_map) {
    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    auto act_len = strnlen(search_query->data, search_query->size);
    std::string search_str(search_query->data, act_len);
    DLOG(INFO) << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << search_str;
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());
    std::wstring search_str_ws = std::wstring(search_str.begin(), search_str.end());

    // unique_ptr with custom deleter
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term{
            _CLNEW lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str()),
            [](lucene::index::Term* term) { _CLDECDELETE(term) }};
    std::unique_ptr<lucene::search::Query> query;

    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        query = std::make_unique<lucene::search::TermQuery>(term.get());
        break;
    case InvertedIndexQueryType::LESS_THAN_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(nullptr, term.get(), false);
        break;
    case InvertedIndexQueryType::LESS_EQUAL_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(nullptr, term.get(), true);
        break;
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(term.get(), nullptr, false);
        break;
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(term.get(), nullptr, true);
        break;
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        query = std::make_unique<lucene::search::FuzzyQuery>(term.get());
        break;
    default:
        return Status::InternalError(fmt::format("Do not support such query {}", enum_to_string(query_type)));
    }

    if (!indexExists(_index_path)) {
        LOG(WARNING) << "inverted index path: " << _index_path << " not exist.";
        return Status::InternalError(fmt::format("Not exists index_file {}", _index_path.c_str()));
    }

    std::shared_ptr<lucene::search::IndexSearcher> index_searcher;
    {
        auto* directory = lucene::store::FSDirectory::getDirectory(_index_path.c_str());

        index_searcher = std::make_shared<lucene::search::IndexSearcher>(directory, true);
        _CLDECDELETE(directory)
    }

    roaring::Roaring result;
    try {
        index_searcher->_search(query.get(), [&result](const int32_t docid, const float_t /*score*/) {
            // docid equal to rowid in segment
            result.add(docid);
        });
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::InternalError(fmt::format("CLuceneError occured, error msg: {}", e.what()));
    }
    bit_map->swap(result);
    return Status::OK();
}

Status StringCLuceneInvertedReader::try_query(OlapReaderStatistics* stats, const std::string& column_name,
                                              const void* query_value, InvertedIndexQueryType query_type,
                                              uint32_t* count) {
    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    auto act_len = strnlen(search_query->data, search_query->size);
    std::string search_str(search_query->data, act_len);
    DLOG(INFO) << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << search_str;
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());
    std::wstring search_str_ws = std::wstring(search_str.begin(), search_str.end());

    // unique_ptr with custom deleter
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term{
            _CLNEW lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str()),
            [](lucene::index::Term* term) { _CLDECDELETE(term) }};
    std::unique_ptr<lucene::search::Query> query;

    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
        query = std::make_unique<lucene::search::TermQuery>(term.get());
        break;
    case InvertedIndexQueryType::LESS_THAN_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(nullptr, term.get(), false);
        break;
    case InvertedIndexQueryType::LESS_EQUAL_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(nullptr, term.get(), true);
        break;
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(term.get(), nullptr, false);
        break;
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY:
        query = std::make_unique<lucene::search::RangeQuery>(term.get(), nullptr, true);
        break;
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
        query = std::make_unique<lucene::search::FuzzyQuery>(term.get());
        break;
    default:
        return Status::InternalError(fmt::format("Do not support such query {}", enum_to_string(query_type)));
    }

    if (!indexExists(_index_path)) {
        LOG(WARNING) << "inverted index path: " << _index_path << " not exist.";
        return Status::InternalError(fmt::format("Not exists index_file {}", _index_path.c_str()));
    }

    std::shared_ptr<lucene::search::IndexSearcher> index_searcher;
    {
        auto* directory = lucene::store::FSDirectory::getDirectory(_index_path.c_str());

        index_searcher = std::make_shared<lucene::search::IndexSearcher>(directory, true);
        _CLDECDELETE(directory)
    }

    try {
        index_searcher->_search(query.get(), [&count](const int32_t /*docid*/, const float_t /*score*/) {
            // docid equal to rowid in segment
            (*count)++;
        });
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::InternalError(fmt::format("CLuceneError occured, error msg: {}", e.what()));
    }
    return Status::OK();
}

Status FullTextCLuceneInvertedReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                                            const void* query_value, InvertedIndexQueryType query_type,
                                            roaring::Roaring* bit_map) {
    return Status::InternalError("Not supported yet");
}

Status FullTextCLuceneInvertedReader::try_query(OlapReaderStatistics* stats, const std::string& column_name,
                                                const void* query_value, InvertedIndexQueryType query_type,
                                                uint32_t* count) {
    return Status::InternalError("Not supported yet");
}

Status BKDIndexReader::query(OlapReaderStatistics* stats, const std::string& column_name, const void* query_value,
                             InvertedIndexQueryType query_type, roaring::Roaring* bit_map) {
    return Status::InternalError("Not supported yet");
}

Status BKDIndexReader::try_query(OlapReaderStatistics* stats, const std::string& column_name, const void* query_value,
                                 InvertedIndexQueryType query_type, uint32_t* count) {
    return Status::InternalError("Not supported yet");
}

} // namespace starrocks