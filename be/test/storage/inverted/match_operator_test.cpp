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

#include "storage/inverted/clucene/match_operator.h"

#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/document/Document.h>
#include <CLucene/document/Field.h>
#include <CLucene/util/Misc.h>
#include <gtest/gtest.h>

#include <vector>

#include "common/config.h"
#include "filesystem"
#include "fs/fs_util.h"
#include "roaring/roaring.hh"

namespace starrocks {

class MatchOperatorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {
        fs::remove_all(path1);
        fs::remove_all(path2);
        fs::remove_all(path3);
        fs::remove_all(path4);
    }

    std::string path1 = "inverted_index_test1";
    std::string path2 = "inverted_index_test2";
    std::string path3 = "inverted_index_test3";
    std::string path4 = "inverted_index_test4";

    std::string _chinese_source[13] = {"咏怀其一",      "咏怀其二",     "江南曲",   "白纻歌",
                                       "闻笛",          "望湖亭",       "海珠寺",   "香山何相国以除夕诗见寄步答",
                                       "送东阳马生序",  "送东阳马生序", "吴起守信", "王冕好学",
                                       "送天台陈庭学序"};
    std::string _english_source[30] = {
            "Unknown util",  "Unknown king", "yes wheel why no", "yes why no",   "Unknown yut",    "Unknown quit",
            "Unknown close", "Unknown king", "Unknown king",     "Unknown peak", "Unknown king",   "Unknown top",
            "Unknown low",   "Unknown hut",  "Unknown king",     "Unknown king", "Unknown please", "Unknown king",
            "Unknown king",  "Unknown king", "Unknown quit",     "Unknown king", "Unknown king",   "Unknown king",
            "Unknown king",  "Unknown king", "Unknown king",     "Unknown king", "Unknown king",   "Unknown king"};

    std::string _no_tokenized_source[30] = {"1035304846", "1060470678", "1062174088", "1029647167", "1013815306",
                                            "1099488629", "1016193277", "1052223411", "1018441698", "1093514402",
                                            "1090061359", "1087860169", "1081310879", "1027822180", "1077980535",
                                            "1046288723", "1019542457", "1081998606", "1026553053", "1021729127",
                                            "1099983318", "1069260426", "1041829821", "1031608061", "1019843378",
                                            "1068525188", "1039180740", "1092664859", "1083126452", "1087311191"};
    std::string _score_source[3] = {"this book is about english", "this book is about chinese",
                                    "this book is about japan"};
};

TEST_F(MatchOperatorTest, test_clucene_no_tokenized) {
    // write
    lucene::analysis::SimpleAnalyzer analyzer;
    lucene::store::FSDirectory* directory = lucene::store::FSDirectory::getDirectory(path1.c_str());
    lucene::index::IndexWriter index_writer(directory, &analyzer, true);
    std::wstring fu = L"fu";

    // untokenized field
    int field_untokenized_config = int(lucene::document::Field::STORE_NO) |
                                   int(lucene::document::Field::INDEX_NONORMS) |
                                   int(lucene::document::Field::INDEX_UNTOKENIZED);
    auto field_ut = _CLNEW lucene::document::Field(fu.c_str(), field_untokenized_config);
    lucene::document::Document doc;
    doc.add(*field_ut);

    for (const std::string& row : _no_tokenized_source) {
        auto field_value_ut = lucene::util::Misc::_charToWide(row.c_str());
        field_ut->setValue(field_value_ut, false);
        index_writer.addDocument(&doc);
    }

    index_writer.close();

    // search
    lucene::search::IndexSearcher searcher(directory);

    // term query
    {
        // where fu = "1090061359"
        std::string search_data = "1090061359";

        roaring::Roaring result;
        MatchTermOperator match_term_operator(&searcher, directory, fu, search_data);
        match_term_operator.match(&result);

        ASSERT_EQ(result.cardinality(), 1);
    }

    // wildcard query
    {
        // where fu like "%78601%"
        std::string search_data = "*78601*";

        roaring::Roaring result;
        MatchWildcardOperator wildcard_operator(&searcher, directory, fu, search_data);
        wildcard_operator.match(&result);
        ASSERT_EQ(result.cardinality(), 1);
    }

    // range query
    {
        // where fu >= "1090000000" and fu < "1100000000"
        lucene::search::BooleanQuery boolean_query;
        std::string search_start = "1090000000";
        std::string search_end = "1100000000";

        roaring::Roaring result_start;
        roaring::Roaring result_end;
        MatchGreatThanOperator great_than_operator(&searcher, directory, fu, search_start, true);
        MatchLessThanOperator less_than_operator(&searcher, directory, fu, search_end, false);

        great_than_operator.match(&result_start);
        less_than_operator.match(&result_end);

        ASSERT_EQ(result_start.and_cardinality(result_end), 5);
    }
    searcher.close();
    directory->close();
    _CLDELETE(directory)
}

TEST_F(MatchOperatorTest, test_clucene_english) {
    // write
    lucene::analysis::SimpleAnalyzer analyzer;
    lucene::store::FSDirectory* directory = lucene::store::FSDirectory::getDirectory(path2.c_str());
    lucene::index::IndexWriter index_writer(directory, &analyzer, true);

    std::wstring fen = L"fen";

    // tokenized by english field
    int field_en_config = int(lucene::document::Field::STORE_NO) | int(lucene::document::Field::INDEX_TOKENIZED);
    auto* field_en = _CLNEW lucene::document::Field(fen.c_str(), field_en_config);
    lucene::document::Document doc;
    doc.add(*field_en);

    for (const std::string& row : _english_source) {
        auto field_value_en = lucene::util::Misc::_charToWide(row.c_str());
        field_en->setValue(field_value_en, false);

        index_writer.addDocument(&doc);
    }

    index_writer.close();

    // search
    lucene::search::IndexSearcher searcher(directory);
    {
        // search english field
        std::string search_data = "king";
        MatchTermOperator match_term_operator(&searcher, directory, fen, search_data);

        roaring::Roaring result;
        match_term_operator.match(&result);
        ASSERT_EQ(result.cardinality(), 18);
    }

    // phrase query
    {
        // where match_phrase(fen, ["yes", "no"], 1) match "yes why no"
        lucene::search::PhraseQuery phrase_query;
        std::vector<std::string> search_words;
        search_words.emplace_back("yes");
        search_words.emplace_back("no");
        MatchPhraseOperator match_phrase_operator(&searcher, directory, fen, search_words, 1);

        roaring::Roaring result;
        match_phrase_operator.match(&result);
        ASSERT_EQ(result.cardinality(), 1);
    }
    searcher.close();
    directory->close();
    _CLDELETE(directory)
}

TEST_F(MatchOperatorTest, test_clucene_chinese) {
    lucene::analysis::LanguageBasedAnalyzer chinese_analyzer;
    chinese_analyzer.setLanguage(L"cjk");
    chinese_analyzer.setStem(false);

    lucene::store::FSDirectory* directory = lucene::store::FSDirectory::getDirectory(path3.c_str());
    lucene::index::IndexWriter index_writer(directory, &chinese_analyzer, true);

    std::wstring fch = L"fch";

    // tokenized by chinese field
    int field_ch_config = int(lucene::document::Field::STORE_NO) | int(lucene::document::Field::INDEX_TOKENIZED);
    auto field_ch = _CLNEW lucene::document::Field(fch.c_str(), field_ch_config);
    lucene::document::Document doc;
    doc.add(*field_ch);

    // write
    for (const std::string& row : _chinese_source) {
        const char* data2 = row.c_str();
        // Every time field->setValue() is called, the `resetValue` will be called, which deletes the reader's memory
        auto char_string_reader = _CLNEW lucene::util::SimpleInputStreamReader(
                _CLNEW lucene::util::AStringReader(data2), lucene::util::SimpleInputStreamReader::UTF8);

        field_ch->setValue(char_string_reader);

        index_writer.addDocument(&doc);
    }

    index_writer.close();

    // search
    lucene::search::IndexSearcher searcher(directory);
    {
        // search chinese field
        std::string search_data = "江南";

        roaring::Roaring result;
        MatchChineseTermOperator match_chinese_operator(&searcher, directory, fch, search_data, &chinese_analyzer);
        match_chinese_operator.match(&result);
        ASSERT_EQ(result.cardinality(), 1);
    }
    searcher.close();
    directory->close();
    _CLDELETE(directory)
}

TEST_F(MatchOperatorTest, test_clucene_score) {
    // write
    lucene::analysis::SimpleAnalyzer analyzer;
    lucene::store::FSDirectory* directory = lucene::store::FSDirectory::getDirectory(path4.c_str());
    lucene::index::IndexWriter index_writer(directory, &analyzer, true);

    std::wstring fen = L"fen";

    // tokenized by english field
    int field_en_config = int(lucene::document::Field::STORE_NO) | int(lucene::document::Field::INDEX_TOKENIZED);
    auto* field_en = new lucene::document::Field(fen.c_str(), field_en_config);
    lucene::document::Document doc;
    doc.add(*field_en);

    for (const std::string& row : _score_source) {
        auto field_value_en = lucene::util::Misc::_charToWide(row.c_str());
        field_en->setValue(field_value_en, false);

        index_writer.addDocument(&doc);
    }

    index_writer.close();

    // search
    lucene::search::IndexSearcher searcher(directory);
    {
        // search english field
        std::string search_data = "chinese";
        MatchTermOperator match_term_operator(&searcher, directory, fen, search_data);
        roaring::Roaring result;
        std::vector<float_t> scores;
        match_term_operator.match(&result, &scores);

        ASSERT_EQ(result.cardinality(), 1);
        ASSERT_GT(scores[0], 0);
    }
    searcher.close();
    directory->close();
    _CLDELETE(directory)
}

} // namespace starrocks
