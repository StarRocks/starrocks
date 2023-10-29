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

#include <CLucene.h>
#include <CLucene/document/Document.h>
#include <CLucene/document/Field.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <gtest/gtest.h>

#include "CLucene/LuceneThreads.h"
#include "roaring/roaring.hh"

namespace starrocks {

class InvertedIndexTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(InvertedIndexTest, test_clucene) {
    // write
    auto analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<TCHAR>>();
    auto ram_directory = std::make_unique<lucene::store::RAMDirectory>();
    auto index_writer = std::make_unique<lucene::index::IndexWriter>(ram_directory.get(), analyzer.get(), true);
    int field_config = int(lucene::document::Field::STORE_NO) | int(lucene::document::Field::INDEX_NONORMS) |
                       int(lucene::document::Field::INDEX_UNTOKENIZED);

    std::wstring fn = L"f1";
    auto* field = new lucene::document::Field(fn.c_str(), field_config);

    std::string data = "ss";
    auto field_value = lucene::util::Misc::_charToWide(data.c_str(), data.size());
    field->setValue(field_value, false);

    auto doc = std::make_unique<lucene::document::Document>();
    doc->add(*field);
    index_writer->addDocument(doc.get());

    index_writer->flush();
    index_writer->close(true);

    // search
    auto searcher = std::make_unique<lucene::search::IndexSearcher>(ram_directory.get());

    std::wstring search_word(data.begin(), data.end());
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term{
            _CLNEW lucene::index::Term(fn.c_str(), search_word.c_str()),
            [](lucene::index::Term* term) { _CLDECDELETE(term) }};
    auto term_query = std::make_unique<lucene::search::TermQuery>(term.get());

    roaring::Roaring result;
    searcher->_search(term_query.get(), [&result](const int32_t docid, const float_t /*score*/) {
        // docid equal to rowid in segment
        result.add(docid);
    });

    ASSERT_TRUE(!result.isEmpty());
    std::cout << "hits words size = " << result.cardinality() << std::endl;
}

} // namespace starrocks
