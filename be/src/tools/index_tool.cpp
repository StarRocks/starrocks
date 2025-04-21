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
#include <fmt/format.h>
#include <gflags/gflags.h>

#include <boost/locale/encoding_utf.hpp>
#include <filesystem>
#include <iostream>
#include <roaring/roaring.hh>

#include "fs/fs.h"
#include "util/defer_op.h"

DECLARE_string(operation);
DEFINE_string(ivt_dir, "", "inverted index directory.");
DEFINE_string(term, "", "term");
DEFINE_string(column_name, "", "column name");

std::string get_usage_for_index_tool(const std::string& progname) {
    constexpr const char* const usage_msg = R"(
  {progname} is the StarRocks BE Inverted Index Meta tool.
    [CAUTION] Stop BE first before using this tool if modification will be made.

  Usage:
    term_query:
      {progname} --operation=term_query --ivt_dir=</path/to/ivt/dir> --term=<term> --column_name=<column_name>
    list_ivt:
      {progname} --operation=list_ivt --ivt_dir=</path/to/ivt/dir>
)";
    return fmt::format(usage_msg, fmt::arg("progname", progname));
}

void search(const std::string& dir, std::string& field, std::string& token) {
    auto* directory = lucene::store::FSDirectory::getDirectory(dir.c_str());
    starrocks::DeferOp d1([&]() {
        if (directory != nullptr) {
            _CL_LDECREF(directory);
            directory->close();
        }
    });
    lucene::search::IndexSearcher index_searcher(directory);

    std::cout << "version: " << (int32_t)(index_searcher.getReader()->getIndexVersion()) << std::endl;

    std::wstring field_ws(field.begin(), field.end());
    std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(token);

    lucene::index::Term* term = _CLNEW lucene::index::Term(field_ws.c_str(), ws_term.c_str());
    starrocks::DeferOp d2([&term]() { _CLDECDELETE(term); });

    std::unique_ptr<lucene::search::Query> query = std::make_unique<lucene::search::TermQuery>(term);

    int32_t total = 0;
    roaring::Roaring result;
    index_searcher._search(query.get(), [&result](const int32_t docid, const float_t /*score*/) {
        // docid equal to rowid in segment
        result.add(docid);
        std::cout << "RowID is " << docid << std::endl;
    });
    total += result.cardinality();
    std::cout << "Term queried count:" << total << std::endl;

    index_searcher.close();
}

void list_ivt(const std::string& dir) {
    auto* directory = lucene::store::FSDirectory::getDirectory(dir.c_str());
    starrocks::DeferOp d1([&]() {
        if (directory != nullptr) {
            _CL_LDECREF(directory);
            directory->close();
        }
    });

    lucene::index::IndexReader* reader = lucene::index::IndexReader::open(true, directory, 4096);
    starrocks::DeferOp d2([&]() { reader->close(); });
    for (int i = 0; i < reader->numDocs(); ++i) {
        lucene::document::Document document;
        if (!reader->document(i, document)) {
            std::cerr << "Failed to read document " << std::endl;
            return;
        }
    }
}

int index_tool_main(int argc, char** argv) {
    std::string usage = get_usage_for_index_tool(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "term_query") {
        if (FLAGS_ivt_dir == "" || FLAGS_term == "" || FLAGS_column_name == "") {
            std::cerr << "invalid params for term_query" << std::endl;
            return -1;
        }
        try {
            //try to search from directory's all files
            search(FLAGS_ivt_dir, FLAGS_column_name, FLAGS_term);
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when search term " << FLAGS_term << " for column " << FLAGS_column_name << ": "
                      << err.what() << std::endl;
            return -1;
        }
    } else if (FLAGS_operation == "list_ivt") {
        if (FLAGS_ivt_dir == "") {
            std::cerr << "invalid params for list." << std::endl;
        }
        try {
            list_ivt(FLAGS_ivt_dir);
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when list inverted index directory " << FLAGS_ivt_dir << ": " << err.what()
                      << std::endl;
            return -1;
        }
    } else {
        std::cerr << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
        return -1;
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}