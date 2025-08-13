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
#include <CLucene/config/repl_wchar.h>
#include <fmt/format.h>
#include <gflags/gflags.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <filesystem>
#include <iostream>
#include <roaring/roaring.hh>

#include "fs/fs.h"
#include "fs/fs_posix.h"
#include "storage/index/inverted/clucene/clucene_file_reader.h"
#include "storage/tablet_index.h"
#include "util/defer_op.h"

DECLARE_string(operation);
DEFINE_string(ivt_dir, "", "inverted index directory, required if ivt_file doesn't provided.");
DEFINE_string(ivt_file, "", "inverted index file, required if ivt_dir doesn't provided.");
DEFINE_string(term, "", "term");
DEFINE_string(field, "", "column name");
DEFINE_int32(num_docs, 100, "The maximum number of documents will be listed.");
DEFINE_bool(print_doc_id, false, "print doc id");

std::string get_usage_for_index_tool(const std::string& progname) {
    constexpr const char* const usage_msg = R"(
  {progname} is the StarRocks BE Inverted Index Meta tool.
    [CAUTION] Stop BE first before using this tool if modification will be made.

  Usage:
    term_position:
      {progname} --operation=term_position --field=<field> --term=<term> --ivt_dir=<ivt_dir> --ivt_file=<ivt_file> --num_docs=<num_docs>
    check_terms_stats：
      {progname} --operation=check_terms_stats --ivt_dir=<ivt_dir> --ivt_file=<ivt_file> --print_doc_id
)";
    return fmt::format(usage_msg, fmt::arg("progname", progname));
}

void term_position(const std::string& field, const std::string& term, const int32_t& num_docs,
                   const std::string& ivt_dir, const std::string& ivt_file) {
    std::wstring field_ws = std::wstring(field.begin(), field.end());
    std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(term);
    auto t = std::make_unique<lucene::index::Term>(field_ws.c_str(), ws_term.c_str());

    auto func = [&](lucene::index::TermPositions* term_positions) {
        const int32_t docId = term_positions->doc();
        const int32_t freq = term_positions->freq();

        std::vector<std::string> positions;
        for (int32_t i = 0; i < freq; ++i) {
            const int32_t pos = term_positions->nextPosition();
            positions.push_back(std::to_string(pos));
        }

        std::cerr << "DocId: " << docId << ", Freq: " << freq << ", Positions: " << boost::join(positions, ",")
                  << std::endl;
    };

    if (ivt_dir != "") {
        auto* directory = lucene::store::FSDirectory::getDirectory(ivt_dir.c_str());
        starrocks::DeferOp defer([&directory]() {
            if (directory != nullptr) {
                _CL_LDECREF(directory);
                directory->close();
            }
        });

        const auto searcher = std::make_unique<lucene::search::IndexSearcher>(directory);
        auto* reader = searcher->getReader();

        lucene::index::TermPositions* term_positions = reader->termPositions(t.get());

        int32_t cur = 0;
        while (cur++ < num_docs && term_positions->next()) {
            func(term_positions);
        }
        return;
    }

    if (ivt_file == "") {
        std::cerr << "empty ivt_file" << std::endl;
        return;
    }

    auto fs = starrocks::new_fs_posix();
    auto index_file_reader = std::make_unique<starrocks::CLuceneFileReader>(std::move(fs), ivt_file);
    auto st1 = index_file_reader->init(4096);
    if (!st1.ok()) {
        std::cerr << "Failed to init " << ivt_file << std::endl;
        return;
    }

    auto st2 = index_file_reader->get_all_directories();
    if (!st2.ok()) {
        std::cerr << "Failed to get all directories." << std::endl;
        return;
    }
    auto dirs = std::move(st2).value();

    int32_t cur = 0;
    for (const auto& dir : dirs | std::views::values) {
        const auto searcher = std::make_unique<lucene::search::IndexSearcher>(dir.get());
        auto* reader = searcher->getReader();

        lucene::index::TermPositions* term_positions = reader->termPositions(t.get());
        while (cur++ < num_docs && term_positions->next()) {
            func(term_positions);
        }
    }
}

void check_terms_stats(const std::string& ivt_dir, const std::string& ivt_file) {
    auto check = [&](lucene::index::IndexReader* r, lucene::store::Directory* directory) {
        printf("Max Docs: %d\n", r->maxDoc());
        printf("Num Docs: %d\n", r->numDocs());

        int64_t ver = r->getCurrentVersion(directory);
        printf("Current Version: %f\n", (float_t)ver);

        lucene::index::TermEnum* te = r->terms();
        int32_t nterms;
        for (nterms = 0; te->next(); nterms++) {
            /* empty */
            std::string token = lucene_wcstoutf8string(te->term(false)->text(), te->term(false)->textLength());
            std::string field = lucene_wcstoutf8string(te->term(false)->field(), lenOfString(te->term(false)->field()));

            printf("Field: %s ", field.c_str());
            printf("Term: %s ", token.c_str());
            printf("Freq: %d\n", te->docFreq());
            if (FLAGS_print_doc_id) {
                lucene::index::TermDocs* td = r->termDocs(te->term());
                while (td->next()) {
                    printf("DocID: %d ", td->doc());
                    printf("TermFreq: %d\n", td->freq());
                }
                _CLLDELETE(td);
            }
        }
        printf("Term count: %d\n\n", nterms);
        te->close();
        _CLLDELETE(te);

        r->close();
        _CLLDELETE(r);
    };

    if (ivt_dir != "") {
        auto* directory = lucene::store::FSDirectory::getDirectory(ivt_dir.c_str());
        starrocks::DeferOp defer([&directory]() {
            if (directory != nullptr) {
                _CL_LDECREF(directory);
                directory->close();
            }
        });
        auto* r = lucene::index::IndexReader::open(directory);
        check(r, directory);
    }

    if (ivt_file == "") {
        std::cerr << "empty ivt_file" << std::endl;
        return;
    }

    auto fs = starrocks::new_fs_posix();
    auto index_file_reader = std::make_unique<starrocks::CLuceneFileReader>(std::move(fs), ivt_file);
    auto st1 = index_file_reader->init(4096);
    if (!st1.ok()) {
        std::cerr << "Failed to init " << ivt_file << ", reason: " << st1.detailed_message() << std::endl;
        return;
    }

    auto st2 = index_file_reader->get_all_directories();
    if (!st2.ok()) {
        std::cerr << "Failed to get all directories, reason: " << st2.status().detailed_message() << std::endl;
        return;
    }
    for (auto dirs = std::move(st2).value(); const auto& dir : dirs | std::views::values) {
        auto* r = lucene::index::IndexReader::open(dir.get());
        check(r, dir.get());
    }
}

int index_tool_main(int argc, char** argv) {
    const std::string usage = get_usage_for_index_tool(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "term_position") {
        if (FLAGS_term == "" || FLAGS_field == "" || (FLAGS_ivt_dir == "" && FLAGS_ivt_file == "")) {
            std::cerr << "invalid params for term_position" << std::endl;
            return -1;
        }
        try {
            //try to list from directory's all files
            term_position(FLAGS_field, FLAGS_term, FLAGS_num_docs, FLAGS_ivt_dir, FLAGS_ivt_file);
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when list term positions, reason: " << err.what() << std::endl;
            return -1;
        }
    } else if (FLAGS_operation == "check_terms_stats") {
        if (FLAGS_ivt_dir == "" && FLAGS_ivt_file == "") {
            std::cerr << "invalid params for term_position" << std::endl;
            return -1;
        }
        try {
            //try to list from directory's all files
            check_terms_stats(FLAGS_ivt_dir, FLAGS_ivt_file);
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when list term stats, reason: " << err.what() << std::endl;
            return -1;
        }
    } else {
        std::cerr << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
        return -1;
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}