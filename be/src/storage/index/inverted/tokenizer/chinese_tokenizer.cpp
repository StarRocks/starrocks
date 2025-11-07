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

#include "chinese_tokenizer.h"

#include <CLucene.h>
#include <CLucene/analysis/standard/StandardAnalyzer.h>

#include <boost/locale/encoding_utf.hpp>

#include "common/config.h"

namespace starrocks {

ChineseTokenizer::ChineseTokenizer()
        : _analyzer(std::make_unique<lucene::analysis::standard::StandardAnalyzer>()),
          _char_string_reader(std::make_unique<lucene::util::StringReader>(L"cjk")) {}

ChineseTokenizer::~ChineseTokenizer() = default;

StatusOr<std::vector<SliceToken>> ChineseTokenizer::tokenize(const Slice* text) {
    // For all kinds of CLucene analyzers, we need to process the text as follows:
    // 1. Convert the text to wstring
    // 2. Tokenize the wstring
    // 3. Convert the tokens to string
    // 4. Add the string to the bitmap index
    std::wstring tchar = boost::locale::conv::utf_to_utf<TCHAR>(text->data, text->data + text->size);

    _char_string_reader->init(tchar.c_str(), tchar.size(), false);
    auto stream = _analyzer->reusableTokenStream(L"", _char_string_reader.get());

    size_t position = 0;
    std::vector<SliceToken> tokens;

    lucene::analysis::Token token;
    while (stream->next(&token)) {
        if (token.termLength() != 0) {
            std::string str =
                    boost::locale::conv::utf_to_utf<char>(token.termBuffer(), token.termBuffer() + token.termLength());
            tokens.emplace_back(str, position++);
        }
    }
    return std::move(tokens);
}

} // namespace starrocks
