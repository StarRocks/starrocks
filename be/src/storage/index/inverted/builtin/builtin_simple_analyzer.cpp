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
#include "storage/index/inverted/builtin/builtin_simple_analyzer.h"

#include <algorithm>
#include <cctype>
#include <cstring>

namespace starrocks {

//==============================================================================
// SimpleCharPredicate Implementation
//==============================================================================

SimpleCharPredicate::SimpleCharPredicate() {
    // Initialize lookup tables based on ASCII character classification
    // This is simplified compared to CLucene's full Unicode support
    for (size_t i = 0; i < LOOKUP_SIZE; ++i) {
        char c = static_cast<char>(i);

        // Token character table: alphanumeric characters
        _token_char_table[i] = std::isalnum(static_cast<unsigned char>(c)) != 0;

        // Normalize table: convert uppercase to lowercase
        _normalize_table[i] = std::tolower(static_cast<unsigned char>(c));
    }
}

bool SimpleCharPredicate::is_token_char(char c) const {
    size_t index = static_cast<unsigned char>(c);
    if (index >= LOOKUP_SIZE) {
        return false; // Non-ASCII characters are not considered token chars in this simplified version
    }
    return _token_char_table[index];
}

char SimpleCharPredicate::normalize(char c) const {
    size_t index = static_cast<unsigned char>(c);
    if (index >= LOOKUP_SIZE) {
        return c; // Return unchanged for non-ASCII
    }
    return _normalize_table[index];
}

//==============================================================================
// SimpleAnalyzer Implementation
//==============================================================================

SimpleAnalyzer::SimpleAnalyzer(
    size_t max_token_length,
    bool normalize_case
) : _max_token_length(max_token_length), _normalize_case(normalize_case) {
    _predicate = std::make_shared<SimpleCharPredicate>();
}

void SimpleAnalyzer::tokenize(char* mutable_text, size_t text_size, std::vector<SliceToken>& tokens) const {
    tokens.clear();

    if (mutable_text == nullptr || text_size == 0) {
        return;
    }
    tokens.reserve(text_size);

    size_t offset = 0;
    size_t position = 0;

    while (offset < text_size) {
        char c = mutable_text[offset];

        // Skip non-token characters
        if (!_predicate->is_token_char(c)) {
            ++offset;
            continue;
        }

        // Found start of token
        size_t token_start = offset;
        size_t token_length = 0;

        // Find end of token
        while (offset < text_size && _predicate->is_token_char(mutable_text[offset])) {
            // Check token length limit (in characters)
            if (token_length >= _max_token_length) {
                break;
            }

            ++offset;
            ++token_length;
        }

        // Create token if we have content
        if (token_length > 0) {
            if (_normalize_case) {
                for (size_t i = 0; i < token_length; ++i) {
                    mutable_text[token_start + i] = _predicate->normalize(mutable_text[token_start + i]);
                }
            }
            tokens.emplace_back(mutable_text + token_start, token_length, position++);
        }
    }
}

} // namespace starrocks
