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
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks {

/**
 * @brief Represents a token extracted from text with its position information.
 */
struct SliceToken {
    Slice text;           // Token text data
    size_t position;      // Position of this token in the sequence

    SliceToken() : text(), position(0) {}

    // Constructor for raw char buffer
    SliceToken(const char* data, size_t len, size_t pos)
        : text(data, len), position(pos) {}

    bool empty() const { return text.empty(); }

    // Get string_view for efficient access
    std::string_view view() const {
        return std::string_view(text.data, text.size);
    }

    // Comparison operators
    bool operator==(const SliceToken& other) const {
        return text == other.text;
    }
};

/**
 * @brief Core tokenizer implementation for simple text analysis
 * Simplified version of CLucene's CharTokenizer using Slice-based tokens
 */
class SimpleAnalyzer {
public:
    /**
     * @brief Constructor
     * @param normalize_case Whether to normalize case (default: false for maximum performance)
     * @param enable_stop_words Whether to filter out stop words (default: true)
     */
    SimpleAnalyzer(bool normalize_case = true, bool enable_stop_words = true);
    ~SimpleAnalyzer() = default;

    /**
     * @brief Tokenize text and output tokens to provided vector
     * @param mutable_text Input text buffer (will be modified for case normalization)
     * @param text_size Size of input text
     * @param tokens Output vector to store tokens with their positions
     */
    void tokenize(char* mutable_text, size_t text_size, std::vector<SliceToken>& tokens) const;

private:
    static const size_t LOOKUP_SIZE = 256;

    static const phmap::flat_hash_set<std::string> builtin_stop_words;

    bool _filter_by_stop_words(const Slice& token) const;

    /**
     * @brief Check if character should be included in token
     * @param c Character to check
     * @return true if character is part of token
     */
    inline bool _is_token_char(char c) const {
        size_t index = static_cast<unsigned char>(c);
        if (index >= LOOKUP_SIZE) {
            return false; // Non-ASCII characters are not considered token chars
        }
        return _token_char_table[index];
    }
    
    /**
     * @brief Normalize character (e.g., convert to lowercase)
     * @param c Character to normalize
     * @return Normalized character
     */
    inline char _normalize(char c) const {
        size_t index = static_cast<unsigned char>(c);
        if (index >= LOOKUP_SIZE) {
            return c; // Return unchanged for non-ASCII
        }
        return _normalize_table[index];
    }

    bool _normalize_case;
    bool _enable_stop_words;
    bool _token_char_table[LOOKUP_SIZE];
    char _normalize_table[LOOKUP_SIZE];
};

} // namespace starrocks
