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

#include <vector>

#include "storage/index/inverted/tokenizer/tokenizer.h"

namespace starrocks {

/**
 * @brief Core tokenizer implementation for simple text analysis
 * Simplified version of CLucene's CharTokenizer using Slice-based tokens
 */
class SimpleAnalyzer : public Tokenizer {
public:
    /**
     * @brief Constructor
     * @param normalize_case Whether to normalize case (default: false for maximum performance)
     */
    SimpleAnalyzer(bool normalize_case = true);
    ~SimpleAnalyzer() override = default;

    StatusOr<std::vector<SliceToken>> tokenize(const Slice* text) override;

    /**
     * @brief Tokenize text and output tokens to provided vector
     * @param mutable_text Input text buffer (will be modified for case normalization)
     * @param text_size Size of input text
     * @return tokens Output vector to store tokens with their positions
     */
    StatusOr<std::vector<SliceToken>> tokenize(char* mutable_text, size_t text_size) override;

private:
    static const size_t LOOKUP_SIZE = 256;

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
    bool _token_char_table[LOOKUP_SIZE];
    char _normalize_table[LOOKUP_SIZE];
};

} // namespace starrocks
