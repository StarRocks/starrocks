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
#include <memory>
#include <string>
#include <string_view>

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
 * @brief Character predicate interface for text tokenization
 * Defines how to determine token characters and normalize them
 */
class CharPredicate {
public:
    virtual ~CharPredicate() = default;

    /**
     * @brief Check if character should be included in token
     * @param c Character to check
     * @return true if character is part of token
     */
    virtual bool is_token_char(char c) const = 0;

    /**
     * @brief Normalize character (e.g., convert to lowercase)
     * @param c Character to normalize
     * @return Normalized character
     */
    virtual char normalize(char c) const = 0;
};

/**
 * @brief Optimized character predicate with lookup tables for ASCII text
 * Based on CLucene's isalnumHits and isalphaNormalizeHits arrays
 */
class SimpleCharPredicate : public CharPredicate {
public:
    SimpleCharPredicate();
    bool is_token_char(char c) const override;
    char normalize(char c) const override;

private:
    static const size_t LOOKUP_SIZE = 256;
    bool _token_char_table[LOOKUP_SIZE];
    char _normalize_table[LOOKUP_SIZE];
};

/**
 * @brief Core tokenizer implementation for simple text analysis
 * Simplified version of CLucene's CharTokenizer using Slice-based tokens
 */
class SimpleAnalyzer {
public:
    /**
     * @brief Constructor
     * @param max_token_length Maximum token length (default: 255)
     * @param normalize_case Whether to normalize case (default: false for maximum performance)
     */
    explicit SimpleAnalyzer(
        size_t max_token_length = 255,
        bool normalize_case = true
    );

    /**
     * @brief Tokenize text and output tokens to provided vector
     * @param mutable_text Input text buffer (will be modified for case normalization)
     * @param text_size Size of input text
     * @param tokens Output vector to store tokens with their positions
     */
    void tokenize(char* mutable_text, size_t text_size, std::vector<SliceToken>& tokens) const;

    /**
     * @brief Set maximum token length
     * @param length Maximum length for tokens
     */
    void set_max_token_length(size_t length) { _max_token_length = length; }

    /**
     * @brief Get maximum token length
     * @return Current maximum token length
     */
    size_t get_max_token_length() const { return _max_token_length; }

private:
    std::shared_ptr<CharPredicate> _predicate;
    size_t _max_token_length;
    bool _normalize_case;
};

} // namespace starrocks
