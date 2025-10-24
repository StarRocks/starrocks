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

#include "common/statusor.h"
#include "util/slice.h"

namespace starrocks {

/**
 * @brief Represents a token extracted from text with its position information.
 */
struct SliceToken {
    std::string holder; // Token text data holder
    Slice text;         // Token text data
    size_t position;    // Position of this token in the sequence

    SliceToken(const std::string& str, const size_t pos)
            : holder(str), text(holder.data(), holder.size()), position(pos) {}

    // Constructor for raw char buffer
    SliceToken(const char* data, const size_t len, const size_t pos) : text(data, len), position(pos) {}

    bool empty() const { return text.empty(); }

    // Get string_view for efficient access
    std::string_view view() const { return std::string_view(text.data, text.size); }

    // Comparison operators
    bool operator==(const SliceToken& other) const { return text == other.text; }
};

class Tokenizer {
public:
    virtual ~Tokenizer() = default;

    /**
     * @brief Tokenize text and output tokens to provided vector
     * @param text Input text buffer (will be modified for case normalization)
     * @return tokens Output vector to store tokens with their positions
     */
    virtual StatusOr<std::vector<SliceToken>> tokenize(const Slice* text) = 0;

    /**
     * @brief Tokenize text and output tokens to provided vector
     * @param mutable_text Input text buffer (will be modified for case normalization)
     * @param text_size Size of input text
     * @return tokens Output vector to store tokens with their positions
     */
    virtual StatusOr<std::vector<SliceToken>> tokenize(char* mutable_text, size_t text_size) {
        const Slice text(mutable_text, text_size);
        return tokenize(&text);
    }
};

class NoneTokenizer final : public Tokenizer {
public:
    NoneTokenizer() = default;
    ~NoneTokenizer() override = default;

    StatusOr<std::vector<SliceToken>> tokenize(const Slice* text) override {
        return std::vector({SliceToken{text->data, text->size, 0}});
    }
};

} // namespace starrocks
