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

#include <utility>

#include "exprs/function_helper.h"
#include "velocypack/vpack.h"

namespace starrocks {

enum ArraySelectorType {
    INVALID,
    NONE,
    SINGLE,
    WILDCARD,
    SLICE,
};

namespace vpack = arangodb::velocypack;

// Array selector syntax:
// 1. arr[x] select the x th element
// 2. arr[*] select all elements
// 3. arr[1:3] select slice of elements
struct ArraySelector {
    ArraySelectorType type = INVALID;

    ArraySelector() = default;
    virtual ~ArraySelector() = default;

    static Status parse(const std::string& str, std::unique_ptr<ArraySelector>* output);

    static bool match(const std::string& input) { return false; }

    virtual void iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) = 0;
};

struct ArraySelectorNone final : public ArraySelector {
    ArraySelectorNone() { type = NONE; }

    void iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) override { return; }
};

struct ArraySelectorSingle final : public ArraySelector {
    int index;

    ArraySelectorSingle(int index) : index(index) { type = SINGLE; }

    static bool match(const std::string& input);

    void iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) override;
};

struct ArraySelectorWildcard final : public ArraySelector {
    ArraySelectorWildcard() { type = WILDCARD; }

    static bool match(const std::string& input);

    void iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) override;
};

struct ArraySelectorSlice final : public ArraySelector {
    int left, right;

    ArraySelectorSlice(int left, int right) : left(left), right(right) { type = SLICE; }

    static bool match(const std::string& input);

    void iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) override;
};

// JsonPath implement that support array building
struct JsonPathPiece {
    std::string key;
    std::shared_ptr<ArraySelector> array_selector;

    JsonPathPiece(std::string key, std::shared_ptr<ArraySelector> selector)
            : key(std::move(key)), array_selector(std::move(std::move(selector))) {}

    JsonPathPiece(std::string key, ArraySelector* selector) : key(std::move(key)), array_selector(selector) {}

    static Status parse(const std::string& path_string, std::vector<JsonPathPiece>* parsed_path);

    static vpack::Slice extract(const JsonValue* json, const std::vector<JsonPathPiece>& jsonpath, vpack::Builder* b);

    // extract slice from root
    static vpack::Slice extract(vpack::Slice root, const std::vector<JsonPathPiece>& jsonpath, int path_index,
                                vpack::Builder* builder);

    // collect json object into builder
    static void collect(vpack::Slice root, const std::vector<JsonPathPiece>& jsonpath, int path_index,
                        vpack::Builder* builder);
};

struct JsonPath {
    std::vector<JsonPathPiece> paths;

    explicit JsonPath(std::vector<JsonPathPiece> value) : paths(std::move(value)) {}
    JsonPath() = default;
    JsonPath(JsonPath&&) = default;
    JsonPath(const JsonPath& rhs) = default;
    ~JsonPath() = default;

    void reset(const JsonPath& rhs);
    void reset(JsonPath&& rhs);

    static StatusOr<JsonPath> parse(Slice path_string);
    static vpack::Slice extract(const JsonValue* json, const JsonPath& jsonpath, vpack::Builder* b);
};

} // namespace starrocks
