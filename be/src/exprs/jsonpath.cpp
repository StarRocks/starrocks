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

#include "exprs/jsonpath.h"

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <algorithm>
#include <boost/tokenizer.hpp>
#include <memory>

#include "column/column_viewer.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "glog/logging.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "util/json.h"
#include "velocypack/vpack.h"

namespace starrocks {

// Regex for match "arr[0][1]", with two capture groups: variable-name, array-indices
static const re2::RE2 JSONPATH_PATTERN(R"(^([^\"\[\]]*)((?:\[(?:[0-9\:\*]+)\])*))", re2::RE2::Quiet);
// Regex for match "[0]"
static const re2::RE2 ARRAY_INDEX_PATTERN(R"(\[([0-9\:\*]+)\])");
static const re2::RE2 ARRAY_SINGLE_SELECTOR(R"(\d+)", re2::RE2::Quiet);
static const re2::RE2 ARRAY_SLICE_SELECTOR(R"(\d+\:\d+)", re2::RE2::Quiet);
static const std::string JSONPATH_ROOT = "$";

bool ArraySelectorSingle::match(const std::string& input) {
    return RE2::FullMatch(input, ARRAY_SINGLE_SELECTOR);
}

bool ArraySelectorWildcard::match(const std::string& input) {
    return input == "*";
}

bool ArraySelectorSlice::match(const std::string& input) {
    return RE2::FullMatch(input, ARRAY_SLICE_SELECTOR);
}

void ArraySelectorSingle::iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) {
    try {
        callback(array_slice.at(index));
    } catch (const vpack::Exception& e) {
        if (e.errorCode() == vpack::Exception::IndexOutOfBounds) {
            callback(noneJsonSlice());
        }
    }
}

void ArraySelectorWildcard::iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) {
    for (auto item : vpack::ArrayIterator(array_slice)) {
        callback(item);
    }
}

void ArraySelectorSlice::iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) {
    int index = 0;
    for (auto item : vpack::ArrayIterator(array_slice)) {
        if (left <= index && index < right) {
            callback(item);
        } else if (index >= right) {
            break;
        }
        index++;
    }
}

// 1. arr[x] select the x th element
// 2. arr[*] select all elements
// 3. arr[1:3] select slice of elements
Status ArraySelector::parse(const std::string& index, std::unique_ptr<ArraySelector>* output) {
    if (index.empty()) {
        *output = std::make_unique<ArraySelectorNone>();
        return Status::OK();
    } else if (ArraySelectorSingle::match(index)) {
        StringParser::ParseResult result;
        int index_int = StringParser::string_to_int<int>(index.c_str(), index.length(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }
        *output = std::make_unique<ArraySelectorSingle>(index_int);
        return Status::OK();
    } else if (ArraySelectorWildcard::match(index)) {
        *output = std::make_unique<ArraySelectorWildcard>();
        return Status::OK();
    } else if (ArraySelectorSlice::match(index)) {
        std::vector<std::string> slices = strings::Split(index, ":");
        if (slices.size() != 2) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }

        StringParser::ParseResult result;
        int left = StringParser::string_to_int<int>(slices[0].c_str(), slices[0].length(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }
        int right = StringParser::string_to_int<int>(slices[1].c_str(), slices[1].length(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }

        *output = std::make_unique<ArraySelectorSlice>(left, right);
        return Status::OK();
    }

    return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
}

Status JsonPathPiece::parse(const std::string& path_string, std::vector<JsonPathPiece>* parsed_paths) {
    if (path_string.size() == 0) return Status::InvalidArgument("Empty json path");

    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    std::vector<std::string> path_exprs;
    try {
        boost::tokenizer<boost::escaped_list_separator<char>> tok(path_string,
                                                                  boost::escaped_list_separator<char>("\\", ".", "\""));
        path_exprs.assign(tok.begin(), tok.end());
    } catch (const boost::escaped_list_error& e) {
        return Status::InvalidArgument(strings::Substitute("Invalid json path $0", e.what()));
    }

    for (int i = 0; i < path_exprs.size(); i++) {
        std::string variable;
        std::string array_pieces;
        auto& current = path_exprs[i];

        if (i == 0) {
            std::shared_ptr<ArraySelector> selector(new ArraySelectorNone());
            if (current != "$") {
                parsed_paths->emplace_back(JsonPathPiece("$", std::move(selector)));
            } else {
                parsed_paths->emplace_back(JsonPathPiece("$", std::move(selector)));
                continue;
            }
        }

        if (!RE2::FullMatch(current, JSONPATH_PATTERN, &variable, &array_pieces)) {
            parsed_paths->emplace_back("", std::unique_ptr<ArraySelector>(new ArraySelectorNone()));
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", path_exprs[i]));
        } else if (array_pieces.empty()) {
            // No array selector
            std::unique_ptr<ArraySelector> selector;
            RETURN_IF_ERROR(ArraySelector::parse(array_pieces, &selector));
            parsed_paths->emplace_back(JsonPathPiece(variable, std::move(selector)));
        } else {
            // Cosume multiple array selector
            re2::StringPiece array_piece(array_pieces);
            std::string single_piece;
            while (RE2::Consume(&array_piece, ARRAY_INDEX_PATTERN, &single_piece)) {
                std::unique_ptr<ArraySelector> selector;
                RETURN_IF_ERROR(ArraySelector::parse(single_piece, &selector));
                parsed_paths->emplace_back(JsonPathPiece(variable, std::move(selector)));
                variable = "";
            }
        }
    }

    return Status::OK();
}

vpack::Slice JsonPathPiece::extract(const JsonValue* json, const std::vector<JsonPathPiece>& jsonpath,
                                    vpack::Builder* b) {
    return extract(json->to_vslice(), jsonpath, 1, b);
}

vpack::Slice JsonPathPiece::extract(vpack::Slice root, const std::vector<JsonPathPiece>& jsonpath, int path_index,
                                    vpack::Builder* builder) {
    vpack::Slice current_value = root;

    for (int i = path_index; i < jsonpath.size(); i++) {
        auto& path_item = jsonpath[i];
        auto item_key = path_item.key;
        auto& array_selector = path_item.array_selector;

        vpack::Slice next_item = current_value;
        if (item_key == JSONPATH_ROOT) {
            // Reset the iterator to root
            next_item = root;
        } else if (!item_key.empty()) {
            // Iterate to a sub-field
            if (!current_value.isObject()) {
                return noneJsonSlice();
            }

            next_item = current_value.get(item_key);
        }
        if (next_item.isNone()) {
            return noneJsonSlice();
        }

        // TODO(mofei) refactor it to ArraySelector
        switch (array_selector->type) {
        case INVALID:
            DCHECK(false);
        case NONE:
            break;
        case SINGLE: {
            if (!next_item.isArray()) {
                return noneJsonSlice();
            }
            array_selector->iterate(next_item, [&](vpack::Slice array_item) { next_item = array_item; });
            break;
        }
        case WILDCARD:
        case SLICE: {
            if (!next_item.isArray()) {
                return noneJsonSlice();
            }
            {
                builder->clear();
                vpack::ArrayBuilder ab(builder);
                array_selector->iterate(next_item, [&](vpack::Slice array_item) {
                    auto sub = extract(array_item, jsonpath, i + 1, builder);
                    if (!sub.isNone()) {
                        builder->add(sub);
                    }
                });
            }
            return builder->slice();
        }
        }

        current_value = next_item;
    }

    return current_value;
}

void JsonPath::reset(const JsonPath& rhs) {
    paths = rhs.paths;
}

void JsonPath::reset(JsonPath&& rhs) {
    paths = std::move(rhs.paths);
}

StatusOr<JsonPath> JsonPath::parse(Slice path_string) {
    std::vector<JsonPathPiece> pieces;
    RETURN_IF_ERROR(JsonPathPiece::parse(path_string.to_string(), &pieces));
    return JsonPath(pieces);
}

vpack::Slice JsonPath::extract(const JsonValue* json, const JsonPath& jsonpath, vpack::Builder* b) {
    return JsonPathPiece::extract(json, jsonpath.paths, b);
}

bool JsonPath::starts_with(const JsonPath* other) const {
    if (other->paths.size() > paths.size()) {
        // this: a.b, other: a.b.c.d
        return false;
    }

    size_t i = 0;
    bool eq_key = true;
    for (; i < other->paths.size(); i++) {
        auto& this_path = paths[i];
        auto& other_path = other->paths[i];
        if (this_path.key != other_path.key) {
            eq_key = false;
            break;
        }
        if (!this_path.array_selector->match(*other_path.array_selector)) {
            break;
        }
    }

    if (i == 0) {
        return false;
    }
    return eq_key;
}

StatusOr<JsonPath*> JsonPath::relativize(const JsonPath* other, JsonPath* output_root) const {
    if (other->paths.size() > paths.size()) {
        // this: a.b, other: a.b.c.d
        return Status::InvalidArgument("Unsupported rollup json path");
    }

    size_t i = 0;
    for (; i < other->paths.size(); ++i) {
        auto& this_path = paths[i];
        auto& other_path = other->paths[i];
        if (this_path.key != other_path.key) {
            break;
        }
        if (!this_path.array_selector->match(*other_path.array_selector)) {
            if (UNLIKELY(NONE != other_path.array_selector->type)) {
                return Status::InvalidArgument(
                        fmt::format("Unsupported json path type: {}", other_path.array_selector->type));
            }
            output_root->paths.emplace_back("", this_path.array_selector);
            i++; // to next
            break;
        }
    }

    for (; i < paths.size(); ++i) {
        output_root->paths.emplace_back(paths[i]);
    }

    if (this->paths[0].key == "$" && !output_root->paths.empty()) {
        output_root->paths.insert(output_root->paths.cbegin(), this->paths[0]);
    }
    return output_root;
}

} // namespace starrocks
