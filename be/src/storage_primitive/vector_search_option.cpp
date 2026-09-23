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

#include "storage_primitive/vector_search_option.h"

#include <cmath>
#include <cstring>

#include "base/string/string_parser.hpp"
#include "fmt/format.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

// Decode the ANN query vector out of the plan, once per fragment instance, for the owner to share
// with every scan range.
//
// `query_vector_f32` is little-endian float32, already the layout tenann reads, so decoding is a
// memcpy. `query_vector` (one decimal string per dimension) comes from an FE older than this BE,
// which the supported upgrade order (BE/CN first, then FE) produces mid-upgrade. That order cannot
// produce the reverse, so neither field set means somebody upgraded out of order: fail loudly
// rather than hand tenann an empty vector and return quietly wrong rows.
Status decode_vector_query_vector(const TVectorSearchOptions& options, std::shared_ptr<const std::vector<float>>* out) {
    auto decoded = std::make_shared<std::vector<float>>();

    if (options.__isset.query_vector_f32) {
        const std::string& blob = options.query_vector_f32;
        if (blob.size() % sizeof(float) != 0) {
            return Status::InvalidArgument(
                    fmt::format("query vector binary size {} is not a multiple of {}", blob.size(), sizeof(float)));
        }
        decoded->resize(blob.size() / sizeof(float));
        std::memcpy(decoded->data(), blob.data(), blob.size());
    } else if (!options.query_vector.empty()) {
        decoded->reserve(options.query_vector.size());
        for (const std::string& element : options.query_vector) {
            StringParser::ParseResult parse_result;
            float value = StringParser::string_to_float<float>(element.data(), element.size(), &parse_result);
            if (parse_result != StringParser::PARSE_SUCCESS) {
                return Status::InvalidArgument(
                        fmt::format("invalid query vector element for vector search: '{}'", element));
            }
            decoded->push_back(value);
        }
    } else {
        return Status::InternalError(
                "vector search is enabled but the plan carries no query vector; the FE may have been "
                "upgraded before the BE/CN (the supported order is BE/CN first)");
    }

    // The vector reaches tenann as a raw pointer, where a non-finite element makes the distance
    // comparisons meaningless rather than loud. The FE rejects these; re-check rather than trust.
    for (float value : *decoded) {
        if (!std::isfinite(value)) {
            return Status::InvalidArgument("query vector contains a non-finite element");
        }
    }

    *out = std::move(decoded);
    return Status::OK();
}

} // namespace starrocks
