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

#include "connector/common/utils.h"

#include <boost/algorithm/string.hpp>

#include "base/compression/compression_utils.h"
#include "formats/utils.h"

namespace starrocks::connector {

std::string normalize_format_name(std::string format) {
    boost::algorithm::trim(format);
    boost::algorithm::to_lower(format);
    boost::algorithm::trim_left_if(format, boost::is_any_of("."));

    // Strip any accidental extension chain like "csv.gz.csv" to keep format canonical.
    auto dot = boost::algorithm::find_first(format, ".");
    if (!dot.empty()) {
        format.erase(dot.begin(), format.end());
    }
    return format;
}

StatusOr<std::string> build_canonical_file_suffix(const std::string& format, TCompressionType::type compression_type) {
    if (format.empty()) {
        return Status::InvalidArgument("file format is empty");
    }
    if (format == formats::CSV) {
        ASSIGN_OR_RETURN(std::string compression_suffix, CompressionUtils::to_compression_ext(compression_type));
        return format + compression_suffix;
    }
    return format;
}

} // namespace starrocks::connector
