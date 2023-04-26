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

#include "storage/tablet_reader_params.h"

#include "thrift/protocol/TDebugProtocol.h"

namespace starrocks {

TabletReaderParams::TabletReaderParams() = default;

std::string TabletReaderParams::to_string() const {
    std::stringstream ss;

    ss << "reader_type=" << reader_type << " skip_aggregation=" << skip_aggregation << " range=" << range
       << " end_range=" << end_range;

    for (auto& key : start_key) {
        ss << " keys=" << key;
    }

    for (auto& key : end_key) {
        ss << " end_keys=" << key;
    }
    return ss.str();
}

std::string to_string(TabletReaderParams::RangeStartOperation range_start_op) {
    switch (range_start_op) {
    case TabletReaderParams::RangeStartOperation::GT:
        return "GT";
    case TabletReaderParams::RangeStartOperation::GE:
        return "GE";
    case TabletReaderParams::RangeStartOperation::EQ:
        return "EQ";
    default:
        return "Unknown";
    }
}

std::string to_string(TabletReaderParams::RangeEndOperation range_end_op) {
    switch (range_end_op) {
    case TabletReaderParams::RangeEndOperation::LT:
        return "LT";
    case TabletReaderParams::RangeEndOperation::LE:
        return "LE";
    case TabletReaderParams::RangeEndOperation::EQ:
        return "EQ";
    default:
        return "Unknown";
    }
}

std::ostream& operator<<(std::ostream& os, TabletReaderParams::RangeStartOperation range_start_op) {
    os << to_string(range_start_op);
    return os;
}

std::ostream& operator<<(std::ostream& os, TabletReaderParams::RangeEndOperation range_end_op) {
    os << to_string(range_end_op);
    return os;
}

} // namespace starrocks
