// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/tablet_reader_params.h"

#include <thrift/protocol/TDebugProtocol.h>

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
