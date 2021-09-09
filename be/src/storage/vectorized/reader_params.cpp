// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/reader_params.h"

#include <thrift/protocol/TDebugProtocol.h>

namespace starrocks::vectorized {

ReaderParams::ReaderParams() : version(-1, 0) {}

void ReaderParams::check_validation() const {
    if (version.first == -1) {
        LOG(FATAL) << "verison is not set. tablet=" << tablet->full_name();
    }
}

std::string ReaderParams::to_string() const {
    std::stringstream ss;

    ss << "tablet=" << tablet->full_name() << " reader_type=" << reader_type << " skip_aggregation=" << skip_aggregation
       << " version=" << version.first << "-" << version.second << " range=" << range << " end_range=" << end_range;

    for (auto& key : start_key) {
        ss << " keys=" << key;
    }

    for (auto& key : end_key) {
        ss << " end_keys=" << key;
    }
    return ss.str();
}

} // namespace starrocks::vectorized
