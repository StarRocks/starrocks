// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "util/hdfs_util.h"

#include <hdfs/hdfs.h>

#include "util/error_util.h"

namespace starrocks {

std::string get_hdfs_err_msg() {
    std::string error_msg = get_str_err_msg();
    std::stringstream ss;
    ss << "error=" << error_msg;
    char* root_cause = hdfsGetLastExceptionRootCause();
    if (root_cause != nullptr) {
        ss << ", root_cause=" << root_cause;
    }
    return ss.str();
}

} // namespace starrocks
