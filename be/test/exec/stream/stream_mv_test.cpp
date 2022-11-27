// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <gtest/gtest.h>

#include "common/config.h"
#include "util/logging.h"

namespace starrocks::stream {} // namespace starrocks::stream

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }
    // Load config from config file.
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be.conf";
    if (!starrocks::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    std::cout << "load be conf:" << conffile << std::endl;

    starrocks::init_glog("be_test", true);

    int r = RUN_ALL_TESTS();

    starrocks::shutdown_logging();

    return r;
}
