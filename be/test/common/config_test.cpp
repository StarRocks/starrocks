// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#define __IN_CONFIGBASE_CPP__
#include "common/configbase.h"
#undef __IN_CONFIGBASE_CPP__

#include <gtest/gtest.h>

#include <thread>

#include "common/status.h"
#include "gutil/strings/join.h"

namespace starrocks {
using namespace config;

class ConfigTest : public testing::Test {
    void SetUp() override { config::Register::_s_field_map->clear(); }
};

TEST_F(ConfigTest, test_list_configs) {
    CONF_Bool(cfg_bool, "false");
    CONF_mBool(cfg_mbool, "true");
    CONF_Double(cfg_double, "123.456");
    CONF_mDouble(cfg_mdouble, "-123.456");
    CONF_Int16(cfg_int16, "2561");
    CONF_mInt16(cfg_mint16, "-2561");
    CONF_Int32(cfg_int32, "65536123");
    CONF_mInt32(cfg_mint32, "-65536123");
    CONF_Int64(cfg_int64, "4294967296123");
    CONF_mInt64(cfg_mint64, "-4294967296123");
    CONF_String(cfg_string, "test_string");
    CONF_mString(cfg_mstring, "test_mstring");
    CONF_Bools(cfg_bools, "true,false,true");
    CONF_Doubles(cfg_doubles, "0.1,0.2,0.3");
    CONF_Int16s(cfg_int16s, "1,2,3");
    CONF_Int32s(cfg_int32s, "10,20,30");
    CONF_Int64s(cfg_int64s, "100,200,300");
    CONF_Strings(cfg_strings, "s1,s2,s3");

    config::init(nullptr);

    std::vector<ConfigInfo> exp_configs = {
            // name,value,type,default,mutable
            {"cfg_bool", "false", "bool", "false", false},
            {"cfg_mbool", "true", "bool", "true", true},
            {"cfg_double", "123.456", "double", "123.456", false},
            {"cfg_mdouble", "-123.456", "double", "-123.456", true},
            {"cfg_int16", "2561", "int16_t", "2561", false},
            {"cfg_mint16", "-2561", "int16_t", "-2561", true},
            {"cfg_int32", "65536123", "int32_t", "65536123", false},
            {"cfg_mint32", "-65536123", "int32_t", "-65536123", true},
            {"cfg_int64", "4294967296123", "int64_t", "4294967296123", false},
            {"cfg_mint64", "-4294967296123", "int64_t", "-4294967296123", true},
            {"cfg_string", "test_string", "std::string", "test_string", false},
            {"cfg_mstring", "test_mstring", "MutableString", "test_mstring", true},
            {"cfg_bools", "true,false,true", "std::vector<bool>", "true,false,true", false},
            {"cfg_doubles", "0.1,0.2,0.3", "std::vector<double>", "0.1,0.2,0.3", false},
            {"cfg_int16s", "1,2,3", "std::vector<int16_t>", "1,2,3", false},
            {"cfg_int32s", "10,20,30", "std::vector<int32_t>", "10,20,30", false},
            {"cfg_int64s", "100,200,300", "std::vector<int64_t>", "100,200,300", false},
            {"cfg_strings", "s1,s2,s3", "std::vector<std::string>", "s1,s2,s3", false},
    };

    std::vector<ConfigInfo> configs = config::list_configs();

    std::sort(exp_configs.begin(), exp_configs.end());
    std::sort(configs.begin(), configs.end());

    auto as_string = [](const ConfigInfo& info) {
        std::ostringstream ss;
        ss << info;
        return ss.str();
    };

    LOG(INFO) << "Expected configs: " << JoinMapped(exp_configs, as_string, ",")
              << " Real configs: " << JoinMapped(configs, as_string, ",");

    ASSERT_EQ(exp_configs.size(), configs.size());
    for (int i = 0, sz = configs.size(); i < sz; ++i) {
        EXPECT_EQ(exp_configs[i], configs[i]);
    }
}

TEST_F(ConfigTest, UpdateConfigs) {
    CONF_Bool(cfg_bool_immutable, "true");
    CONF_mBool(cfg_bool, "false");
    CONF_mDouble(cfg_double, "123.456");
    CONF_mInt16(cfg_int16_t, "2561");
    CONF_mInt32(cfg_int32_t, "65536123");
    CONF_mInt64(cfg_int64_t, "4294967296123");
    CONF_String(cfg_std_string, "starrocks_config_test_string");
    CONF_mString(cfg_std_string_mutable, "starrocks_config_test_string_mutable");

    config::init(nullptr);

    // bool
    ASSERT_FALSE(cfg_bool);
    ASSERT_TRUE(config::set_config("cfg_bool", "true").ok());
    ASSERT_TRUE(cfg_bool);

    // double
    ASSERT_EQ(cfg_double, 123.456);
    ASSERT_TRUE(config::set_config("cfg_double", "654.321").ok());
    ASSERT_EQ(cfg_double, 654.321);

    // int16
    ASSERT_EQ(cfg_int16_t, 2561);
    ASSERT_TRUE(config::set_config("cfg_int16_t", "2562").ok());
    ASSERT_EQ(cfg_int16_t, 2562);

    // int32
    ASSERT_EQ(cfg_int32_t, 65536123);
    ASSERT_TRUE(config::set_config("cfg_int32_t", "65536124").ok());
    ASSERT_EQ(cfg_int32_t, 65536124);

    // int64
    ASSERT_EQ(cfg_int64_t, 4294967296123);
    ASSERT_TRUE(config::set_config("cfg_int64_t", "4294967296124").ok());
    ASSERT_EQ(cfg_int64_t, 4294967296124);

    // string
    ASSERT_EQ(cfg_std_string_mutable.value(), "starrocks_config_test_string_mutable");
    ASSERT_TRUE(config::set_config("cfg_std_string_mutable", "hello SR").ok());
    ASSERT_EQ(cfg_std_string_mutable.value(), "hello SR");

    // not exist
    Status s = config::set_config("cfg_not_exist", "123");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Not found: 'cfg_not_exist' is not found");

    // immutable
    ASSERT_TRUE(cfg_bool_immutable);
    s = config::set_config("cfg_bool_immutable", "false");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Not supported: 'cfg_bool_immutable' is not support to modify");
    ASSERT_TRUE(cfg_bool_immutable);

    // convert error
    s = config::set_config("cfg_bool", "falseeee");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Invalid argument: convert 'falseeee' as bool failed");
    ASSERT_TRUE(cfg_bool);

    s = config::set_config("cfg_double", "");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Invalid argument: convert '' as double failed");
    ASSERT_EQ(cfg_double, 654.321);

    // convert error
    s = config::set_config("cfg_int32_t", "4294967296124");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Invalid argument: convert '4294967296124' as int32_t failed");
    ASSERT_EQ(cfg_int32_t, 65536124);

    // not support
    s = config::set_config("cfg_std_string", "test");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Not supported: 'cfg_std_string' is not support to modify");
    ASSERT_EQ(cfg_std_string, "starrocks_config_test_string");
}

TEST_F(ConfigTest, test_read_write_mutable_string_concurrently) {
    CONF_mString(config_test_mstring, "default");

    ASSERT_TRUE(config::init(nullptr));

    EXPECT_EQ("default", config_test_mstring.value());

    std::vector<std::thread> threads;
    threads.reserve(5);
    for (int i = 0; i < 5; i++) {
        threads.emplace_back([&, id = i]() {
            if (id < 2) { // writer
                for (int j = 0; j < 200; j++) {
                    auto st = set_config("config_test_mstring", std::to_string(id));
                    ASSERT_TRUE(st.ok()) << st;
                }
            } else { // reader
                auto prev_val = config_test_mstring.value();
                for (int j = 0; j < 1000; j++) {
                    std::string val = config_test_mstring.value();
                    if (val != "default" && val != "0" && val != "1") {
                        GTEST_FAIL() << val;
                    } else if (val != prev_val) {
                        LOG(INFO) << "config value changed to " << val;
                        prev_val = std::move(val);
                    }
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}

} // namespace starrocks
