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

#include <gmock/gmock.h> // EXPECT_THAT, ElementsAre
#include <gtest/gtest.h>

#include <iostream>
#include <sstream>
#include <streambuf>
#include <thread>

#include "common/status.h"
#include "gutil/strings/join.h"

namespace starrocks {
using namespace config;

using namespace ::testing;

namespace {

class ostream_redirect {
public:
    ostream_redirect(std::ostream& os, std::streambuf* buf) : _os(os), _buf(os.rdbuf(buf)) {}
    ~ostream_redirect() { _os.rdbuf(_buf); }

private:
    std::ostream& _os;
    std::streambuf* _buf;
};

} // namespace

class ConfigTest : public testing::Test {
    void SetUp() override { config::TEST_clear_configs(); }
};

TEST_F(ConfigTest, test_init) {
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
    CONF_Bool(cfg_bool_env, "false");
    CONF_String_enum(cfg_string_enum, "true", "true,false");
    // Invalid config file name
    { EXPECT_FALSE(config::init("/path/to/nonexist/file")); }
    // Invalid bool value
    {
        std::stringstream ss;
        ss << R"DEL(
           cfg_bool = t
           )DEL";

        EXPECT_FALSE(config::init(ss));
    }
    // Invalid numeric value
    {
        std::stringstream ss;
        ss << R"DEL(
           cfg_int32 = 0xAB
           )DEL";

        EXPECT_FALSE(config::init(ss));
    }

    // Invalid env
    {
        std::stringstream ss;
        ss << R"DEL(
           cfg_string = ${xxxx}
           )DEL";

        EXPECT_FALSE(config::init(ss));
    }
    // Invalid enum value
    {
        std::stringstream ss;
        ss << R"DEL(
           cfg_string_enum = unknown
           )DEL";
        EXPECT_FALSE(config::init(ss));
    }

    // ignore env var config
    {
        std::stringstream ss;
        ss << R"DEL(
           JAVA_OPTS = -Xmx4g
           )DEL";
        std::stringstream stringbuf;
        ostream_redirect cerrbuf(std::cerr, stringbuf.rdbuf());
        EXPECT_TRUE(config::init(ss));
        std::string err_text = stringbuf.str();
        // no error message prompted
        EXPECT_TRUE(err_text.empty()) << "ErrorText: " << err_text;
    }

    // contains the eror message about the unknown configvar `java_opts`
    {
        std::stringstream ss;
        ss << R"DEL(
           java_opts = -Xmx4g
           )DEL";
        std::stringstream stringbuf;
        ostream_redirect cerrbuf(std::cerr, stringbuf.rdbuf());
        EXPECT_TRUE(config::init(ss));
        std::string err_text = stringbuf.str();
        // error prompted for Unknown config `java_opts`
        EXPECT_NE(std::string::npos, err_text.find("java_opts"));
    }

    // Move the definition here so that it won't fail other tests due to non-existence of ${ConfigTestEnv1}
    CONF_String(cfg_string_env, "prefix/${ConfigTestEnv1}/suffix");
    // Valid input
    {
        std::stringstream ss;
        ss << R"DEL(
           #comment
           cfg_bool = true
           cfg_mbool = false
           
           cfg_double = 10.0

           # cfg_int16 = 12
           cfg_mint16 = 12

           cfg_string = test string
           
           cfg_mstring = key =value 
           
           cfg_int32s = 123, 456, 789 
      
           cfg_strings = text1, hello world , StarRocks
           
           cfg_bool_env = ${ConfigTestEnv2}

           cfg_string_enum = false
           )DEL";

        ASSERT_EQ(0, ::setenv("ConfigTestEnv1", "env1_value", 1));
        ASSERT_EQ(0, ::setenv("ConfigTestEnv2", " true", 1));

        EXPECT_TRUE(config::init(ss));
    }

    EXPECT_EQ(true, cfg_bool);
    EXPECT_EQ(false, cfg_mbool);
    EXPECT_EQ(10, cfg_double);
    EXPECT_EQ(-123.456, cfg_mdouble);
    EXPECT_EQ(2561, cfg_int16);
    EXPECT_EQ(12, cfg_mint16);
    EXPECT_EQ(4294967296123, cfg_int64);
    EXPECT_EQ(-4294967296123, cfg_mint64);
    EXPECT_EQ("test string", cfg_string);
    EXPECT_EQ("key =value", cfg_mstring.value());
    EXPECT_THAT(cfg_bools, ElementsAre(true, false, true));
    EXPECT_THAT(cfg_doubles, ElementsAre(0.1, 0.2, 0.3));
    EXPECT_THAT(cfg_int16s, ElementsAre(1, 2, 3));
    EXPECT_THAT(cfg_int32s, ElementsAre(123, 456, 789));
    EXPECT_THAT(cfg_int64s, ElementsAre(100, 200, 300));
    EXPECT_THAT(cfg_strings, ElementsAre("text1", "hello world", "StarRocks"));
    EXPECT_EQ("prefix/env1_value/suffix", cfg_string_env);
    EXPECT_EQ(true, cfg_bool_env);
    EXPECT_EQ("false", cfg_string_enum);
}

TEST_F(ConfigTest, test_invalid_default_value) {
    CONF_Int32(cfg_int32, "false");
    ASSERT_FALSE(config::init(nullptr));
}

TEST_F(ConfigTest, test_unknown_config) {
    CONF_Int32(cfg_int32, "10");
    std::stringstream ss;
    ss << "x = y\n";
    ASSERT_TRUE(config::init(ss));
    EXPECT_EQ(10, cfg_int32);
}

TEST_F(ConfigTest, test_empty_string_value) {
    CONF_String(cfg_string, "10");
    std::stringstream ss;
    ss << "cfg_string=\n";
    ASSERT_TRUE(config::init(ss));
    EXPECT_EQ("", cfg_string);
}

TEST_F(ConfigTest, test_no_string_value) {
    CONF_String(cfg_string, "10");
    std::stringstream ss;
    ss << "cfg_string\n";
    ASSERT_TRUE(config::init(ss));
    EXPECT_EQ("", cfg_string);
}

TEST_F(ConfigTest, test_duplicate_assignment) {
    CONF_Int32(cfg_int32, "10");
    std::stringstream ss;
    ss << R"(cfg_int32 = 1
             cfg_int32 = 2
            )";
    ASSERT_TRUE(config::init(ss));
    EXPECT_EQ(2, cfg_int32);
}

TEST_F(ConfigTest, test_duplicate_alias_assignment) {
    CONF_Int32(cfg_int32, "10");
    CONF_Alias(cfg_int32, cfg_int32_alias);
    std::stringstream ss;
    ss << R"(cfg_int32 = 1
             cfg_int32_alias = 2
            )";
    ASSERT_TRUE(config::init(ss));
    EXPECT_EQ(2, cfg_int32);
}

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

    ASSERT_TRUE(config::init(nullptr));

    std::vector<ConfigInfo> exp_configs = {
            // name,value,type,default,mutable
            {"cfg_bool", "false", "bool", "false", false},
            {"cfg_mbool", "true", "bool", "true", true},
            {"cfg_double", "123.456", "double", "123.456", false},
            {"cfg_mdouble", "-123.456", "double", "-123.456", true},
            {"cfg_int16", "2561", "int16", "2561", false},
            {"cfg_mint16", "-2561", "int16", "-2561", true},
            {"cfg_int32", "65536123", "int32", "65536123", false},
            {"cfg_mint32", "-65536123", "int32", "-65536123", true},
            {"cfg_int64", "4294967296123", "int64", "4294967296123", false},
            {"cfg_mint64", "-4294967296123", "int64", "-4294967296123", true},
            {"cfg_string", "test_string", "string", "test_string", false},
            {"cfg_mstring", "test_mstring", "string", "test_mstring", true},
            {"cfg_bools", "true,false,true", "list<bool>", "true,false,true", false},
            {"cfg_doubles", "0.1,0.2,0.3", "list<double>", "0.1,0.2,0.3", false},
            {"cfg_int16s", "1,2,3", "list<int16>", "1,2,3", false},
            {"cfg_int32s", "10,20,30", "list<int32>", "10,20,30", false},
            {"cfg_int64s", "100,200,300", "list<int64>", "100,200,300", false},
            {"cfg_strings", "s1,s2,s3", "list<string>", "s1,s2,s3", false},
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

TEST_F(ConfigTest, test_empty_list) {
    CONF_Strings(cfg_strings, "");
    CONF_Int32s(cfg_int32s, "");
    ASSERT_TRUE(config::init(nullptr));
    EXPECT_EQ(0, cfg_strings.size());
    EXPECT_EQ(0, cfg_int32s.size());
}

TEST_F(ConfigTest, test_list_with_spaces) {
    CONF_Strings(cfg_strings, "");
    CONF_Int32s(cfg_int32s, "");
    std::stringstream ss;
    ss << R"(cfg_strings=,s1,,s2, s3,s4 
             cfg_int32s=,10,, 12, 14 
          )";
    ASSERT_TRUE(config::init(ss));
    EXPECT_THAT(cfg_strings, ElementsAre("s1", "s2", "s3", "s4"));
    EXPECT_THAT(cfg_int32s, ElementsAre(10, 12, 14));
}

TEST_F(ConfigTest, test_set_config) {
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
    ASSERT_TRUE(config::rollback_config("cfg_bool").ok());
    ASSERT_FALSE(cfg_bool);
    ASSERT_TRUE(config::set_config("cfg_bool", "true").ok());
    ASSERT_TRUE(cfg_bool);

    // double
    ASSERT_EQ(cfg_double, 123.456);
    ASSERT_TRUE(config::set_config("cfg_double", "654.321").ok());
    ASSERT_EQ(cfg_double, 654.321);
    ASSERT_TRUE(config::rollback_config("cfg_double").ok());
    ASSERT_EQ(cfg_double, 123.456);
    ASSERT_TRUE(config::set_config("cfg_double", "654.321").ok());
    ASSERT_EQ(cfg_double, 654.321);

    // int16
    ASSERT_EQ(cfg_int16_t, 2561);
    ASSERT_TRUE(config::set_config("cfg_int16_t", "2562").ok());
    ASSERT_EQ(cfg_int16_t, 2562);
    ASSERT_TRUE(config::rollback_config("cfg_int16_t").ok());
    ASSERT_EQ(cfg_int16_t, 2561);

    // int32
    ASSERT_EQ(cfg_int32_t, 65536123);
    ASSERT_TRUE(config::set_config("cfg_int32_t", "65536124").ok());
    ASSERT_EQ(cfg_int32_t, 65536124);
    ASSERT_TRUE(config::rollback_config("cfg_int32_t").ok());
    ASSERT_EQ(cfg_int32_t, 65536123);
    ASSERT_TRUE(config::set_config("cfg_int32_t", "65536124").ok());
    ASSERT_EQ(cfg_int32_t, 65536124);

    // int64
    ASSERT_EQ(cfg_int64_t, 4294967296123);
    ASSERT_TRUE(config::set_config("cfg_int64_t", "4294967296124").ok());
    ASSERT_EQ(cfg_int64_t, 4294967296124);
    ASSERT_TRUE(config::rollback_config("cfg_int64_t").ok());
    ASSERT_EQ(cfg_int64_t, 4294967296123);

    // string
    ASSERT_EQ(cfg_std_string_mutable.value(), "starrocks_config_test_string_mutable");
    ASSERT_TRUE(config::set_config("cfg_std_string_mutable", "hello SR").ok());
    ASSERT_EQ(cfg_std_string_mutable.value(), "hello SR");
    ASSERT_TRUE(config::rollback_config("cfg_std_string_mutable").ok());
    ASSERT_EQ(cfg_std_string_mutable.value(), "starrocks_config_test_string_mutable");

    // not exist
    Status s = config::set_config("cfg_not_exist", "123");
    ASSERT_TRUE(s.is_not_found()) << s;

    // immutable
    ASSERT_TRUE(cfg_bool_immutable);
    s = config::set_config("cfg_bool_immutable", "false");
    ASSERT_TRUE(s.is_not_supported()) << s;
    ASSERT_TRUE(cfg_bool_immutable);

    // convert error
    s = config::set_config("cfg_bool", "falseeee");
    ASSERT_TRUE(s.is_invalid_argument()) << s;
    ASSERT_TRUE(cfg_bool);

    s = config::set_config("cfg_double", "");
    ASSERT_TRUE(s.is_invalid_argument()) << s;
    ASSERT_EQ(cfg_double, 654.321);

    // convert error
    s = config::set_config("cfg_int32_t", "4294967296124");
    ASSERT_TRUE(s.is_invalid_argument()) << s;
    ASSERT_EQ(cfg_int32_t, 65536124);

    // not support
    s = config::set_config("cfg_std_string", "test");
    ASSERT_TRUE(s.is_not_supported()) << s;
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

TEST_F(ConfigTest, test_alias01) {
    CONF_mInt16(cfg_int32, "8000");
    CONF_Alias(cfg_int32, cfg_int32_alias);

    std::stringstream ss;
    ss << R"DEL(
        cfg_int32_alias = 8080
       )DEL";

    EXPECT_TRUE(config::init(ss));
    EXPECT_EQ(8080, cfg_int32);
}

TEST_F(ConfigTest, test_alias02) {
    CONF_mInt16(cfg_int32, "8000");
    CONF_Alias(cfg_int32, cfg_int32_alias);

    std::stringstream ss;
    ss << R"DEL(
        cfg_int32_alias = 8080
        cfg_int32 = 8001
       )DEL";

    EXPECT_TRUE(config::init(ss));
    EXPECT_EQ(8001, cfg_int32);
}

TEST_F(ConfigTest, test_alias03) {
    CONF_mInt16(cfg_int32, "8000");
    CONF_Alias(cfg_int32, cfg_int32_alias1);
    CONF_Alias(cfg_int32, cfg_int32_alias2);

    std::stringstream ss;
    ss << R"DEL(
        cfg_int32_alias1 = 8080
        cfg_int32_alias2 = 8090
       )DEL";

    EXPECT_TRUE(config::init(ss));
    EXPECT_EQ(8090, cfg_int32);
}

TEST_F(ConfigTest, test_alias04) {
    CONF_mInt16(cfg_int32, "8000");
    CONF_Alias(cfg_int32, cfg_int32_alias1);
    CONF_Alias(cfg_int32, cfg_int32_alias2);

    // Different assgnment order from test_alias03
    std::stringstream ss;
    ss << R"DEL(
        cfg_int32_alias2 = 8090
        cfg_int32_alias1 = 8080
       )DEL";

    EXPECT_TRUE(config::init(ss));
    EXPECT_EQ(8080, cfg_int32);
}

} // namespace starrocks
