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

#include "common/status.h"

namespace starrocks {
using namespace config;

class ConfigTest : public testing::Test {
    void SetUp() override { config::Register::_s_field_map->clear(); }
};

TEST_F(ConfigTest, DumpAllConfigs) {
    CONF_Bool(cfg_bool_false, "false");
    CONF_Bool(cfg_bool_true, "true");
    CONF_Double(cfg_double, "123.456");
<<<<<<< HEAD
    CONF_Int16(cfg_int16_t, "2561");
    CONF_Int32(cfg_int32_t, "65536123");
    CONF_Int64(cfg_int64_t, "4294967296123");
    CONF_String(cfg_std_string, "starrocks_config_test_string");
    CONF_Bools(cfg_std_vector_bool, "true,false,true");
    CONF_Doubles(cfg_std_vector_double, "123.456,123.4567,123.45678");
    CONF_Int16s(cfg_std_vector_int16_t, "2561,2562,2563");
    CONF_Int32s(cfg_std_vector_int32_t, "65536123,65536234,65536345");
    CONF_Int64s(cfg_std_vector_int64_t, "4294967296123,4294967296234,4294967296345");
    CONF_Strings(cfg_std_vector_std_string, "starrocks,config,test,string");
=======
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
    CONF_String(cfg_string_env, "prefix/${ConfigTestEnv1}/suffix");
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
>>>>>>> 75f45f8f5f ([Feature] add be config brpc_connection_type (#42824))

    config::init(nullptr, true);
    std::stringstream ss;
    for (const auto& it : *(config::full_conf_map)) {
        ss << it.first << "=" << it.second << std::endl;
    }
<<<<<<< HEAD
    ASSERT_EQ(
            "cfg_bool_false=0\ncfg_bool_true=1\ncfg_double=123.456\ncfg_int16_t=2561\ncfg_int32_t="
            "65536123\ncfg_int64_t=4294967296123\ncfg_std_string=starrocks_config_test_string\ncfg_std_"
            "vector_bool=1, 0, 1\ncfg_std_vector_double=123.456, 123.457, "
            "123.457\ncfg_std_vector_int16_t=2561, 2562, 2563\ncfg_std_vector_int32_t=65536123, "
            "65536234, 65536345\ncfg_std_vector_int64_t=4294967296123, 4294967296234, "
            "4294967296345\ncfg_std_vector_std_string=starrocks, config, test, string\n",
            ss.str());
=======
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
>>>>>>> 75f45f8f5f ([Feature] add be config brpc_connection_type (#42824))
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

    config::init(nullptr, true);

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
    ASSERT_EQ(cfg_std_string_mutable, "starrocks_config_test_string_mutable");
    ASSERT_TRUE(config::set_config("cfg_std_string_mutable", "hello SR").ok());
    ASSERT_EQ(cfg_std_string_mutable, "hello SR");

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

} // namespace starrocks
