// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/delete_handler_test.cpp

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

#include "storage/delete_handler.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "runtime/exec_env.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/push_handler.h"
#include "storage/storage_engine.h"
#include "util/file_utils.h"
#include "util/logging.h"
#include "util/mem_info.h"

using namespace std;
using namespace starrocks;
using google::protobuf::RepeatedPtrField;

namespace starrocks {

static StorageEngine* k_engine = nullptr;
static MemTracker* k_tablet_meta_mem_tracker = nullptr;
static MemTracker* k_schema_change_mem_tracker = nullptr;

void set_up() {
    ExecEnv::GetInstance()->init_mem_tracker();
    config::storage_root_path = std::filesystem::current_path().string() + "/data_test";
    FileUtils::remove_all(config::storage_root_path);
    FileUtils::remove_all(string(getenv("STARROCKS_HOME")) + UNUSED_PREFIX);
    FileUtils::create_dir(config::storage_root_path);
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path);
    config::min_file_descriptor_number = 1000;
    config::tablet_map_shard_size = 1;
    config::txn_map_shard_size = 1;
    config::txn_shard_size = 1;
    config::storage_format_version = 2;

    k_tablet_meta_mem_tracker = new MemTracker();
    k_schema_change_mem_tracker = new MemTracker();
    starrocks::EngineOptions options;
    options.store_paths = paths;
    options.tablet_meta_mem_tracker = k_tablet_meta_mem_tracker;
    options.schema_change_mem_tracker = k_schema_change_mem_tracker;
    Status s = starrocks::StorageEngine::open(options, &k_engine);
    ASSERT_TRUE(s.ok()) << s.to_string();
}

void tear_down() {
    config::storage_root_path = std::filesystem::current_path().string() + "/data_test";
    FileUtils::remove_all(config::storage_root_path);
    FileUtils::remove_all(string(getenv("STARROCKS_HOME")) + UNUSED_PREFIX);
    k_tablet_meta_mem_tracker->release(k_tablet_meta_mem_tracker->consumption());
    k_schema_change_mem_tracker->release(k_schema_change_mem_tracker->consumption());
    delete k_tablet_meta_mem_tracker;
    delete k_schema_change_mem_tracker;
}

void set_default_create_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = random();
    request->__set_version(1);
    request->__set_version_hash(0);
    request->tablet_schema.schema_hash = 270068375;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    k4.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    k5.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k5);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::DECIMAL;
    k9.column_type.__set_precision(6);
    k9.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    k10.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    k11.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k11);

    TColumn k12;
    k12.column_name = "k12";
    k12.__set_is_key(true);
    k12.column_type.__set_len(64);
    k12.column_type.type = TPrimitiveType::CHAR;
    request->tablet_schema.columns.push_back(k12);

    TColumn k13;
    k13.column_name = "k13";
    k13.__set_is_key(true);
    k13.column_type.__set_len(64);
    k13.column_type.type = TPrimitiveType::VARCHAR;
    request->tablet_schema.columns.push_back(k13);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    v.column_type.type = TPrimitiveType::BIGINT;
    v.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v);
}

void set_create_duplicate_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = random();
    request->__set_version(1);
    request->__set_version_hash(0);
    request->tablet_schema.schema_hash = 270068376;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::DUP_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    k4.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    k5.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k5);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::DECIMAL;
    k9.column_type.__set_precision(6);
    k9.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    k10.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    k11.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k11);

    TColumn k12;
    k12.column_name = "k12";
    k12.__set_is_key(true);
    k12.column_type.__set_len(64);
    k12.column_type.type = TPrimitiveType::CHAR;
    request->tablet_schema.columns.push_back(k12);

    TColumn k13;
    k13.column_name = "k13";
    k13.__set_is_key(true);
    k13.column_type.__set_len(64);
    k13.column_type.type = TPrimitiveType::VARCHAR;
    request->tablet_schema.columns.push_back(k13);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    v.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(v);
}

class TestDeleteConditionHandler : public testing::Test {
protected:
    void SetUp() {
        config::storage_root_path = std::filesystem::current_path().string() + "/data_delete_condition";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        set_default_create_tablet_request(&_create_tablet);
        auto res = k_engine->create_tablet(_create_tablet);
        ASSERT_TRUE(res.ok()) << res.to_string();
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(tablet.get() != nullptr);
        _tablet_path = tablet->tablet_path();

        set_create_duplicate_tablet_request(&_create_dup_tablet);
        res = k_engine->create_tablet(_create_dup_tablet);
        ASSERT_TRUE(res.ok()) << res.to_string();
        dup_tablet = k_engine->tablet_manager()->get_tablet(_create_dup_tablet.tablet_id);
        ASSERT_TRUE(dup_tablet.get() != nullptr);
        _dup_tablet_path = tablet->tablet_path();
    }

    void TearDown() {
        tablet.reset();
        dup_tablet.reset();
        (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
    }

    std::string _tablet_path;
    std::string _dup_tablet_path;
    TabletSharedPtr tablet;
    TabletSharedPtr dup_tablet;
    TCreateTabletReq _create_tablet;
    TCreateTabletReq _create_dup_tablet;
    DeleteConditionHandler _delete_condition_handler;
};

TEST_F(TestDeleteConditionHandler, StoreCondSucceed) {
    OLAPStatus success_res;
    std::vector<TCondition> conditions;

    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = ">";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("3");
    conditions.push_back(condition);

    condition.column_name = "k3";
    condition.condition_op = "<=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("5");
    conditions.push_back(condition);

    condition.column_name = "k4";
    condition.condition_op = "IS";
    condition.condition_values.clear();
    condition.condition_values.push_back("NULL");
    conditions.push_back(condition);

    condition.column_name = "k5";
    condition.condition_op = "*=";
    condition.condition_values.clear();
    condition.condition_values.push_back("7");
    conditions.push_back(condition);

    condition.column_name = "k12";
    condition.condition_op = "!*=";
    condition.condition_values.clear();
    condition.condition_values.push_back("9");
    conditions.push_back(condition);

    condition.column_name = "k13";
    condition.condition_op = "*=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    success_res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_SUCCESS, success_res);

    // Verify that the filter criteria stored in the header are correct
    ASSERT_EQ(size_t(6), del_pred.sub_predicates_size());
    EXPECT_STREQ("k1=1", del_pred.sub_predicates(0).c_str());
    EXPECT_STREQ("k2>>3", del_pred.sub_predicates(1).c_str());
    EXPECT_STREQ("k3<=5", del_pred.sub_predicates(2).c_str());
    EXPECT_STREQ("k4 IS NULL", del_pred.sub_predicates(3).c_str());
    EXPECT_STREQ("k5=7", del_pred.sub_predicates(4).c_str());
    EXPECT_STREQ("k12!=9", del_pred.sub_predicates(5).c_str());

    ASSERT_EQ(size_t(1), del_pred.in_predicates_size());
    ASSERT_FALSE(del_pred.in_predicates(0).is_not_in());
    EXPECT_STREQ("k13", del_pred.in_predicates(0).column_name().c_str());
    ASSERT_EQ(std::size_t(2), del_pred.in_predicates(0).values().size());
}

// empty string
TEST_F(TestDeleteConditionHandler, StoreCondInvalidParameters) {
    std::vector<TCondition> conditions;
    DeletePredicatePB del_pred;
    OLAPStatus failed_res =
            _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_ERR_DELETE_INVALID_PARAMETERS, failed_res);
}

TEST_F(TestDeleteConditionHandler, StoreCondNonexistentColumn) {
    // 'k100' is an invalid column
    std::vector<TCondition> conditions;
    TCondition condition;
    condition.column_name = "k100";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("2");
    conditions.push_back(condition);
    DeletePredicatePB del_pred;
    OLAPStatus failed_res =
            _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, failed_res);

    // 'v' is a value column
    conditions.clear();
    condition.column_name = "v";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("5");
    conditions.push_back(condition);

    failed_res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, failed_res);

    // value column in duplicate model can be deleted;
    conditions.clear();
    condition.column_name = "v";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("5");
    conditions.push_back(condition);

    OLAPStatus success_res =
            _delete_condition_handler.generate_delete_predicate(dup_tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_SUCCESS, success_res);
}

// delete condition does not match
class TestDeleteConditionHandler2 : public testing::Test {
protected:
    void SetUp() {
        config::storage_root_path = std::filesystem::current_path().string() + "/data_delete_condition";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        set_default_create_tablet_request(&_create_tablet);
        auto res = k_engine->create_tablet(_create_tablet);
        ASSERT_TRUE(res.ok()) << res.to_string();
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(tablet.get() != nullptr);
        _tablet_path = tablet->tablet_path();
    }

    void TearDown() {
        tablet.reset();
        (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
    }

    std::string _tablet_path;
    TabletSharedPtr tablet;
    TCreateTabletReq _create_tablet;
    DeleteConditionHandler _delete_condition_handler;
};

TEST_F(TestDeleteConditionHandler2, ValidConditionValue) {
    OLAPStatus res;
    std::vector<TCondition> conditions;

    // k1,k2,k3,k4 type is int8, int16, int32, int64
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("-1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("-1");
    conditions.push_back(condition);

    condition.column_name = "k3";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("-1");
    conditions.push_back(condition);

    condition.column_name = "k4";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("-1");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // k5 type is int128
    conditions.clear();
    condition.column_name = "k5";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_2);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // k9 type is decimal, precision=6, frac=3
    conditions.clear();
    condition.column_name = "k9";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("2.3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_3;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_3);
    ASSERT_EQ(OLAP_SUCCESS, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2");
    DeletePredicatePB del_pred_4;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_4);
    ASSERT_EQ(OLAP_SUCCESS, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-2");
    DeletePredicatePB del_pred_5;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_5);
    ASSERT_EQ(OLAP_SUCCESS, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-2.3");
    DeletePredicatePB del_pred_6;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_6);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // k10,k11 type is date, datetime
    conditions.clear();
    condition.column_name = "k10";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("2014-01-01");
    conditions.push_back(condition);

    condition.column_name = "k10";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("2014-01-01 00:00:00");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_7;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_7);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // k12,k13 type is string(64), varchar(64)
    conditions.clear();
    condition.column_name = "k12";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("YWFh");
    conditions.push_back(condition);

    condition.column_name = "k13";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("YWFhYQ==");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_8;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_8);
    ASSERT_EQ(OLAP_SUCCESS, res);
}

TEST_F(TestDeleteConditionHandler2, InvalidConditionValue) {
    OLAPStatus res;
    std::vector<TCondition> conditions;

    // Test k1 max, k1 type is int8
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1000");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_1;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_1);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // test k1 min, k1 type is int8
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-1000");
    DeletePredicatePB del_pred_2;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_2);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k2(int16) max value
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k2";
    conditions[0].condition_values.emplace_back("32768");
    DeletePredicatePB del_pred_3;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_3);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k2(int16) min value
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-32769");
    DeletePredicatePB del_pred_4;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_4);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k3(int32) max
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k3";
    conditions[0].condition_values.emplace_back("2147483648");
    DeletePredicatePB del_pred_5;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_5);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k3(int32) min value
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-2147483649");
    DeletePredicatePB del_pred_6;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_6);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k4(int64) max
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k4";
    conditions[0].condition_values.emplace_back("9223372036854775808");
    DeletePredicatePB del_pred_7;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_7);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k4(int64) min
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-9223372036854775809");
    DeletePredicatePB del_pred_8;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_8);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k5(int128) max
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k5";
    conditions[0].condition_values.emplace_back("170141183460469231731687303715884105728");
    DeletePredicatePB del_pred_9;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_9);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k5(int128) min
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-170141183460469231731687303715884105729");
    DeletePredicatePB del_pred_10;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_10);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k9 integer overflow, type is decimal, precision=6, frac=3
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k9";
    conditions[0].condition_values.emplace_back("12347876.5");
    DeletePredicatePB del_pred_11;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_11);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k9 scale overflow, type is decimal, precision=6, frac=3
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("1.2345678");
    DeletePredicatePB del_pred_12;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_12);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // k9 has point
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("1.");
    DeletePredicatePB del_pred_13;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_13);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // invalid date
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k10";
    conditions[0].condition_values.emplace_back("20130101");
    DeletePredicatePB del_pred_14;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_14);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-64-01");
    DeletePredicatePB del_pred_15;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_15);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-40");
    DeletePredicatePB del_pred_16;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_16);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // invalid datetime
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k11";
    conditions[0].condition_values.emplace_back("20130101 00:00:00");
    DeletePredicatePB del_pred_17;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_17);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-64-01 00:00:00");
    DeletePredicatePB del_pred_18;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_18);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-40 00:00:00");
    DeletePredicatePB del_pred_19;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_19);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-01 24:00:00");
    DeletePredicatePB del_pred_20;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_20);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-01 00:60:00");
    DeletePredicatePB del_pred_21;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_21);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-01 00:00:60");
    DeletePredicatePB del_pred_22;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_22);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    // too long varchar
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k12";
    conditions[0].condition_values.emplace_back(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWE=;k13=YWFhYQ==");
    DeletePredicatePB del_pred_23;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_23);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);

    conditions[0].condition_values.clear();
    conditions[0].column_name = "k13";
    conditions[0].condition_values.emplace_back(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWE=;k13=YWFhYQ==");
    DeletePredicatePB del_pred_24;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_24);
    EXPECT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);
}

class TestDeleteHandler : public testing::Test {
protected:
    void SetUp() {
        config::storage_root_path = std::filesystem::current_path().string() + "/data_delete_condition";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        set_default_create_tablet_request(&_create_tablet);
        auto res = k_engine->create_tablet(_create_tablet);
        ASSERT_TRUE(res.ok()) << res.to_string();
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(tablet != nullptr);
        _tablet_path = tablet->tablet_path();

        _data_row_cursor.init(tablet->tablet_schema());
        _data_row_cursor.allocate_memory_for_string_type(tablet->tablet_schema());
    }

    void TearDown() {
        tablet.reset();
        _delete_handler.finalize();
        (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
    }

    std::string _tablet_path;
    RowCursor _data_row_cursor;
    TabletSharedPtr tablet;
    TCreateTabletReq _create_tablet;
    DeleteHandler _delete_handler;
    DeleteConditionHandler _delete_condition_handler;
};

TEST_F(TestDeleteHandler, InitSuccess) {
    OLAPStatus res;
    std::vector<TCondition> conditions;

    // add delete condition in FileHeader
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = ">";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("3");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "<=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("5");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred, 1);

    conditions.clear();
    condition.column_name = "k1";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_2);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred_2, 2);

    conditions.clear();
    condition.column_name = "k2";
    condition.condition_op = ">=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_3;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_3);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred_3, 3);

    conditions.clear();
    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_4;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_4);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred_4, 4);

    // Get delete condition whose version less than 7 form header
    res = _delete_handler.init(tablet->tablet_schema(), tablet->delete_predicates(), 4);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(4, _delete_handler.conditions_num());
    std::vector<int32_t> conds_version = _delete_handler.get_conds_version();
    EXPECT_EQ(4, conds_version.size());
    sort(conds_version.begin(), conds_version.end());
    EXPECT_EQ(1, conds_version[0]);
    EXPECT_EQ(2, conds_version[1]);
    EXPECT_EQ(3, conds_version[2]);
    EXPECT_EQ(4, conds_version[3]);

    _delete_handler.finalize();
}

// Test that a filter condition contains subconditions that are and related to each other
// That is, the data will be filtered only if all the subconditions contained in a filter condition are satisfied
TEST_F(TestDeleteHandler, FilterDataSubconditions) {
    OLAPStatus res;
    std::vector<TCondition> conditions;

    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("4");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred, 1);

    // Specify version number 10 to load all filters in the Header (in this case, only filter 1)
    res = _delete_handler.init(tablet->tablet_schema(), tablet->delete_predicates(), 4);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, _delete_handler.conditions_num());

    std::vector<string> data_str;
    data_str.emplace_back("1");
    data_str.emplace_back("6");
    data_str.emplace_back("8");
    data_str.emplace_back("-1");
    data_str.emplace_back("16");
    data_str.emplace_back("1.2");
    data_str.emplace_back("2014-01-01");
    data_str.emplace_back("2014-01-01 00:00:00");
    data_str.emplace_back("YWFH");
    data_str.emplace_back("YWFH==");
    data_str.emplace_back("1");
    OlapTuple tuple1(data_str);
    res = _data_row_cursor.from_tuple(tuple1);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_TRUE(_delete_handler.is_filter_data(1, _data_row_cursor));

    data_str[1] = "4";
    OlapTuple tuple2(data_str);
    res = _data_row_cursor.from_tuple(tuple2);
    ASSERT_EQ(OLAP_SUCCESS, res);
    // not match: k2!=4
    ASSERT_FALSE(_delete_handler.is_filter_data(1, _data_row_cursor));

    _delete_handler.finalize();
}

// Test that multiple filter conditions are or related to each other
TEST_F(TestDeleteHandler, FilterDataConditions) {
    OLAPStatus res;
    std::vector<TCondition> conditions;

    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("4");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred, 1);

    conditions.clear();
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_2);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred_2, 2);

    conditions.clear();
    condition.column_name = "k2";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("5");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_3;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_3);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred_3, 3);

    // Specify version number 4 to load all filters in the meta (in this case, only filter 1)
    res = _delete_handler.init(tablet->tablet_schema(), tablet->delete_predicates(), 4);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(3, _delete_handler.conditions_num());

    std::vector<string> data_str;
    data_str.emplace_back("4");
    data_str.emplace_back("5");
    data_str.emplace_back("8");
    data_str.emplace_back("-1");
    data_str.emplace_back("16");
    data_str.emplace_back("1.2");
    data_str.emplace_back("2014-01-01");
    data_str.emplace_back("2014-01-01 00:00:00");
    data_str.emplace_back("YWFH");
    data_str.emplace_back("YWFH==");
    data_str.emplace_back("1");
    OlapTuple tuple(data_str);
    res = _data_row_cursor.from_tuple(tuple);
    ASSERT_EQ(OLAP_SUCCESS, res);
    // This row will be filted becuase condition 3
    ASSERT_TRUE(_delete_handler.is_filter_data(3, _data_row_cursor));

    _delete_handler.finalize();
}

// Test that filter conditions with version numbers less than the data version will not work when filtering
TEST_F(TestDeleteHandler, FilterDataVersion) {
    OLAPStatus res;
    std::vector<TCondition> conditions;

    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("4");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred, 3);

    conditions.clear();
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = _delete_condition_handler.generate_delete_predicate(tablet->tablet_schema(), conditions, &del_pred_2);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet->add_delete_predicate(del_pred_2, 4);

    // Specify version number 4 to load all filters in the meta (filter condition 1, filter condition 2)
    res = _delete_handler.init(tablet->tablet_schema(), tablet->delete_predicates(), 4);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(2, _delete_handler.conditions_num());

    std::vector<string> data_str;
    data_str.emplace_back("1");
    data_str.emplace_back("6");
    data_str.emplace_back("8");
    data_str.emplace_back("-1");
    data_str.emplace_back("16");
    data_str.emplace_back("1.2");
    data_str.emplace_back("2014-01-01");
    data_str.emplace_back("2014-01-01 00:00:00");
    data_str.emplace_back("YWFH");
    data_str.emplace_back("YWFH==");
    data_str.emplace_back("1");
    OlapTuple tuple(data_str);
    res = _data_row_cursor.from_tuple(tuple);
    ASSERT_EQ(OLAP_SUCCESS, res);
    // If the data version is less than 3, then filter condition 1 takes effect and this data is filtered
    ASSERT_TRUE(_delete_handler.is_filter_data(2, _data_row_cursor));
    // Filter condition 1 will be skipped if the data version is greater than 3
    ASSERT_FALSE(_delete_handler.is_filter_data(4, _data_row_cursor));

    _delete_handler.finalize();
}

} // namespace starrocks

int main(int argc, char** argv) {
    starrocks::init_glog("be-test");
    starrocks::MemInfo::init();
    int ret = starrocks::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    config::mem_limit = "10g";

    starrocks::set_up();
    ret = RUN_ALL_TESTS();
    starrocks::tear_down();

    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
