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

#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/logging.h"
#include "util/mem_info.h"

using namespace std;
using namespace starrocks;
using google::protobuf::RepeatedPtrField;

namespace starrocks {

static StorageEngine* k_engine = nullptr;
static MemTracker* k_metadata_mem_tracker = nullptr;
static MemTracker* k_schema_change_mem_tracker = nullptr;
static std::string k_default_storage_root_path = "";

static void set_up(const std::string& sub_path) {
    config::mem_limit = "10g";
    CHECK(GlobalEnv::GetInstance()->init().ok());
    k_default_storage_root_path = config::storage_root_path;
    config::storage_root_path = std::filesystem::current_path().string() + "/" + sub_path;
    fs::remove_all(config::storage_root_path);
    fs::remove_all(string(getenv("STARROCKS_HOME")) + UNUSED_PREFIX);
    fs::create_directories(config::storage_root_path);
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path);
    config::min_file_descriptor_number = 1000;
    config::tablet_map_shard_size = 1;
    config::txn_map_shard_size = 1;
    config::txn_shard_size = 1;

    k_metadata_mem_tracker = new MemTracker();
    k_schema_change_mem_tracker = new MemTracker();
    starrocks::EngineOptions options;
    options.store_paths = paths;
    Status s = starrocks::StorageEngine::open(options, &k_engine);
    ASSERT_TRUE(s.ok()) << s.to_string();
}

static void tear_down(const std::string& sub_path) {
    config::storage_root_path = std::filesystem::current_path().string() + "/" + sub_path;
    fs::remove_all(config::storage_root_path);
    fs::remove_all(string(getenv("STARROCKS_HOME")) + UNUSED_PREFIX);
    k_metadata_mem_tracker->release(k_metadata_mem_tracker->consumption());
    k_schema_change_mem_tracker->release(k_schema_change_mem_tracker->consumption());
    if (k_engine != nullptr) {
        k_engine->stop();
        delete k_engine;
        k_engine = nullptr;
    }
    delete k_metadata_mem_tracker;
    delete k_schema_change_mem_tracker;
    config::storage_root_path = k_default_storage_root_path;
}

void set_scalar_type(TColumn* tcolumn, TPrimitiveType::type type) {
    TScalarType scalar_type;
    scalar_type.__set_type(type);

    tcolumn->type_desc.types.resize(1);
    tcolumn->type_desc.types.back().__set_type(TTypeNodeType::SCALAR);
    tcolumn->type_desc.types.back().__set_scalar_type(scalar_type);
    tcolumn->__isset.type_desc = true;
}

void set_decimal_type(TColumn* tcolumn, TPrimitiveType::type type, int precision, int scale) {
    TScalarType scalar_type;
    scalar_type.__set_type(type);
    scalar_type.__set_precision(precision);
    scalar_type.__set_scale(scale);

    tcolumn->type_desc.types.resize(1);
    tcolumn->type_desc.types.back().__set_type(TTypeNodeType::SCALAR);
    tcolumn->type_desc.types.back().__set_scalar_type(scalar_type);
    tcolumn->__isset.type_desc = true;
}

void set_varchar_type(TColumn* tcolumn, TPrimitiveType::type type, int length) {
    TScalarType scalar_type;
    scalar_type.__set_type(type);
    scalar_type.__set_len(length);

    tcolumn->type_desc.types.resize(1);
    tcolumn->type_desc.types.back().__set_type(TTypeNodeType::SCALAR);
    tcolumn->type_desc.types.back().__set_scalar_type(scalar_type);
    tcolumn->__isset.type_desc = true;
}

void set_key_columns(TCreateTabletReq* request) {
    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    set_scalar_type(&k1, TPrimitiveType::TINYINT);
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    set_scalar_type(&k2, TPrimitiveType::SMALLINT);
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    set_scalar_type(&k3, TPrimitiveType::INT);
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    set_scalar_type(&k4, TPrimitiveType::BIGINT);
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    set_scalar_type(&k5, TPrimitiveType::LARGEINT);
    request->tablet_schema.columns.push_back(k5);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::DECIMALV2;
    k9.column_type.__set_precision(6);
    k9.column_type.__set_scale(3);
    set_decimal_type(&k9, TPrimitiveType::DECIMALV2, 6, 3);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    set_scalar_type(&k10, TPrimitiveType::DATE);
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    set_scalar_type(&k11, TPrimitiveType::DATETIME);
    request->tablet_schema.columns.push_back(k11);

    TColumn k12;
    k12.column_name = "k12";
    k12.__set_is_key(true);
    set_varchar_type(&k12, TPrimitiveType::CHAR, 64);
    request->tablet_schema.columns.push_back(k12);

    TColumn k13;
    k13.column_name = "k13";
    k13.__set_is_key(true);
    set_varchar_type(&k13, TPrimitiveType::CHAR, 64);
    request->tablet_schema.columns.push_back(k13);

    TColumn k14;
    k14.column_name = "k14";
    k14.__set_is_key(true);
    set_varchar_type(&k14, TPrimitiveType::VARCHAR, 64);
    request->tablet_schema.columns.push_back(k14);
}

void set_default_create_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = random();
    request->__set_version(1);
    request->__set_version_hash(0);
    request->tablet_schema.schema_hash = 270068375;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    set_key_columns(request);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    set_scalar_type(&v, TPrimitiveType::BIGINT);
    v.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v);
}

void set_create_duplicate_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = random();
    request->__set_version(1);
    request->tablet_schema.schema_hash = 270068376;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::DUP_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    set_key_columns(request);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    set_scalar_type(&v, TPrimitiveType::BIGINT);
    request->tablet_schema.columns.push_back(v);
}

class TestDeleteConditionHandler : public testing::Test {
public:
    static const std::string kSubPath;

    static void SetUpTestSuite() { set_up(std::string(kSubPath)); }
    static void TearDownTestSuite() { tear_down(std::string(kSubPath)); }

protected:
    void SetUp() override {
        config::storage_root_path = std::filesystem::current_path().string() + std::string("/") + kSubPath;
        fs::remove_all(config::storage_root_path);
        ASSERT_TRUE(fs::create_directories(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        set_default_create_tablet_request(&_create_tablet);
        auto res = k_engine->create_tablet(_create_tablet);
        ASSERT_TRUE(res.ok()) << res.to_string();
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(tablet.get() != nullptr);
        _schema_hash_path = tablet->schema_hash_path();

        set_create_duplicate_tablet_request(&_create_dup_tablet);
        res = k_engine->create_tablet(_create_dup_tablet);
        ASSERT_TRUE(res.ok()) << res.to_string();
        dup_tablet = k_engine->tablet_manager()->get_tablet(_create_dup_tablet.tablet_id);
        ASSERT_TRUE(dup_tablet.get() != nullptr);
        _dup_tablet_path = tablet->schema_hash_path();
    }

    void TearDown() override {
        tablet.reset();
        dup_tablet.reset();
        (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
    }

    std::string _schema_hash_path;
    std::string _dup_tablet_path;
    TabletSharedPtr tablet;
    TabletSharedPtr dup_tablet;
    TCreateTabletReq _create_tablet;
    TCreateTabletReq _create_dup_tablet;
    DeleteConditionHandler _delete_condition_handler;
};

const std::string TestDeleteConditionHandler::kSubPath = std::string("data_delete_condition");

TEST_F(TestDeleteConditionHandler, StoreCondSucceed) {
    Status success_res;
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
    condition.condition_values.emplace_back("NULL");
    conditions.push_back(condition);

    condition.column_name = "k5";
    condition.condition_op = "*=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("7");
    conditions.push_back(condition);

    condition.column_name = "k12";
    condition.condition_op = "!*=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("9");
    conditions.push_back(condition);

    condition.column_name = "k13";
    condition.condition_op = "*=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    condition.condition_values.emplace_back("3");
    conditions.push_back(condition);

    condition.column_name = "k14";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back(" a");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    success_res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                                      &del_pred);
    ASSERT_EQ(true, success_res.ok());

    // Verify that the filter criteria stored in the header are correct
    ASSERT_EQ(size_t(7), del_pred.sub_predicates_size());
    EXPECT_STREQ("k1=1", del_pred.sub_predicates(0).c_str());
    EXPECT_STREQ("k2>>3", del_pred.sub_predicates(1).c_str());
    EXPECT_STREQ("k3<=5", del_pred.sub_predicates(2).c_str());
    EXPECT_STREQ("k4 IS NULL", del_pred.sub_predicates(3).c_str());
    EXPECT_STREQ("k5=7", del_pred.sub_predicates(4).c_str());
    EXPECT_STREQ("k12!=9", del_pred.sub_predicates(5).c_str());
    EXPECT_STREQ("k14= a", del_pred.sub_predicates(6).c_str());

    ASSERT_EQ(size_t(1), del_pred.in_predicates_size());
    ASSERT_FALSE(del_pred.in_predicates(0).is_not_in());
    EXPECT_STREQ("k13", del_pred.in_predicates(0).column_name().c_str());
    ASSERT_EQ(std::size_t(2), del_pred.in_predicates(0).values().size());

    // Test parse_condition
    condition.condition_values.clear();
    ASSERT_TRUE(DeleteHandler::parse_condition(del_pred.sub_predicates(0), &condition));
    EXPECT_STREQ("k1", condition.column_name.c_str());
    EXPECT_STREQ("=", condition.condition_op.c_str());
    EXPECT_STREQ("1", condition.condition_values[0].c_str());

    condition.condition_values.clear();
    ASSERT_TRUE(DeleteHandler::parse_condition(del_pred.sub_predicates(1), &condition));
    EXPECT_STREQ("k2", condition.column_name.c_str());
    EXPECT_STREQ(">>", condition.condition_op.c_str());
    EXPECT_STREQ("3", condition.condition_values[0].c_str());

    condition.condition_values.clear();
    ASSERT_TRUE(DeleteHandler::parse_condition(del_pred.sub_predicates(2), &condition));
    EXPECT_STREQ("k3", condition.column_name.c_str());
    EXPECT_STREQ("<=", condition.condition_op.c_str());
    EXPECT_STREQ("5", condition.condition_values[0].c_str());

    condition.condition_values.clear();
    ASSERT_TRUE(DeleteHandler::parse_condition(del_pred.sub_predicates(3), &condition));
    EXPECT_STREQ("k4", condition.column_name.c_str());
    EXPECT_STREQ("IS", condition.condition_op.c_str());
    EXPECT_STREQ("NULL", condition.condition_values[0].c_str());

    condition.condition_values.clear();
    ASSERT_TRUE(DeleteHandler::parse_condition(del_pred.sub_predicates(4), &condition));
    EXPECT_STREQ("k5", condition.column_name.c_str());
    EXPECT_STREQ("=", condition.condition_op.c_str());
    EXPECT_STREQ("7", condition.condition_values[0].c_str());

    condition.condition_values.clear();
    ASSERT_TRUE(DeleteHandler::parse_condition(del_pred.sub_predicates(5), &condition));
    EXPECT_STREQ("k12", condition.column_name.c_str());
    EXPECT_STREQ("!=", condition.condition_op.c_str());
    EXPECT_STREQ("9", condition.condition_values[0].c_str());

    condition.condition_values.clear();
    ASSERT_TRUE(DeleteHandler::parse_condition(del_pred.sub_predicates(6), &condition));
    EXPECT_STREQ("k14", condition.column_name.c_str());
    EXPECT_STREQ("=", condition.condition_op.c_str());
    EXPECT_STREQ(" a", condition.condition_values[0].c_str());
}

// empty string
TEST_F(TestDeleteConditionHandler, StoreCondInvalidParameters) {
    std::vector<TCondition> conditions;
    DeletePredicatePB del_pred;
    Status failed_res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(),
                                                                            conditions, &del_pred);
    ASSERT_EQ(true, failed_res.is_invalid_argument());
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
    Status failed_res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(),
                                                                            conditions, &del_pred);
    ASSERT_TRUE(failed_res.is_invalid_argument());

    // 'v' is a value column
    conditions.clear();
    condition.column_name = "v";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("5");
    conditions.push_back(condition);

    failed_res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                                     &del_pred);
    ASSERT_TRUE(failed_res.is_invalid_argument());

    // value column in duplicate model can be deleted;
    conditions.clear();
    condition.column_name = "v";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("5");
    conditions.push_back(condition);

    Status success_res = _delete_condition_handler.generate_delete_predicate(dup_tablet->unsafe_tablet_schema_ref(),
                                                                             conditions, &del_pred);
    ASSERT_EQ(true, success_res.ok());
}

// delete condition does not match
class TestDeleteConditionHandler2 : public testing::Test {
public:
    static const std::string kSubPath;
    static void SetUpTestSuite() { set_up(std::string(kSubPath)); }
    static void TearDownTestSuite() { tear_down(std::string(kSubPath)); }

protected:
    void SetUp() override {
        config::storage_root_path = std::filesystem::current_path().string() + std::string("/") + kSubPath;
        fs::remove_all(config::storage_root_path);
        ASSERT_TRUE(fs::create_directories(config::storage_root_path).ok());

        // 1. Prepare for query split key.
        // create base tablet
        set_default_create_tablet_request(&_create_tablet);
        auto res = k_engine->create_tablet(_create_tablet);
        ASSERT_TRUE(res.ok()) << res.to_string();
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(tablet.get() != nullptr);
        _schema_hash_path = tablet->schema_hash_path();
    }

    void TearDown() override {
        tablet.reset();
        (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_create_tablet.tablet_id);
        ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
    }

    std::string _schema_hash_path;
    TabletSharedPtr tablet;
    TCreateTabletReq _create_tablet;
    DeleteConditionHandler _delete_condition_handler;
};

const std::string TestDeleteConditionHandler2::kSubPath = std::string("data_delete_condition2");

TEST_F(TestDeleteConditionHandler2, ValidConditionValue) {
    Status res;
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
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred);
    ASSERT_TRUE(res.ok());

    // k5 type is int128
    conditions.clear();
    condition.column_name = "k5";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_2);
    ASSERT_TRUE(res.ok());

    // k9 type is decimal, precision=6, frac=3
    conditions.clear();
    condition.column_name = "k9";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("2.3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_3;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_3);
    ASSERT_EQ(true, res.ok());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2");
    DeletePredicatePB del_pred_4;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_4);
    ASSERT_TRUE(res.ok());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-2");
    DeletePredicatePB del_pred_5;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_5);
    ASSERT_TRUE(res.ok());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-2.3");
    DeletePredicatePB del_pred_6;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_6);
    ASSERT_TRUE(res.ok());

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
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_7);
    ASSERT_TRUE(res.ok());

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
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_8);
    ASSERT_TRUE(res.ok());
}

TEST_F(TestDeleteConditionHandler2, InvalidConditionValue) {
    Status res;
    std::vector<TCondition> conditions;

    // Test k1 max, k1 type is int8
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.emplace_back("1000");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_1;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_1);
    ASSERT_TRUE(res.is_invalid_argument());

    // test k1 min, k1 type is int8
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-1000");
    DeletePredicatePB del_pred_2;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_2);
    ASSERT_TRUE(res.is_invalid_argument());

    // k2(int16) max value
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k2";
    conditions[0].condition_values.emplace_back("32768");
    DeletePredicatePB del_pred_3;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_3);
    ASSERT_TRUE(res.is_invalid_argument());

    // k2(int16) min value
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-32769");
    DeletePredicatePB del_pred_4;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_4);
    ASSERT_TRUE(res.is_invalid_argument());

    // k3(int32) max
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k3";
    conditions[0].condition_values.emplace_back("2147483648");
    DeletePredicatePB del_pred_5;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_5);
    ASSERT_TRUE(res.is_invalid_argument());

    // k3(int32) min value
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-2147483649");
    DeletePredicatePB del_pred_6;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_6);
    ASSERT_TRUE(res.is_invalid_argument());

    // k4(int64) max
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k4";
    conditions[0].condition_values.emplace_back("9223372036854775808");
    DeletePredicatePB del_pred_7;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_7);
    ASSERT_TRUE(res.is_invalid_argument());

    // k4(int64) min
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-9223372036854775809");
    DeletePredicatePB del_pred_8;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_8);
    ASSERT_TRUE(res.is_invalid_argument());

    // k5(int128) max
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k5";
    conditions[0].condition_values.emplace_back("170141183460469231731687303715884105728");
    DeletePredicatePB del_pred_9;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_9);
    ASSERT_TRUE(res.is_invalid_argument());

    // k5(int128) min
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("-170141183460469231731687303715884105729");
    DeletePredicatePB del_pred_10;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_10);
    ASSERT_TRUE(res.is_invalid_argument());

    // k9 integer overflow, type is decimal, precision=6, frac=3
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k9";
    conditions[0].condition_values.emplace_back("12347876.5");
    DeletePredicatePB del_pred_11;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_11);
    ASSERT_TRUE(res.is_invalid_argument());

    // k9 scale overflow, type is decimal, precision=6, frac=3
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("1.2345678");
    DeletePredicatePB del_pred_12;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_12);
    ASSERT_TRUE(res.is_invalid_argument());

    // k9 has point
    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("1.");
    DeletePredicatePB del_pred_13;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_13);
    ASSERT_TRUE(res.is_invalid_argument());

    // invalid date
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k10";
    conditions[0].condition_values.emplace_back("20130101");
    DeletePredicatePB del_pred_14;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_14);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();

    conditions[0].condition_values.emplace_back("2013-64-01");
    DeletePredicatePB del_pred_15;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_15);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-40");
    DeletePredicatePB del_pred_16;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_16);
    ASSERT_TRUE(res.is_invalid_argument());

    // invalid datetime
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k11";
    conditions[0].condition_values.emplace_back("20130101 00:00:00");
    DeletePredicatePB del_pred_17;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_17);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-64-01 00:00:00");
    DeletePredicatePB del_pred_18;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_18);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-40 00:00:00");
    DeletePredicatePB del_pred_19;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_19);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-01 24:00:00");
    DeletePredicatePB del_pred_20;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_20);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-01 00:60:00");
    DeletePredicatePB del_pred_21;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_21);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();
    conditions[0].condition_values.emplace_back("2013-01-01 00:00:60");
    DeletePredicatePB del_pred_22;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_22);
    ASSERT_TRUE(res.is_invalid_argument());

    // too long varchar
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k12";
    conditions[0].condition_values.emplace_back(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWE=;k13=YWFhYQ==");
    DeletePredicatePB del_pred_23;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_23);
    ASSERT_TRUE(res.is_invalid_argument());

    conditions[0].condition_values.clear();
    conditions[0].column_name = "k13";
    conditions[0].condition_values.emplace_back(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWE=;k13=YWFhYQ==");
    DeletePredicatePB del_pred_24;
    res = _delete_condition_handler.generate_delete_predicate(tablet->unsafe_tablet_schema_ref(), conditions,
                                                              &del_pred_24);
    ASSERT_TRUE(res.is_invalid_argument());
}

} // namespace starrocks
