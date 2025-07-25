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

#ifdef USE_STAROS
#include "service/staros_worker.h"

#include <aws/core/Aws.h>
#include <fslib/configuration.h>
#include <fslib/fslib_all_initializer.h>
#include <gtest/gtest.h>

#include <condition_variable>
#include <functional>

#include "common/config.h"
#include "common/shutdown_hook.h"

namespace starrocks {

static void add_shard_listener(std::vector<StarOSWorker::ShardId>* shardIds, int* counter, StarOSWorker::ShardId id) {
    shardIds->push_back(id);
    ++*counter;
}

static Aws::SDKOptions _s_options;

class StarOSWorkerTest : public ::testing::Test {
public:
    static void SetUpTestCase() { Aws::InitAPI(_s_options); }

    static void TearDownTestCase() {
        staros::starlet::common::ShutdownHook::shutdown();
        Aws::ShutdownAPI(_s_options);
    }
};

TEST_F(StarOSWorkerTest, test_add_listener) {
    int counter = 0;
    std::vector<StarOSWorker::ShardId> ids;

    auto worker = std::make_unique<StarOSWorker>();

    StarOSWorker::ShardInfo info;

    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    info.id = 1;
    EXPECT_TRUE(worker->add_shard(info).ok());

    // no shard registered, counter and ids will not be modified
    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    // register the counter;
    worker->register_add_shard_listener(std::bind(&add_shard_listener, &ids, &counter, std::placeholders::_1));

    info.id = 2;
    EXPECT_TRUE(worker->add_shard(info).ok());

    // shard:2 added
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
    EXPECT_EQ(2, ids[0]);

    // add it again
    EXPECT_TRUE(worker->add_shard(info).ok());
    // no change, the shard:2 is already added
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
}

TEST_F(StarOSWorkerTest, test_fs_cache) {
    staros::starlet::fslib::register_builtin_filesystems();
    staros::starlet::ShardInfo shard_info;
    shard_info.id = 1;
    auto fs_info = shard_info.path_info.mutable_fs_info();
    fs_info->set_fs_type(staros::FileStoreType::S3);
    auto s3_fs_info = fs_info->mutable_s3_fs_info();
    s3_fs_info->set_bucket("test_bucket");
    s3_fs_info->set_endpoint("test_endpoint");
    s3_fs_info->set_region("us-east-1");
    auto credential = s3_fs_info->mutable_credential();
    auto simple_credential = credential->mutable_simple_credential();
    simple_credential->set_access_key("test_ak");
    simple_credential->set_access_key_secret("test_sk");
    // set full path
    shard_info.path_info.set_full_path(absl::StrFormat("s3://%s/%d/", s3_fs_info->bucket(), time(NULL)));

    // cache settings
    shard_info.cache_info.set_enable_cache(false);
    shard_info.cache_info.set_async_write_back(false);

    auto schema_or = StarOSWorker::build_scheme_from_shard_info(shard_info);
    EXPECT_TRUE(schema_or.ok());
    auto schema = schema_or.value();

    auto conf_or = shard_info.fslib_conf_from_this(false, "");
    EXPECT_TRUE(conf_or.ok());
    auto conf = conf_or.value();

    auto cache_key = StarOSWorker::get_cache_key(schema, conf);

    auto worker = std::make_shared<StarOSWorker>();
    g_worker = worker;

    EXPECT_TRUE(worker->add_shard(shard_info).ok());

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->get_shard_filesystem(shard_info.id, conf).ok());

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->remove_shard(shard_info.id).ok());

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));
}

TEST_F(StarOSWorkerTest, test_build_scheme_from_shard_info) {
    staros::starlet::ShardInfo shard_info;
    shard_info.id = 1;

    // Set the file system type to GS
    auto fs_info = shard_info.path_info.mutable_fs_info();
    fs_info->set_fs_type(staros::FileStoreType::GS);

    // Call the function and verify the result
    auto scheme_or = StarOSWorker::build_scheme_from_shard_info(shard_info);
    EXPECT_TRUE(scheme_or.ok());
    EXPECT_EQ("gs://", scheme_or.value());
}

TEST_F(StarOSWorkerTest, test_fs_cache_concurrent) {
    staros::starlet::fslib::register_builtin_filesystems();
    staros::starlet::ShardInfo shard_info;
    shard_info.id = 1;
    auto fs_info = shard_info.path_info.mutable_fs_info();
    fs_info->set_fs_type(staros::FileStoreType::S3);
    auto s3_fs_info = fs_info->mutable_s3_fs_info();
    s3_fs_info->set_bucket("test_bucket");
    s3_fs_info->set_endpoint("test_endpoint");
    s3_fs_info->set_region("us-east-1");
    auto credential = s3_fs_info->mutable_credential();
    auto simple_credential = credential->mutable_simple_credential();
    simple_credential->set_access_key("test_ak");
    simple_credential->set_access_key_secret("test_sk");
    shard_info.path_info.set_full_path(absl::StrFormat("s3://%s/%d/", s3_fs_info->bucket(), time(NULL)));

    shard_info.cache_info.set_enable_cache(true);
    shard_info.cache_info.set_async_write_back(false);

    auto schema_or = StarOSWorker::build_scheme_from_shard_info(shard_info);
    EXPECT_TRUE(schema_or.ok());
    auto schema = schema_or.value();

    auto conf_or = shard_info.fslib_conf_from_this(false, "");
    EXPECT_TRUE(conf_or.ok());
    auto conf = conf_or.value();

    auto cache_key = StarOSWorker::get_cache_key(schema, conf);

    auto worker = std::make_shared<StarOSWorker>();
    g_worker = worker;

    EXPECT_TRUE(worker->add_shard(shard_info).ok());

    std::shared_ptr<std::string> key1, key2;
    std::mutex mtx;
    std::condition_variable cv;
    bool ready = false;
    int ready_count = 0;

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));

    auto thread_func = [&](std::shared_ptr<std::string>& key) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            ready_count++;
            cv.notify_all();
            cv.wait(lock, [&] { return ready; });
        }

        auto result = worker->build_filesystem_from_shard_info(shard_info, conf);
        EXPECT_TRUE(result.ok());
        key = result->first;
    };

    std::thread t1(thread_func, std::ref(key1));
    std::thread t2(thread_func, std::ref(key2));

    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return ready_count == 2; });
        ready = true;
    }
    cv.notify_all();

    t1.join();
    t2.join();

    EXPECT_EQ(key1.get(), key2.get());

    EXPECT_EQ(*key1, *key2);

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->get_shard_filesystem(shard_info.id, conf).ok());

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->remove_shard(shard_info.id).ok());

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    key1.reset();
    key2.reset();

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));
}

} // namespace starrocks
#endif
