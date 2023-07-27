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

#include "common/s3_uri.h"

#include <gtest/gtest.h>

#include "testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(URITest, virtual_host_url) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://mybucket.s3.us-west-2.amazonaws.com/puppy.png"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("us-west-2", uri.region());
    EXPECT_EQ("mybucket", uri.bucket());
    EXPECT_EQ("puppy.png", uri.key());
    EXPECT_EQ("s3.us-west-2.amazonaws.com", uri.endpoint());
}

PARALLEL_TEST(URITest, path_style_url) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://s3.us-west-2.amazonaws.com/mybucket/puppy.jpg"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("us-west-2", uri.region());
    EXPECT_EQ("mybucket", uri.bucket());
    EXPECT_EQ("puppy.jpg", uri.key());
    EXPECT_EQ("s3.us-west-2.amazonaws.com", uri.endpoint());
}

PARALLEL_TEST(URITest, s3_scheme) {
    {
        S3URI uri;
        ASSERT_TRUE(uri.parse("s3://mybucket/puppy.jpg"));
        EXPECT_EQ("s3", uri.scheme());
        EXPECT_EQ("", uri.region());
        EXPECT_EQ("mybucket", uri.bucket());
        EXPECT_EQ("puppy.jpg", uri.key());
        EXPECT_EQ("", uri.endpoint());
    }
    {
        S3URI uri;
        ASSERT_TRUE(uri.parse("S3://test.name.with.dot/another.test/foo.parquet"));
        EXPECT_EQ("s3", uri.scheme());
        EXPECT_EQ("", uri.region());
        EXPECT_EQ("test.name.with.dot", uri.bucket());
        EXPECT_EQ("another.test/foo.parquet", uri.key());
        EXPECT_EQ("", uri.endpoint());
    }
    {
        S3URI uri;
        ASSERT_TRUE(uri.parse("s3A://test.name.with.dot/another.test/foo.parquet"));
        EXPECT_EQ("s3a", uri.scheme());
        EXPECT_EQ("", uri.region());
        EXPECT_EQ("test.name.with.dot", uri.bucket());
        EXPECT_EQ("another.test/foo.parquet", uri.key());
        EXPECT_EQ("", uri.endpoint());
    }
    {
        S3URI uri;
        ASSERT_TRUE(uri.parse("s3n://test.name.with.dot/another.test/foo.parquet"));
        EXPECT_EQ("s3n", uri.scheme());
        EXPECT_EQ("", uri.region());
        EXPECT_EQ("test.name.with.dot", uri.bucket());
        EXPECT_EQ("another.test/foo.parquet", uri.key());
        EXPECT_EQ("", uri.endpoint());
    }
}

PARALLEL_TEST(URITest, virtual_host_non_s3_url) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://examplebucket.oss-cn-hangzhou.aliyuncs.com/exampledir/example.txt"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("", uri.region());
    EXPECT_EQ("examplebucket", uri.bucket());
    EXPECT_EQ("exampledir/example.txt", uri.key());
    EXPECT_EQ("oss-cn-hangzhou.aliyuncs.com", uri.endpoint());
}

PARALLEL_TEST(URITest, with_query_and_fragment) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://examplebucket.oss-cn-hangzhou.aliyuncs.com/exampledir/example.txt?a=b#xyz"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("", uri.region());
    EXPECT_EQ("examplebucket", uri.bucket());
    EXPECT_EQ("exampledir/example.txt", uri.key());
    EXPECT_EQ("oss-cn-hangzhou.aliyuncs.com", uri.endpoint());
}

PARALLEL_TEST(URITest, empty) {
    S3URI uri;
    ASSERT_FALSE(uri.parse(""));
}

PARALLEL_TEST(URITest, missing_scheme) {
    S3URI uri;
    ASSERT_FALSE(uri.parse("/bucket/puppy.jpg"));
}

PARALLEL_TEST(URITest, oss_bucket) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("oss://sr-test/dataset/smith/foo.parquet"));
    EXPECT_EQ("oss", uri.scheme());
    EXPECT_EQ("", uri.region());
    EXPECT_EQ("sr-test", uri.bucket());
    EXPECT_EQ("dataset/smith/foo.parquet", uri.key());
    EXPECT_EQ("", uri.endpoint());
}

TEST(URITest, s3_complex_path) {
    {
        S3URI uri;
        ASSERT_TRUE(uri.parse("s3://smith-us-west-2/134f/hello/event_date=2023-07-24\\ 23%3A00%3A00/foo.parquet"));
        EXPECT_EQ("s3", uri.scheme());
        EXPECT_EQ("", uri.region());
        EXPECT_EQ("smith-us-west-2", uri.bucket());
        EXPECT_EQ("134f/hello/event_date=2023-07-24\\ 23%3A00%3A00/foo.parquet", uri.key());
        EXPECT_EQ("", uri.endpoint());
    }
    {
        S3URI uri;
        ASSERT_TRUE(uri.parse("s3://smith-us-west-2/134f/hello/event_date=2023-07-24\\%2023%3A00%3A00/foo.parquet"));
        EXPECT_EQ("s3", uri.scheme());
        EXPECT_EQ("", uri.region());
        EXPECT_EQ("smith-us-west-2", uri.bucket());
        EXPECT_EQ("134f/hello/event_date=2023-07-24\\%2023%3A00%3A00/foo.parquet", uri.key());
        EXPECT_EQ("", uri.endpoint());
    }
}

} // namespace starrocks
