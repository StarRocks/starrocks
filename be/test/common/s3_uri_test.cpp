// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "common/s3_uri.h"

#include <gtest/gtest.h>

#include "testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(S3URITest, virtual_host_url) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://mybucket.s3.us-west-2.amazonaws.com/puppy.png"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("us-west-2", uri.region());
    EXPECT_EQ("mybucket", uri.bucket());
    EXPECT_EQ("puppy.png", uri.key());
    EXPECT_EQ("s3.us-west-2.amazonaws.com", uri.endpoint());
}

PARALLEL_TEST(S3URITest, path_style_url) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://s3.us-west-2.amazonaws.com/mybucket/puppy.jpg"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("us-west-2", uri.region());
    EXPECT_EQ("mybucket", uri.bucket());
    EXPECT_EQ("puppy.jpg", uri.key());
    EXPECT_EQ("s3.us-west-2.amazonaws.com", uri.endpoint());
}

PARALLEL_TEST(S3URITest, s3_scheme) {
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

PARALLEL_TEST(S3URITest, virtual_host_non_s3_url) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://examplebucket.oss-cn-hangzhou.aliyuncs.com/exampledir/example.txt"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("", uri.region());
    EXPECT_EQ("examplebucket", uri.bucket());
    EXPECT_EQ("exampledir/example.txt", uri.key());
    EXPECT_EQ("oss-cn-hangzhou.aliyuncs.com", uri.endpoint());
}

PARALLEL_TEST(S3URITest, with_query_and_fragment) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("https://examplebucket.oss-cn-hangzhou.aliyuncs.com/exampledir/example.txt?a=b#xyz"));
    EXPECT_EQ("https", uri.scheme());
    EXPECT_EQ("", uri.region());
    EXPECT_EQ("examplebucket", uri.bucket());
    EXPECT_EQ("exampledir/example.txt", uri.key());
    EXPECT_EQ("oss-cn-hangzhou.aliyuncs.com", uri.endpoint());
}

PARALLEL_TEST(S3URITest, empty) {
    S3URI uri;
    ASSERT_FALSE(uri.parse(""));
}

PARALLEL_TEST(S3URITest, missing_scheme) {
    S3URI uri;
    ASSERT_FALSE(uri.parse("/bucket/puppy.jpg"));
}

PARALLEL_TEST(S3URITest, oss_bucket) {
    S3URI uri;
    ASSERT_TRUE(uri.parse("oss://sr-test/dataset/smith/foo.parquet"));
    EXPECT_EQ("oss", uri.scheme());
    EXPECT_EQ("", uri.region());
    EXPECT_EQ("sr-test", uri.bucket());
    EXPECT_EQ("dataset/smith/foo.parquet", uri.key());
    EXPECT_EQ("", uri.endpoint());
}

} // namespace starrocks
