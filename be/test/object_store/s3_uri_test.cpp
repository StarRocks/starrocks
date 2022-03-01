// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "object_store/s3_uri.h"

#include <gtest/gtest.h>

#include "testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(S3URITest, test_parse) {
    {
        S3URI uri("s3://bucket/object-key");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("s3", uri.schema());
        ASSERT_EQ("bucket", uri.bucket());
        ASSERT_EQ("object-key", uri.key());
    }
    {
        S3URI uri("oss://bucket/object-key");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("oss", uri.schema());
        ASSERT_EQ("bucket", uri.bucket());
        ASSERT_EQ("object-key", uri.key());
    }
    {
        S3URI uri("s3a://bucket/object-key");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("s3a", uri.schema());
        ASSERT_EQ("bucket", uri.bucket());
        ASSERT_EQ("object-key", uri.key());
    }
    {
        S3URI uri("s3n://bucket/object-key");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("s3n", uri.schema());
        ASSERT_EQ("bucket", uri.bucket());
        ASSERT_EQ("object-key", uri.key());
    }
    {
        S3URI uri("s3://bucket/path/of/object");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("s3", uri.schema());
        ASSERT_EQ("bucket", uri.bucket());
        ASSERT_EQ("path/of/object", uri.key());
    }
    {
        S3URI uri("s3://bucket/path/of/object?a=b");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("s3", uri.schema());
        ASSERT_EQ("bucket", uri.bucket());
        ASSERT_EQ("path/of/object", uri.key());
    }
    {
        S3URI uri("s3://bucket/path/of/object#xyz");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("s3", uri.schema());
        ASSERT_EQ("bucket", uri.bucket());
        ASSERT_EQ("path/of/object", uri.key());
    }
    {
        S3URI uri("s3://bucket.region.amazons.com/path/of/object?a=b#xyz");
        ASSERT_TRUE(uri.parse());
        ASSERT_EQ("s3", uri.schema());
        ASSERT_EQ("bucket.region.amazons.com", uri.bucket());
        ASSERT_EQ("path/of/object", uri.key());
    }
}

PARALLEL_TEST(S3URITest, test_invalid_uri) {
    {
        S3URI uri("bucket/object-key");
        ASSERT_FALSE(uri.parse());
    }
    {
        S3URI uri("://bucket/object-key");
        ASSERT_FALSE(uri.parse());
    }
    {
        S3URI uri("s3:/bucket/object-key");
        ASSERT_FALSE(uri.parse());
    }
    {
        S3URI uri("s3://object-key");
        ASSERT_FALSE(uri.parse());
    }
}
} // namespace starrocks
