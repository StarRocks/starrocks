// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "util/buffered_stream.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "io/string_input_stream.h"

namespace starrocks {

class BufferedStreamTest : public testing::Test {
public:
    BufferedStreamTest() {}
    virtual ~BufferedStreamTest() {}
};

TEST_F(BufferedStreamTest, Normal) {
    std::string test_str;
    test_str.resize(10);
    for (int i = 0; i < 10; ++i) {
        test_str[i] = i;
    }
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(test_str)), "string-file");

    DefaultBufferedInputStream stream(&file, 0, 10);

    ASSERT_EQ(0, stream.tell());
    {
        stream.seek_to(1);
        const uint8_t* value;
        size_t nbytes = 1;
        auto st = stream.get_bytes(&value, &nbytes, true);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(1, *value);

        stream.skip(5);
        nbytes = 1;
        st = stream.get_bytes(&value, &nbytes, true);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(6, *value);

        stream.skip(4);
        nbytes = 1;
        st = stream.get_bytes(&value, &nbytes, true);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(0, nbytes);

        stream.seek_to(5);
        nbytes = 1;
        st = stream.get_bytes(&value, &nbytes, true);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(5, *value);
    }
}

TEST_F(BufferedStreamTest, Large) {
    std::string test_str;
    test_str.resize(66 * 1024);
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(test_str)), "string-file");

    DefaultBufferedInputStream stream(&file, 0, 66 * 1024);

    // get 1K
    {
        const uint8_t* buf;
        size_t nbytes = 1024;
        auto st = stream.get_bytes(&buf, &nbytes, false);
        ASSERT_TRUE(st.ok());
    }
    // get 65K to enlarge the buffer
    {
        const uint8_t* buf;
        size_t nbytes = 65 * 1024;
        auto st = stream.get_bytes(&buf, &nbytes, false);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(65 * 1024, nbytes);
    }
    // get nothing
    {
        const uint8_t* buf;
        size_t nbytes = 1 * 1024;
        auto st = stream.get_bytes(&buf, &nbytes, false);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(0, nbytes);
    }
}

TEST_F(BufferedStreamTest, Large2) {
    std::string test_str;
    test_str.resize(65 * 1024);
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(test_str)), "string-file");

    DefaultBufferedInputStream stream(&file, 0, 65 * 1024);

    // get 1K
    {
        const uint8_t* buf;
        size_t nbytes = 1024;
        auto st = stream.get_bytes(&buf, &nbytes, false);
        ASSERT_TRUE(st.ok());
    }
    // get 64K to move
    {
        const uint8_t* buf;
        size_t nbytes = 64 * 1024;
        auto st = stream.get_bytes(&buf, &nbytes, false);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(64 * 1024, nbytes);
    }
    // get nothing
    {
        const uint8_t* buf;
        size_t nbytes = 1 * 1024;
        auto st = stream.get_bytes(&buf, &nbytes, false);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(0, nbytes);
    }
}

} // namespace starrocks
