// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/output_stream_wrapper.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace starrocks {

class OutputStreamWrapperTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::error_code error_code;
        std::filesystem::path path("FileOutputStreamTest");
        std::filesystem::remove_all(path, error_code);
        std::filesystem::create_directory(path, error_code);
        if (error_code) {
            std::cerr << "Fail to create directory " << path << ": " << error_code << std::endl;
            LOG(FATAL) << "Fail to create directory " << path << ": " << error_code;
        }

        std::filesystem::path file = path / "test.txt";
        auto res = Env::Default()->new_writable_file(file.string());
        if (!res.ok()) {
            std::cerr << "Fail to create " << file << ": " << res.status() << std::endl;
            LOG(FATAL) << "Fail to create " << file << ": " << res.status();
        }
        _file = std::move(res).value();
        _stream = std::make_unique<OutputStreamWrapper>(_file.get());
    }

    void TearDown() override {
        std::error_code error_code;
        std::filesystem::path path("FileOutputStreamTest");
        std::filesystem::remove_all(path, error_code);
    }

private:
    std::unique_ptr<WritableFile> _file;
    std::unique_ptr<OutputStreamWrapper> _stream;
};

// NOLINTNEXTLINE
TEST_F(OutputStreamWrapperTest, test_write) {
    auto& stream = *_stream;

    stream << 10 << " hello";
    ASSERT_TRUE(stream.good());
    ASSERT_EQ(8, stream.size());
    ASSERT_TRUE(stream.append(" world!").ok());
    ASSERT_EQ(15, stream.size());
    stream << " apple";
    ASSERT_TRUE(stream.good());
    ASSERT_TRUE(stream.good());
    ASSERT_EQ(21, stream.size());

    auto rf = *Env::Default()->new_random_access_file(_file->filename());
    std::string buff(21, 0);
    Slice slice(buff);
    uint64_t size = 0;
    auto st = rf->size(&size);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(21, size);

    st = rf->read_at_fully(0, slice.data, slice.size);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ("10 hello world! apple", buff);
}

} // namespace starrocks
