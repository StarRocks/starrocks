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

#include "fs/bundle_file.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "fs/fs_util.h"
namespace starrocks {

class BundleFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        _test_dir = "./test_bundle_file_" + std::to_string(time(nullptr));
        ASSERT_OK(fs::create_directories(_test_dir));
    }

    void TearDown() override {
        if (!_test_dir.empty()) {
            fs::remove_all(_test_dir);
        }
    }

    std::string _test_dir;
};

TEST_F(BundleFileTest, test_write_read) {
    // test data
    std::string test_data1 = "Hello, SharedFile!";
    std::string test_data2 = "This is test data for shared file.";
    std::string test_data3 = "More data to test aggregation.";

    std::string bundle_file_path = _test_dir + "/test_bundle_file.dat";

    // 1. create shared file context
    auto shared_context = std::make_shared<BundleWritableFileContext>();
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });

    // 2. test write
    {
        WritableFileOptions opts;

        // first writer
        auto shared_writer1 = std::make_unique<BundleWritableFile>(shared_context.get(), opts.encryption_info);

        // second writer
        auto shared_writer2 = std::make_unique<BundleWritableFile>(shared_context.get(), opts.encryption_info);

        // third writer
        auto shared_writer3 = std::make_unique<BundleWritableFile>(shared_context.get(), opts.encryption_info);

        // write data
        shared_context->increase_active_writers();
        shared_context->increase_active_writers();
        shared_context->increase_active_writers();
        ASSERT_OK(shared_writer1->append(test_data1));
        ASSERT_OK(shared_writer2->append(test_data2));
        ASSERT_OK(shared_writer3->append(test_data3));

        // sync
        ASSERT_OK(shared_writer1->sync());
        ASSERT_OK(shared_writer2->sync());
        ASSERT_OK(shared_writer3->sync());

        // close writers
        ASSERT_OK(shared_writer1->close());
        ASSERT_OK(shared_writer2->close());
        ASSERT_OK(shared_writer3->close());
        // decrease active writers
        ASSERT_OK(shared_context->decrease_active_writers());
        ASSERT_OK(shared_context->decrease_active_writers());
        ASSERT_OK(shared_context->decrease_active_writers());
    }

    // 3. test whole read
    {
        // read the shared file
        ASSIGN_OR_ABORT(auto reader, fs::new_random_access_file(bundle_file_path));

        // verify file size
        ASSIGN_OR_ABORT(auto file_size, reader->get_size());
        size_t expected_size = test_data1.size() + test_data2.size() + test_data3.size();
        ASSERT_EQ(file_size, expected_size);

        // read and verify the content
        std::string read_buffer;
        read_buffer.resize(file_size);
        ASSIGN_OR_ABORT(auto bytes_read, reader->read_at(0, read_buffer.data(), file_size));
        ASSERT_EQ(bytes_read, file_size);

        // verify the content
        std::string expected_content = test_data1 + test_data2 + test_data3;
        ASSERT_EQ(read_buffer, expected_content);
    }

    // 4. test partial read
    {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(bundle_file_path));
        RandomAccessFileOptions opts;

        // read first segment
        FileInfo file_info1{.path = bundle_file_path, .size = test_data1.size(), .bundle_file_offset = 0};
        ASSIGN_OR_ABORT(auto reader1, fs->new_random_access_file_with_bundling(opts, file_info1));
        std::string buffer1;
        buffer1.resize(test_data1.size());
        ASSIGN_OR_ABORT(auto bytes_read1, reader1->read_at(0, buffer1.data(), test_data1.size()));
        ASSERT_EQ(bytes_read1, test_data1.size());
        ASSERT_EQ(buffer1, test_data1);

        // read second segment
        FileInfo file_info2{
                .path = bundle_file_path, .size = test_data2.size(), .bundle_file_offset = test_data1.size()};
        ASSIGN_OR_ABORT(auto reader2, fs->new_random_access_file_with_bundling(opts, file_info2));
        std::string buffer2;
        buffer2.resize(test_data2.size());
        ASSIGN_OR_ABORT(auto bytes_read2, reader2->read_at(0, buffer2.data(), test_data2.size()));
        ASSERT_EQ(bytes_read2, test_data2.size());
        ASSERT_EQ(buffer2, test_data2);

        // read third segment
        FileInfo file_info3{.path = bundle_file_path,
                            .size = test_data3.size(),
                            .bundle_file_offset = test_data1.size() + test_data2.size()};
        ASSIGN_OR_ABORT(auto reader3, fs->new_random_access_file_with_bundling(opts, file_info3));
        std::string buffer3;
        buffer3.resize(test_data3.size());
        ASSIGN_OR_ABORT(auto bytes_read3, reader3->read_at(0, buffer3.data(), test_data3.size()));
        ASSERT_EQ(bytes_read3, test_data3.size());
        ASSERT_EQ(buffer3, test_data3);
    }
}

TEST_F(BundleFileTest, test_empty_write) {
    std::string bundle_file_path = _test_dir + "/test_empty_bundle_file.dat";

    auto shared_context = std::make_shared<BundleWritableFileContext>();
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });

    WritableFileOptions opts;
    auto shared_writer = std::make_unique<BundleWritableFile>(shared_context.get(), opts.encryption_info);
    shared_context->increase_active_writers();
    // write empty data
    ASSERT_OK(shared_writer->append(""));
    ASSERT_OK(shared_writer->sync());
    ASSERT_OK(shared_writer->close());
    shared_context->decrease_active_writers();
    ASSIGN_OR_ABORT(auto reader, fs::new_random_access_file(bundle_file_path));
    // verify file size
    ASSIGN_OR_ABORT(auto file_size, reader->get_size());
    ASSERT_EQ(file_size, 0);
    // read and verify the content
    std::string content;
    content.resize(file_size);
    ASSIGN_OR_ABORT(auto bytes_read, reader->read_at(0, content.data(), file_size));
    ASSERT_EQ(bytes_read, file_size);
    ASSERT_EQ(content, "");
}

TEST_F(BundleFileTest, test_concurrent_write) {
    std::string bundle_file_path = _test_dir + "/test_concurrent_bundle_file.dat";

    auto shared_context = std::make_shared<BundleWritableFileContext>();
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });

    const int num_writers = 5;
    const std::string base_data = "Writer_";

    // concurrent write data
    {
        std::vector<std::unique_ptr<BundleWritableFile>> writers;
        WritableFileOptions opts;

        // create multiple writers
        for (int i = 0; i < num_writers; ++i) {
            auto shared_writer = std::make_unique<BundleWritableFile>(shared_context.get(), opts.encryption_info);
            writers.push_back(std::move(shared_writer));
        }

        // concurrently write data
        for (int i = 0; i < num_writers; ++i) {
            std::string data = base_data + std::to_string(i) + "_data";
            ASSERT_OK(writers[i]->append(data));
        }

        // close writers
        for (auto& writer : writers) {
            ASSERT_OK(writer->sync());
            ASSERT_OK(writer->close());
        }
    }

    // verify the content of the shared file
    {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(bundle_file_path));
        // 1. create num_writers readers
        std::vector<std::unique_ptr<RandomAccessFile>> readers;
        RandomAccessFileOptions opts;
        for (int i = 0; i < num_writers; ++i) {
            FileInfo file_info{.path = bundle_file_path,
                               .size = base_data.size() + std::to_string(i).size() + 5,
                               .bundle_file_offset = (base_data.size() + std::to_string(i).size() + 5) * i};
            ASSIGN_OR_ABORT(auto reader, fs->new_random_access_file_with_bundling(opts, file_info));
            readers.push_back(std::move(reader));
        }
        // 2. read and verify the content
        for (int i = 0; i < num_writers; ++i) {
            std::string expected_data = base_data + std::to_string(i) + "_data";
            std::string read_buffer;
            read_buffer.resize(expected_data.size());
            ASSIGN_OR_ABORT(auto bytes_read, readers[i]->read_at(0, read_buffer.data(), expected_data.size()));
            ASSERT_EQ(bytes_read, expected_data.size());
            ASSERT_EQ(read_buffer, expected_data);
        }
    }
}

TEST_F(BundleFileTest, test_touch_cache_and_statistics) {
    std::string test_data1 = "Hello, SharedFile!";
    std::string bundle_file_path = _test_dir + "/test_touch_cache_bundle_file.dat";

    auto shared_context = std::make_shared<BundleWritableFileContext>();
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });
    shared_context->try_create_bundle_file([&]() {
        WritableFileOptions opts;
        return fs::new_writable_file(opts, bundle_file_path);
    });

    WritableFileOptions wopts;
    auto shared_writer = std::make_unique<BundleWritableFile>(shared_context.get(), wopts.encryption_info);
    shared_context->increase_active_writers();
    // write some data
    ASSERT_OK(shared_writer->append(test_data1));
    ASSERT_OK(shared_writer->sync());
    ASSERT_OK(shared_writer->close());
    shared_context->decrease_active_writers();

    // touch cache
    RandomAccessFileOptions ropts;
    FileInfo file_info2{.path = bundle_file_path, .size = test_data1.size(), .bundle_file_offset = 0};
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(bundle_file_path));
    ASSIGN_OR_ABORT(auto reader, fs->new_random_access_file_with_bundling(ropts, file_info2));
    ASSERT_OK(reader->touch_cache(0, sizeof(test_data1))); // touch the first 30 bytes

    // get numeric statistics
    ASSIGN_OR_ABORT(auto numeric_stats, reader->get_numeric_statistics());
}

} // namespace starrocks
