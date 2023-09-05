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

#include "fs/fs_memory.h"

#include <butil/files/file_path.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include "gutil/strings/join.h"
#include "testutil/assert.h"

namespace starrocks {

class MemoryFileSystemTest : public ::testing::Test {
protected:
    void SetUp() override { _fs = new MemoryFileSystem(); }

    void TearDown() override { delete _fs; }

    MemoryFileSystem* _fs = nullptr;
};

class SequentialFileWrapper {
public:
    explicit SequentialFileWrapper(SequentialFile* file) : _file(file) {}

    std::string read_len(size_t len) {
        std::string buff(len, '\0');
        ASSIGN_OR_ABORT(auto n, _file->read(buff.data(), buff.size()));
        buff.resize(n);
        return buff;
    }

    void reset(SequentialFile* file) { _file = file; }

private:
    SequentialFile* _file;
};

class DirectoryWalker {
public:
    explicit DirectoryWalker(FileSystem* fs) : _fs(fs) {}

    std::vector<std::string> walk(const std::string& path) {
        std::string normalized_path;
        CHECK_OK(_fs->canonicalize(path, &normalized_path));
        std::vector<std::string> result{normalized_path};
        auto status_or = _fs->is_directory(normalized_path);
        CHECK_OK(status_or.status());
        if (status_or.value()) {
            CHECK_OK(_fs->iterate_dir(normalized_path, [&, this](std::string_view filename) -> bool {
                auto subdir = walk(fmt::format("{}/{}", normalized_path, filename));
                result.insert(result.end(), subdir.begin(), subdir.end());
                return true;
            }));
        }
        return result;
    }

private:
    FileSystem* _fs;
};

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_canonicalize) {
    struct TestCase {
        bool success;
        std::string input;
        std::string output;
    };
    TestCase cases[] = {
            {true, "/", "/"},
            {true, "////", "/"},
            {true, "/../../..", "/"},
            {true, "/tmp/starrocks/../.", "/tmp"},
            {true, "/usr/bin/", "/usr/bin"},
            {true, "/usr//bin///", "/usr/bin"},
            {true, "/usr//bin///././.", "/usr/bin"},
            {false, "usr/bin", "usr/bin"},
            {false, "", ""},
    };

    for (const auto& t : cases) {
        std::string result;
        Status st = _fs->canonicalize(t.input, &result);
        if (t.success) {
            st.permit_unchecked_error();
            EXPECT_EQ(t.output, result) << st.to_string();
        } else {
            EXPECT_FALSE(st.ok());
        }
    }
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_create_and_list_dir) {
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/tmp"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/user"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/tmp/a"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/tmp/a/b"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/bin/"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/include"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/include/g++"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/include/llvm"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/bin/"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/bin/gcc"));

    EXPECT_STATUS(Status::NotFound(""), _fs->create_dir("/tmp/b/c"));
    EXPECT_STATUS(Status::AlreadyExist(""), _fs->create_dir("/tmp//"));
    EXPECT_STATUS(Status::InvalidArgument(""), _fs->create_dir("tmp/b"));

    bool created = false;
    EXPECT_STATUS(Status::OK(), _fs->create_dir_if_missing("/tmp", &created));
    EXPECT_FALSE(created);

    EXPECT_STATUS(Status::OK(), _fs->create_dir_if_missing("/starrocks", &created));
    EXPECT_TRUE(created);

    EXPECT_STATUS(Status::NotFound(""), _fs->create_dir_if_missing("/nonexist/starrocks", &created));

    EXPECT_STATUS(Status::OK(), _fs->create_file("/fileA"));
    EXPECT_STATUS(Status::AlreadyExist(""), _fs->create_dir_if_missing("/fileA", &created));

    struct ListDirCase {
        Status ret;
        std::string dirname;
        std::vector<std::string> children;
    };

    ListDirCase cases[] = {
            {Status::OK(), "/", {"tmp", "user", "usr", "bin", "include", "starrocks", "fileA"}},
            {Status::OK(), "/tmp", {"a"}},
            {Status::OK(), "/user", {}},
            {Status::OK(), "/usr", {"bin"}},
            {Status::OK(), "/usr/bin", {"gcc"}},
            {Status::OK(), "/usr/bin/gcc/", {}},
            {Status::OK(), "/include", {"g++", "llvm"}},
            {Status::OK(), "/include/g++", {}},
            {Status::OK(), "/include/llvm", {}},
            {Status::NotFound(""), "/tmp/b", {}},
            {Status::InvalidArgument(""), "user", {}},
    };

    for (auto& t : cases) {
        std::cout << "List " << t.dirname << std::endl;
        std::vector<std::string> children;
        EXPECT_STATUS(t.ret, _fs->get_children(t.dirname, &children));
        std::sort(t.children.begin(), t.children.end());
        std::sort(children.begin(), children.end());
        EXPECT_EQ(JoinStrings(t.children, ","), JoinStrings(children, ","));
    }
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_delete_dir) {
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/a"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/bin"));

    EXPECT_STATUS(Status::OK(), _fs->delete_dir("/bin"));
    EXPECT_STATUS(Status::IOError(""), _fs->delete_dir("/usr"));
    EXPECT_STATUS(Status::NotFound(""), _fs->delete_dir("/home"));

    std::vector<std::string> children;
    EXPECT_STATUS(Status::OK(), _fs->get_children("/", &children));
    EXPECT_EQ(1, children.size());
    EXPECT_EQ("usr", children[0]);
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_delete_dir_recursive) {
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/a"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/b"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/b/a"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/usr/b/a/a"));

    EXPECT_STATUS(Status::OK(), _fs->delete_dir_recursive("/usr"));

    std::vector<std::string> children;
    EXPECT_STATUS(Status::OK(), _fs->get_children("/", &children));
    EXPECT_EQ(0, children.size());
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_create_dir_recursive) {
    EXPECT_STATUS(Status::OK(), _fs->create_dir_recursive("/usr/b/a/a"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir_recursive("/usr/b/a/a"));
    EXPECT_TRUE(_fs->is_directory("/usr").value());
    EXPECT_TRUE(_fs->is_directory("/usr/b").value());
    EXPECT_TRUE(_fs->is_directory("/usr/b/a").value());
    EXPECT_TRUE(_fs->is_directory("/usr/b/a/a").value());
    EXPECT_STATUS(Status::OK(), _fs->delete_dir_recursive("/usr"));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_new_writable_file) {
    std::unique_ptr<WritableFile> file;
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    EXPECT_STATUS(Status::IOError(""), _fs->new_writable_file(opts, "/").status());
    file = *_fs->new_writable_file(opts, "/1.csv");
    ASSERT_OK(file->append("abc"));
    ASSERT_OK(file->close());
    std::vector<std::string> children;
    EXPECT_STATUS(Status::OK(), _fs->get_children("/", &children));
    ASSERT_EQ(1, children.size()) << JoinStrings(children, ",");
    EXPECT_EQ("1.csv", children[0]);
    ASSIGN_OR_ABORT(const uint64_t size, _fs->get_file_size("/1.csv"));
    EXPECT_EQ(3, size);
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_delete_file) {
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto file = *_fs->new_writable_file(opts, "/1.csv");
    ASSERT_OK(file->append("abc"));
    ASSERT_OK(file->close());

    EXPECT_STATUS(Status::NotFound(""), _fs->delete_file("/tmp"));
    EXPECT_STATUS(Status::NotFound(""), _fs->delete_dir("/1.csv"));
    EXPECT_STATUS(Status::OK(), _fs->delete_file("/1.csv"));

    std::vector<std::string> children;
    EXPECT_STATUS(Status::OK(), _fs->get_children("/", &children));
    EXPECT_EQ(0, children.size()) << JoinStrings(children, ",");
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_sequential_read) {
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    std::unique_ptr<WritableFile> writable_file;
    std::unique_ptr<SequentialFile> readable_file;
    writable_file = *_fs->new_writable_file(opts, "/a.txt");
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));

    readable_file = *_fs->new_sequential_file("/a.txt");
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("first line\nsecond line\n", wrapper.read_len(100));
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_CREATE_OR_OPEN_WITH_TRUNCATE) {
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto writable_file = *_fs->new_writable_file(opts, "/a.txt");
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    ASSERT_OK(writable_file->close());

    writable_file = *_fs->new_writable_file(opts, "/a.txt");

    auto readable_file = *_fs->new_sequential_file("/a.txt");
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_CREATE_OR_OPEN) {
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto writable_file = *_fs->new_writable_file(opts, "/a.txt");
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    ASSERT_OK(writable_file->close());

    opts.mode = FileSystem::CREATE_OR_OPEN;
    writable_file = *_fs->new_writable_file(opts, "/a.txt");

    auto readable_file = *_fs->new_sequential_file("/a.txt");
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("first line\nsecond line\n", wrapper.read_len(100));
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_MUST_EXIST) {
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto writable_file = *_fs->new_writable_file(opts, "/a.txt");
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    ASSERT_OK(writable_file->close());

    opts.mode = FileSystem::MUST_EXIST;
    writable_file = *_fs->new_writable_file(opts, "/a.txt");

    auto readable_file = *_fs->new_sequential_file("/a.txt");
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("first line\nsecond line\n", wrapper.read_len(100));
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_MUST_CREATE) {
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    std::unique_ptr<WritableFile> writable_file = *_fs->new_writable_file(opts, "/a.txt");
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    ASSERT_OK(writable_file->close());

    opts.mode = FileSystem::MUST_CREATE;
    EXPECT_STATUS(Status::AlreadyExist(""), _fs->new_writable_file(opts, "/a.txt").status());
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_link_file) {
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/tmp"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/home"));
    EXPECT_STATUS(Status::OK(), _fs->create_file("/tmp/a.txt"));
    EXPECT_STATUS(Status::OK(), _fs->create_file("/home/b.txt"));

    EXPECT_STATUS(Status::NotFound(""), _fs->link_file("/xx", "/home/xx"));
    EXPECT_STATUS(Status::AlreadyExist(""), _fs->link_file("/tmp/a.txt", "/home/b.txt"));
    EXPECT_STATUS(Status::OK(), _fs->link_file("/tmp/a.txt", "/home/a.txt"));

    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    std::unique_ptr<WritableFile> w = *_fs->new_writable_file(opts, "/tmp/a.txt");
    std::string content;

    EXPECT_STATUS(Status::OK(), w->append("content in a.txt"));
    EXPECT_STATUS(Status::OK(), _fs->delete_file("/tmp/a.txt"));
    EXPECT_STATUS(Status::NotFound(""), _fs->read_file("/tmp/a.txt", &content));

    EXPECT_STATUS(Status::OK(), _fs->read_file("/home/a.txt", &content));
    EXPECT_EQ("content in a.txt", content);

    EXPECT_STATUS(Status::OK(), _fs->delete_file("/home/a.txt"));
    EXPECT_STATUS(Status::NotFound(""), _fs->read_file("/home/a.txt", &content));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_rename) {
    std::vector<std::string> children;
    DirectoryWalker walker(_fs);

    EXPECT_STATUS(Status::OK(), _fs->create_dir("/dir1"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/dir2"));
    EXPECT_STATUS(Status::OK(), _fs->create_file("/file1"));
    EXPECT_STATUS(Status::OK(), _fs->create_file("/file2"));

    EXPECT_STATUS(Status::InvalidArgument(""), _fs->rename_file("/dir1", "/dir1/tmp"));
    EXPECT_STATUS(Status::NotFound(""), _fs->rename_file("/dir1", "/xxx/tmp"));
    EXPECT_STATUS(Status::NotFound(""), _fs->rename_file("/dir1/a.txt", "/dir2/a.txt"));
    EXPECT_STATUS(Status::IOError(""), _fs->rename_file("/dir1", "/file1"));
    EXPECT_STATUS(Status::IOError(""), _fs->rename_file("/file1", "/dir2"));

    EXPECT_STATUS(Status::OK(), _fs->rename_file("/dir1", "/dir1"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir2\n"
            "/file1\n"
            "/file2",
            JoinStrings(walker.walk("/"), "\n"));

    EXPECT_STATUS(Status::OK(), _fs->rename_file("/file1", "/file3"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir2\n"
            "/file2\n"
            "/file3",
            JoinStrings(walker.walk("/"), "\n"));

    EXPECT_STATUS(Status::OK(), _fs->rename_file("/file2", "/dir2/file2"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir2\n"
            "/dir2/file2\n"
            "/file3",
            JoinStrings(walker.walk("/"), "\n"));

    EXPECT_STATUS(Status::IOError(""), _fs->rename_file("/dir1", "/dir2"));

    EXPECT_STATUS(Status::OK(), _fs->rename_file("/dir2", "/dir1"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir1/file2\n"
            "/file3",
            JoinStrings(walker.walk("/"), "\n"));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_rename02) {
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/dir1"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/dir2"));
    EXPECT_STATUS(Status::OK(), _fs->create_file("/dir2/a"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/dir2/dir21"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/dir2/dir21/dir31/"));
    EXPECT_STATUS(Status::OK(), _fs->create_file("/dir2/dir21/b"));
    EXPECT_STATUS(Status::OK(), _fs->create_dir("/dir3"));

    DirectoryWalker walker(_fs);
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir2\n"
            "/dir2/a\n"
            "/dir2/dir21\n"
            "/dir2/dir21/b\n"
            "/dir2/dir21/dir31\n"
            "/dir3",
            JoinStrings(walker.walk("/"), "\n"));

    EXPECT_STATUS(Status::OK(), _fs->rename_file("/dir2", "/dir4"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir3\n"
            "/dir4\n"
            "/dir4/a\n"
            "/dir4/dir21\n"
            "/dir4/dir21/b\n"
            "/dir4/dir21/dir31",
            JoinStrings(walker.walk("/"), "\n"));
}

// NOLINTNEXTLINE
TEST_F(MemoryFileSystemTest, test_random_access_file) {
    const std::string content = "stay hungry stay foolish";
    EXPECT_STATUS(Status::OK(), _fs->append_file("/a.txt", content));

    auto f = *_fs->new_random_access_file("/a.txt");

    ASSIGN_OR_ABORT(const uint64_t size, f->get_size());
    EXPECT_EQ(content.size(), size);

    std::string buff(4, '\0');
    Slice slice(buff);
    ASSERT_OK(f->read_at_fully(0, slice.data, slice.size));
    EXPECT_EQ("stay", slice);

    ASSERT_OK(f->read_at_fully(5, slice.data, slice.size));
    EXPECT_EQ("hung", slice);

    ASSIGN_OR_ABORT(slice.size, f->read_at(17, slice.data, slice.size));
    EXPECT_EQ("fool", slice);

    ASSIGN_OR_ABORT(slice.size, f->read_at(21, slice.data, slice.size));
    EXPECT_EQ("ish", slice);

    EXPECT_ERROR(f->read_at_fully(22, slice.data, slice.size));
}

TEST_F(MemoryFileSystemTest, test_iterate_dir2) {
    ASSERT_OK(_fs->create_dir("/home"));
    ASSERT_OK(_fs->create_dir("/home/code"));
    ASSIGN_OR_ABORT(auto f, _fs->new_writable_file("/home/gcc"));
    ASSERT_OK(f->append("test"));
    ASSERT_OK(f->close());

    ASSERT_OK(_fs->iterate_dir2("/home", [](DirEntry entry) -> bool {
        auto name = entry.name;
        if (name == "code") {
            CHECK(entry.is_dir.has_value());
            CHECK(entry.is_dir.value());
            CHECK(!entry.mtime.has_value());
            CHECK(!entry.size.has_value());
        } else if (name == "gcc") {
            CHECK(entry.is_dir.has_value());
            CHECK(!entry.is_dir.value());
            CHECK(!entry.mtime.has_value());
            CHECK(entry.size.has_value());
            CHECK_EQ(4, entry.size.value());
        } else {
            CHECK(false) << "Unexpected file " << name;
        }
        return true;
    }));
}

} // namespace starrocks
