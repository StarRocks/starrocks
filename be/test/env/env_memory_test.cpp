// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/env_memory.h"

#include <butil/files/file_path.h>
#include <gtest/gtest.h>

#include "gutil/strings/join.h"

namespace starrocks {

#define CHECK_OK(stmt)                                \
    do {                                              \
        Status __status = (stmt);                     \
        CHECK(__status.ok()) << __status.to_string(); \
    } while (0)

#define ASSERT_OK(stmt)                                    \
    do {                                                   \
        Status __status = (stmt);                          \
        ASSERT_TRUE(_status.ok()) << __status.to_string(); \
    }

#define EXPECT_STATUS(expect, stmt)                                                          \
    do {                                                                                     \
        Status exp = (expect);                                                               \
        Status real = (stmt);                                                                \
        EXPECT_EQ(exp.code(), real.code()) << exp.to_string() << " vs " << real.to_string(); \
    } while (0)

class EnvMemoryTest : public ::testing::Test {
protected:
    void SetUp() override { _env = new EnvMemory(); }

    void TearDown() override { delete _env; }

    EnvMemory* _env = nullptr;
};

class SequentialFileWrapper {
public:
    explicit SequentialFileWrapper(SequentialFile* file) : _file(file) {}

    std::string read_len(size_t len) {
        std::string buff(len, '\0');
        Slice s(buff);
        Status st = _file->read(&s);
        CHECK(st.ok()) << st.to_string();
        buff.resize(s.size);
        return buff;
    }

    void reset(SequentialFile* file) { _file = file; }

private:
    SequentialFile* _file;
};

class DirectoryWalker {
public:
    explicit DirectoryWalker(Env* env) : _env(env) {}

    std::vector<std::string> walk(const std::string& path) {
        std::string normalized_path;
        CHECK_OK(_env->canonicalize(path, &normalized_path));
        std::vector<std::string> result{normalized_path};
        bool is_dir = false;
        CHECK_OK(_env->is_directory(normalized_path, &is_dir));
        if (is_dir) {
            CHECK_OK(_env->iterate_dir(normalized_path, [&, this](const char* filename) -> bool {
                auto subdir = walk(normalized_path + "/" + filename);
                result.insert(result.end(), subdir.begin(), subdir.end());
                return true;
            }));
        }
        return result;
    }

private:
    Env* _env;
};

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_canonicalize) {
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
        Status st = _env->canonicalize(t.input, &result);
        if (t.success) {
            EXPECT_EQ(t.output, result) << st.to_string();
        } else {
            EXPECT_FALSE(st.ok());
        }
    }
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_create_and_list_dir) {
    EXPECT_STATUS(Status::OK(), _env->create_dir("/tmp"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/user"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/tmp/a"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/tmp/a/b"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/bin/"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/include"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/include/g++"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/include/llvm"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/usr/"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/usr/bin/"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/usr/bin/gcc"));

    EXPECT_STATUS(Status::NotFound(""), _env->create_dir("/tmp/b/c"));
    EXPECT_STATUS(Status::AlreadyExist(""), _env->create_dir("/tmp//"));
    EXPECT_STATUS(Status::InvalidArgument(""), _env->create_dir("tmp/b"));

    bool created = false;
    EXPECT_STATUS(Status::OK(), _env->create_dir_if_missing("/tmp", &created));
    EXPECT_FALSE(created);

    EXPECT_STATUS(Status::OK(), _env->create_dir_if_missing("/starrocks", &created));
    EXPECT_TRUE(created);

    EXPECT_STATUS(Status::NotFound(""), _env->create_dir_if_missing("/nonexist/starrocks", &created));

    EXPECT_STATUS(Status::OK(), _env->create_file("/fileA"));
    EXPECT_STATUS(Status::AlreadyExist(""), _env->create_dir_if_missing("/fileA", &created));

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
        EXPECT_STATUS(t.ret, _env->get_children(t.dirname, &children));
        std::sort(t.children.begin(), t.children.end());
        std::sort(children.begin(), children.end());
        EXPECT_EQ(JoinStrings(t.children, ","), JoinStrings(children, ","));
    }
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_delete_dir) {
    EXPECT_STATUS(Status::OK(), _env->create_dir("/usr"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/usr/a"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/bin"));

    EXPECT_STATUS(Status::OK(), _env->delete_dir("/bin"));
    EXPECT_STATUS(Status::IOError(""), _env->delete_dir("/usr"));
    EXPECT_STATUS(Status::NotFound(""), _env->delete_dir("/home"));

    std::vector<std::string> children;
    EXPECT_STATUS(Status::OK(), _env->get_children("/", &children));
    EXPECT_EQ(1, children.size());
    EXPECT_EQ("usr", children[0]);
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_new_writable_file) {
    std::unique_ptr<WritableFile> file;
    EXPECT_STATUS(Status::IOError(""), _env->new_writable_file("/", &file));
    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/1.csv", &file));
    file->append("abc");
    file->close();
    std::vector<std::string> children;
    EXPECT_STATUS(Status::OK(), _env->get_children("/", &children));
    ASSERT_EQ(1, children.size()) << JoinStrings(children, ",");
    EXPECT_EQ("1.csv", children[0]);
    uint64_t size = 0;
    EXPECT_STATUS(Status::OK(), _env->get_file_size("/1.csv", &size));
    EXPECT_EQ(3, size);
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_delete_file) {
    std::unique_ptr<WritableFile> file;
    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/1.csv", &file));
    file->append("abc");
    file->close();

    EXPECT_STATUS(Status::NotFound(""), _env->delete_file("/tmp"));
    EXPECT_STATUS(Status::NotFound(""), _env->delete_dir("/1.csv"));
    EXPECT_STATUS(Status::OK(), _env->delete_file("/1.csv"));

    std::vector<std::string> children;
    EXPECT_STATUS(Status::OK(), _env->get_children("/", &children));
    EXPECT_EQ(0, children.size()) << JoinStrings(children, ",");
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_sequential_read) {
    std::unique_ptr<WritableFile> writable_file;
    std::unique_ptr<SequentialFile> readable_file;
    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/a.txt", &writable_file));
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));

    EXPECT_STATUS(Status::OK(), _env->new_sequential_file("/a.txt", &readable_file));
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("first line\nsecond line\n", wrapper.read_len(100));
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_CREATE_OR_OPEN_WITH_TRUNCATE) {
    std::unique_ptr<WritableFile> writable_file;
    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/a.txt", &writable_file));
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    writable_file->close();

    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/a.txt", &writable_file));

    std::unique_ptr<SequentialFile> readable_file;
    EXPECT_STATUS(Status::OK(), _env->new_sequential_file("/a.txt", &readable_file));
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_CREATE_OR_OPEN) {
    std::unique_ptr<WritableFile> writable_file;
    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/a.txt", &writable_file));
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    writable_file->close();

    WritableFileOptions opts{.mode = Env::CREATE_OR_OPEN};
    EXPECT_STATUS(Status::OK(), _env->new_writable_file(opts, "/a.txt", &writable_file));

    std::unique_ptr<SequentialFile> readable_file;
    EXPECT_STATUS(Status::OK(), _env->new_sequential_file("/a.txt", &readable_file));
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("first line\nsecond line\n", wrapper.read_len(100));
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_MUST_EXIST) {
    std::unique_ptr<WritableFile> writable_file;
    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/a.txt", &writable_file));
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    writable_file->close();

    WritableFileOptions opts{.mode = Env::MUST_EXIST};
    EXPECT_STATUS(Status::OK(), _env->new_writable_file(opts, "/a.txt", &writable_file));

    std::unique_ptr<SequentialFile> readable_file;
    EXPECT_STATUS(Status::OK(), _env->new_sequential_file("/a.txt", &readable_file));
    SequentialFileWrapper wrapper(readable_file.get());
    EXPECT_EQ("first line\nsecond line\n", wrapper.read_len(100));
    EXPECT_EQ("", wrapper.read_len(100));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_MUST_CREATE) {
    std::unique_ptr<WritableFile> writable_file;
    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/a.txt", &writable_file));
    EXPECT_STATUS(Status::OK(), writable_file->append("first line\n"));
    EXPECT_STATUS(Status::OK(), writable_file->append("second line\n"));
    writable_file->close();

    WritableFileOptions opts{.mode = Env::MUST_CREATE};
    EXPECT_STATUS(Status::AlreadyExist(""), _env->new_writable_file(opts, "/a.txt", &writable_file));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_link_file) {
    EXPECT_STATUS(Status::OK(), _env->create_dir("/tmp"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/home"));
    EXPECT_STATUS(Status::OK(), _env->create_file("/tmp/a.txt"));
    EXPECT_STATUS(Status::OK(), _env->create_file("/home/b.txt"));

    EXPECT_STATUS(Status::NotFound(""), _env->link_file("/xx", "/home/xx"));
    EXPECT_STATUS(Status::AlreadyExist(""), _env->link_file("/tmp/a.txt", "/home/b.txt"));
    EXPECT_STATUS(Status::OK(), _env->link_file("/tmp/a.txt", "/home/a.txt"));

    std::unique_ptr<WritableFile> w;
    std::string content;

    EXPECT_STATUS(Status::OK(), _env->new_writable_file("/tmp/a.txt", &w));
    EXPECT_STATUS(Status::OK(), w->append("content in a.txt"));
    EXPECT_STATUS(Status::OK(), _env->delete_file("/tmp/a.txt"));
    EXPECT_STATUS(Status::NotFound(""), _env->read_file("/tmp/a.txt", &content));

    EXPECT_STATUS(Status::OK(), _env->read_file("/home/a.txt", &content));
    EXPECT_EQ("content in a.txt", content);

    EXPECT_STATUS(Status::OK(), _env->delete_file("/home/a.txt"));
    EXPECT_STATUS(Status::NotFound(""), _env->read_file("/home/a.txt", &content));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_rename) {
    std::vector<std::string> children;
    DirectoryWalker walker(_env);

    EXPECT_STATUS(Status::OK(), _env->create_dir("/dir1"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/dir2"));
    EXPECT_STATUS(Status::OK(), _env->create_file("/file1"));
    EXPECT_STATUS(Status::OK(), _env->create_file("/file2"));

    EXPECT_STATUS(Status::InvalidArgument(""), _env->rename_file("/dir1", "/dir1/tmp"));
    EXPECT_STATUS(Status::NotFound(""), _env->rename_file("/dir1", "/xxx/tmp"));
    EXPECT_STATUS(Status::NotFound(""), _env->rename_file("/dir1/a.txt", "/dir2/a.txt"));
    EXPECT_STATUS(Status::IOError(""), _env->rename_file("/dir1", "/file1"));
    EXPECT_STATUS(Status::IOError(""), _env->rename_file("/file1", "/dir2"));

    EXPECT_STATUS(Status::OK(), _env->rename_file("/dir1", "/dir1"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir2\n"
            "/file1\n"
            "/file2",
            JoinStrings(walker.walk("/"), "\n"));

    EXPECT_STATUS(Status::OK(), _env->rename_file("/file1", "/file3"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir2\n"
            "/file2\n"
            "/file3",
            JoinStrings(walker.walk("/"), "\n"));

    EXPECT_STATUS(Status::OK(), _env->rename_file("/file2", "/dir2/file2"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir2\n"
            "/dir2/file2\n"
            "/file3",
            JoinStrings(walker.walk("/"), "\n"));

    EXPECT_STATUS(Status::IOError(""), _env->rename_file("/dir1", "/dir2"));

    EXPECT_STATUS(Status::OK(), _env->rename_file("/dir2", "/dir1"));
    EXPECT_EQ(
            "/\n"
            "/dir1\n"
            "/dir1/file2\n"
            "/file3",
            JoinStrings(walker.walk("/"), "\n"));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_rename02) {
    EXPECT_STATUS(Status::OK(), _env->create_dir("/dir1"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/dir2"));
    EXPECT_STATUS(Status::OK(), _env->create_file("/dir2/a"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/dir2/dir21"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/dir2/dir21/dir31/"));
    EXPECT_STATUS(Status::OK(), _env->create_file("/dir2/dir21/b"));
    EXPECT_STATUS(Status::OK(), _env->create_dir("/dir3"));

    DirectoryWalker walker(_env);
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

    EXPECT_STATUS(Status::OK(), _env->rename_file("/dir2", "/dir4"));
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
TEST_F(EnvMemoryTest, test_random_rw_file) {
    std::unique_ptr<RandomRWFile> file;
    EXPECT_STATUS(Status::OK(), _env->new_random_rw_file("/a.txt", &file));
    EXPECT_STATUS(Status::OK(), file->write_at(10, "aaaa"));
    EXPECT_STATUS(Status::OK(), file->write_at(0, "0123456789"));
    EXPECT_STATUS(Status::OK(), file->write_at(5, "54321"));

    std::string buff(10, '\0');
    Slice slice1(buff.data(), 5);
    Slice slice2(buff.data() + 5, 5);
    uint64_t size = 0;
    EXPECT_STATUS(Status::OK(), file->size(&size));
    EXPECT_EQ(14, size);

    EXPECT_STATUS(Status::OK(), file->read_at(5, slice1));
    EXPECT_EQ("54321", slice1);

    std::vector<Slice> vec{slice1, slice2};
    EXPECT_STATUS(Status::OK(), file->readv_at(0, vec.data(), 2));
    EXPECT_EQ("01234", slice1);
    EXPECT_EQ("54321", slice2);

    EXPECT_STATUS(Status::IOError(""), file->read_at(10, slice1));
    EXPECT_STATUS(Status::IOError(""), file->readv_at(5, vec.data(), 2));
}

// NOLINTNEXTLINE
TEST_F(EnvMemoryTest, test_random_access_file) {
    const std::string content = "stay hungry stay foolish";
    EXPECT_STATUS(Status::OK(), _env->append_file("/a.txt", content));

    std::unique_ptr<RandomAccessFile> f;
    EXPECT_STATUS(Status::OK(), _env->new_random_access_file("/a.txt", &f));

    uint64_t size = 0;
    EXPECT_STATUS(Status::OK(), f->size(&size));
    EXPECT_EQ(content.size(), size);

    std::string buff(4, '\0');
    Slice slice(buff);
    EXPECT_STATUS(Status::OK(), f->read_at(0, slice));
    EXPECT_EQ("stay", slice);

    EXPECT_STATUS(Status::OK(), f->read_at(5, slice));
    EXPECT_EQ("hung", slice);

    EXPECT_STATUS(Status::OK(), f->read(17, &slice));
    EXPECT_EQ("fool", slice);

    EXPECT_STATUS(Status::OK(), f->read(21, &slice));
    EXPECT_EQ("ish", slice);

    EXPECT_STATUS(Status::IOError(""), f->read_at(22, slice));
}

} // namespace starrocks
