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

#include "connector/hive/paimon/paimon_file_system.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "fs/fs.h"

namespace starrocks {
namespace {

struct WriteFaults {
    bool short_write = false;
    bool append_error = false;
    bool flush_error = false;
    bool close_error = false;
    int append_calls = 0;
    int flush_calls = 0;
    int close_calls = 0;
};

// Faults wrap a real local file rather than substituting an in-memory FS.
class FaultyWritableFile final : public WritableFile {
public:
    FaultyWritableFile(std::unique_ptr<WritableFile> file, std::shared_ptr<WriteFaults> faults)
            : _file(std::move(file)), _faults(std::move(faults)) {}
    ~FaultyWritableFile() override { (void)close(); }

    Status append(const Slice& data) override {
        ++_faults->append_calls;
        if (_faults->append_error) {
            return Status::IOError("injected append failure");
        }
        if (_faults->short_write && data.size != 0) {
            return _file->append(Slice(data.data, data.size - 1));
        }
        return _file->append(data);
    }
    Status appendv(const Slice* data, size_t count) override {
        for (size_t i = 0; i < count; ++i) {
            RETURN_IF_ERROR(append(data[i]));
        }
        return Status::OK();
    }
    Status pre_allocate(uint64_t size) override { return _file->pre_allocate(size); }
    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        _closed = true;
        ++_faults->close_calls;
        auto status = _file->close();
        return _faults->close_error ? Status::IOError("injected close failure") : status;
    }
    Status flush(FlushMode mode) override {
        ++_faults->flush_calls;
        EXPECT_EQ(FLUSH_SYNC, mode);
        return _faults->flush_error ? Status::IOError("injected flush failure") : _file->flush(mode);
    }
    Status sync() override { return _file->sync(); }
    uint64_t size() const override { return _file->size(); }
    const std::string& filename() const override { return _file->filename(); }

private:
    std::unique_ptr<WritableFile> _file;
    std::shared_ptr<WriteFaults> _faults;
    bool _closed = false;
};

class FaultInjectingFileSystem final : public FileSystem {
public:
    explicit FaultInjectingFileSystem(FileSystem* fs) : _fs(fs) {}

    Type type() const override { return reported_type; }
    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& options,
                                                                 const std::string& path) override {
        return _fs->new_sequential_file(options, path);
    }
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& options,
                                                                     const std::string& path) override {
        return _fs->new_random_access_file(options, path);
    }
    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override {
        return new_writable_file(WritableFileOptions{}, path);
    }
    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& options,
                                                             const std::string& path) override {
        ++create_calls;
        last_mode = options.mode;
        EXPECT_TRUE(options.sync_on_close);
        if (create_error) {
            return Status::IOError("injected create failure");
        }
        if (create_racing_file) {
            auto racing_file = _fs->new_writable_file(path);
            if (!racing_file.ok()) {
                return racing_file.status();
            }
            RETURN_IF_ERROR(racing_file.value()->append("racing owner"));
            RETURN_IF_ERROR(racing_file.value()->close());
        }
        auto result = _fs->new_writable_file(options, path);
        if (!result.ok()) {
            return result.status();
        }
        return std::make_unique<FaultyWritableFile>(std::move(result).value(), faults);
    }
    Status path_exists(const std::string& path) override { return _fs->path_exists(path); }
    Status get_children(const std::string& path, std::vector<std::string>* result) override {
        return _fs->get_children(path, result);
    }
    Status iterate_dir(const std::string& path, const std::function<bool(std::string_view)>& cb) override {
        return _fs->iterate_dir(path, cb);
    }
    Status iterate_dir2(const std::string& path, const std::function<bool(DirEntry)>& cb) override {
        return _fs->iterate_dir2(path, cb);
    }
    Status delete_file(const std::string& path) override {
        ++delete_calls;
        if (path == delete_error_path) {
            return Status::IOError("injected delete failure");
        }
        return _fs->delete_file(path);
    }
    Status create_dir(const std::string& path) override { return _fs->create_dir(path); }
    Status create_dir_if_missing(const std::string& path, bool* created) override {
        return _fs->create_dir_if_missing(path, created);
    }
    Status create_dir_recursive(const std::string& path) override {
        return mkdir_error ? Status::IOError("injected mkdir failure") : _fs->create_dir_recursive(path);
    }
    Status delete_dir(const std::string& path) override { return _fs->delete_dir(path); }
    Status delete_dir_recursive(const std::string& path) override {
        ++recursive_delete_calls;
        return _fs->delete_dir_recursive(path);
    }
    Status sync_dir(const std::string& path) override { return _fs->sync_dir(path); }
    StatusOr<bool> is_directory(const std::string& path) override {
        if (exists_error) {
            return Status::IOError("injected existence failure");
        }
        return _fs->is_directory(path);
    }
    Status canonicalize(const std::string& path, std::string* result) override {
        return _fs->canonicalize(path, result);
    }
    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        auto is_dir = _fs->is_directory(path);
        if (is_dir.ok() && is_dir.value()) {
            ++directory_size_calls;
            return Status::IOError("directory has no file size");
        }
        return _fs->get_file_size(path);
    }
    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return _fs->get_file_modified_time(path);
    }
    Status rename_file(const std::string& src, const std::string& dst) override {
        ++rename_calls;
        return rename_error ? Status::IOError("injected rename failure") : _fs->rename_file(src, dst);
    }
    Status link_file(const std::string& src, const std::string& dst) override { return _fs->link_file(src, dst); }

    std::shared_ptr<WriteFaults> faults = std::make_shared<WriteFaults>();
    Type reported_type = POSIX;
    OpenMode last_mode = MUST_EXIST;
    bool create_error = false;
    bool create_racing_file = false;
    bool exists_error = false;
    bool mkdir_error = false;
    bool rename_error = false;
    std::string delete_error_path;
    int create_calls = 0;
    int rename_calls = 0;
    int delete_calls = 0;
    int recursive_delete_calls = 0;
    int directory_size_calls = 0;

private:
    FileSystem* _fs;
};

class PaimonFileSystemWriteTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/paimon_fs_write_test.XXXXXX";
        auto* directory = ::mkdtemp(path);
        ASSERT_NE(nullptr, directory);
        _root = directory;
        _local_fs = FileSystem::Default();
        _fs = std::make_unique<FaultInjectingFileSystem>(_local_fs);
        _tracker = std::make_shared<PaimonWriteFileTracker>(_fs.get());
        _adapter = PaimonFileSystem::create_for_write(_tracker);
        ASSERT_NE(nullptr, _adapter);
    }
    void TearDown() override {
        _adapter.reset();
        _tracker.reset();
        _fs.reset();
        if (_local_fs != nullptr && !_root.empty()) {
            EXPECT_OK(_local_fs->delete_dir_recursive(_root));
        }
    }
    std::string path(const std::string& name) const { return _root + "/" + name; }
    void create_preexisting(const std::string& name, const std::string& content) {
        ASSIGN_OR_ABORT(auto file, _local_fs->new_writable_file(path(name)));
        ASSERT_OK(file->append(content));
        ASSERT_OK(file->close());
    }
    void expect_content(const std::string& name, const std::string& expected) {
        std::string actual;
        auto status = _adapter->ReadFile(path(name), &actual);
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(expected, actual);
    }

    std::string _root;
    FileSystem* _local_fs = nullptr;
    std::unique_ptr<FaultInjectingFileSystem> _fs;
    std::shared_ptr<PaimonWriteFileTracker> _tracker;
    std::shared_ptr<PaimonFileSystem> _adapter;
};

TEST_F(PaimonFileSystemWriteTest, RoundTripWithoutDataCacheAndIdempotentClose) {
    EXPECT_FALSE(_adapter->datacache_enabled());
    ASSERT_TRUE(_adapter->Mkdirs(path("child/nested")).ok());
    ASSERT_TRUE(_adapter->Mkdirs(path("child/nested")).ok());
    auto result = _adapter->Create(path("child/nested/data"), false);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    auto output = std::move(result).value();
    ASSERT_TRUE(output->GetPos().ok());
    EXPECT_EQ(0, output->GetPos().value());
    EXPECT_EQ(path("child/nested/data"), output->GetUri().value());
    EXPECT_TRUE(output->Write(nullptr, -1).status().IsInvalid());
    EXPECT_TRUE(output->Write(nullptr, 1).status().IsInvalid());
    ASSERT_TRUE(output->Write(nullptr, 0).ok());
    EXPECT_EQ(0, _fs->faults->append_calls);
    auto written = output->Write("abcd", 4);
    ASSERT_TRUE(written.ok()) << written.status().ToString();
    EXPECT_EQ(4, written.value());
    ASSERT_TRUE(output->Flush().ok());
    ASSERT_TRUE(output->Write("ef", 2).ok());
    EXPECT_EQ(6, output->GetPos().value());
    EXPECT_TRUE(output->Close().ok());
    EXPECT_TRUE(output->Close().ok());
    EXPECT_TRUE(output->Flush().ok());
    EXPECT_EQ(6, output->GetPos().value());
    EXPECT_TRUE(output->Write("g", 1).status().IsInvalid());
    output.reset();
    EXPECT_EQ(1, _fs->faults->close_calls);
    EXPECT_EQ(1, _fs->faults->flush_calls);
    EXPECT_EQ(FileSystem::MUST_CREATE, _fs->last_mode);
    expect_content("child/nested/data", "abcdef");
    EXPECT_EQ(6, _adapter->get_stats().sequential_read_bytes);
    EXPECT_EQ(std::vector<std::string>{path("child/nested/data")}, _tracker->owned_files());
    ASSERT_OK(_tracker->rollback());
    EXPECT_TRUE(_local_fs->path_exists(path("child/nested/data")).is_not_found());
    EXPECT_OK(_local_fs->path_exists(path("child/nested")));
    EXPECT_EQ(0, _fs->recursive_delete_calls);
}

TEST_F(PaimonFileSystemWriteTest, PreservesReadOnlyConstructor) {
    create_preexisting("old", "old data");
    PaimonFileSystem reader(_fs.get(), DataCacheOptions{});
    EXPECT_TRUE(reader.Create(path("new"), false).status().IsNotImplemented());
    EXPECT_TRUE(reader.Create(path("old"), true).status().IsNotImplemented());
    EXPECT_TRUE(reader.Mkdirs(path("dir")).IsNotImplemented());
    EXPECT_TRUE(reader.Rename(path("old"), path("new")).IsNotImplemented());
    EXPECT_TRUE(reader.Delete(path("old"), true).IsNotImplemented());
    EXPECT_TRUE(reader.WriteFile(path("new"), "data", false).IsNotImplemented());
    EXPECT_TRUE(reader.AtomicStore(path("new"), "data").IsNotImplemented());
    EXPECT_EQ(0, _fs->create_calls);
    expect_content("old", "old data");
}

TEST_F(PaimonFileSystemWriteTest, ExclusiveCreateAndOnlyOwnedOverwrite) {
    create_preexisting("old", "must survive");
    EXPECT_TRUE(_adapter->Create(path("old"), false).status().IsExist());
    EXPECT_TRUE(_adapter->Create(path("old"), true).status().IsExist());
    EXPECT_TRUE(_tracker->owned_files().empty());
    EXPECT_TRUE(_adapter->Delete(path("old"), false).IsInvalid());
    auto result = _adapter->Create(path("new"), true);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    auto output = std::move(result).value();
    EXPECT_EQ(FileSystem::MUST_CREATE, _fs->last_mode);
    EXPECT_TRUE(_adapter->Create(path("new"), true).status().IsInvalid());
    ASSERT_TRUE(output->Write("long data", 9).ok());
    ASSERT_TRUE(output->Close().ok());
    EXPECT_TRUE(_adapter->Create(path("new"), false).status().IsExist());
    ASSERT_TRUE(_adapter->WriteFile(path("new"), "x", true).ok());
    EXPECT_EQ(FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE, _fs->last_mode);
    expect_content("new", "x");
    ASSERT_OK(_tracker->rollback());
    ASSERT_OK(_tracker->rollback());
    expect_content("old", "must survive");
    EXPECT_TRUE(_adapter->Create(path("after-rollback"), false).status().IsInvalid());
}

TEST_F(PaimonFileSystemWriteTest, MustCreateDoesNotTruncateAFileCreatedAfterPrecheckOnLocalFs) {
    _fs->create_racing_file = true;
    auto result = _adapter->Create(path("raced"), false);
    EXPECT_TRUE(result.status().IsExist()) << result.status().ToString();
    EXPECT_EQ(FileSystem::MUST_CREATE, _fs->last_mode);
    EXPECT_TRUE(_tracker->owned_files().empty());
    EXPECT_TRUE(_adapter->Delete(path("raced"), true).IsInvalid());
    ASSERT_OK(_tracker->rollback());
    expect_content("raced", "racing owner");
}

TEST_F(PaimonFileSystemWriteTest, ShortWriteAndAppendFailureRemainErrorsThroughClose) {
    for (bool short_write : {true, false}) {
        _fs->faults->short_write = short_write;
        _fs->faults->append_error = !short_write;
        auto result = _adapter->Create(path(short_write ? "short" : "failed"), false);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        auto output = std::move(result).value();
        auto written = output->Write("abc", 3);
        ASSERT_FALSE(written.ok());
        EXPECT_TRUE(written.status().IsIOError());
        const auto error = written.status().ToString();
        EXPECT_TRUE(output->GetPos().status().IsIOError());
        EXPECT_EQ(error, output->Flush().ToString());
        EXPECT_EQ(error, output->Write("x", 1).status().ToString());
        EXPECT_EQ(error, output->Close().ToString());
        EXPECT_EQ(error, output->Close().ToString());
        EXPECT_FALSE(_tracker->mark_published(_tracker->owned_files()).ok());
    }
    EXPECT_EQ(2, _fs->faults->append_calls);
    EXPECT_EQ(2, _fs->faults->close_calls);
    ASSERT_OK(_tracker->rollback());
    EXPECT_TRUE(_tracker->owned_files().empty());
}

TEST_F(PaimonFileSystemWriteTest, FlushAndCloseFailuresAreNotDiscarded) {
    _fs->faults->flush_error = true;
    _fs->faults->close_error = true;
    auto result = _adapter->Create(path("flush-failed"), false);
    ASSERT_TRUE(result.ok());
    auto output = std::move(result).value();
    ASSERT_TRUE(output->Write("data", 4).ok());
    EXPECT_TRUE(output->Flush().IsIOError());
    auto status = output->Close();
    EXPECT_TRUE(status.IsIOError());
    EXPECT_NE(std::string::npos, status.ToString().find("injected flush failure"));
    EXPECT_NE(std::string::npos, status.ToString().find("injected close failure"));
    EXPECT_EQ(status.ToString(), output->Close().ToString());
    EXPECT_EQ(1, _fs->faults->close_calls);
    _fs->faults->flush_error = false;
    status = _adapter->WriteFile(path("close-failed"), "data", false);
    EXPECT_TRUE(status.IsIOError());
    EXPECT_NE(std::string::npos, status.ToString().find("injected close failure"));
    EXPECT_FALSE(_tracker->mark_published(_tracker->owned_files()).ok());
    ASSERT_OK(_tracker->rollback());
}

TEST_F(PaimonFileSystemWriteTest, DestructorClosesButDoesNotPublish) {
    auto result = _adapter->Create(path("unclosed"), false);
    ASSERT_TRUE(result.ok());
    auto output = std::move(result).value();
    ASSERT_TRUE(output->Write("data", 4).ok());
    output.reset();
    EXPECT_EQ(1, _fs->faults->close_calls);
    EXPECT_EQ(std::vector<std::string>{path("unclosed")}, _tracker->owned_files());
    ASSERT_OK(_tracker->rollback());
    EXPECT_TRUE(_local_fs->path_exists(path("unclosed")).is_not_found());
}

TEST_F(PaimonFileSystemWriteTest, ObjectStoreFlushFinalizesOnceAndRejectsMoreWrites) {
    // Local IO with S3 lifecycle semantics; no cloud client or network involved.
    _fs->reported_type = FileSystem::S3;
    auto result = _adapter->Create(path("object"), false);
    ASSERT_TRUE(result.ok());
    auto output = std::move(result).value();
    ASSERT_TRUE(output->Write("data", 4).ok());
    ASSERT_TRUE(output->Flush().ok());
    EXPECT_EQ(4, output->GetPos().value());
    EXPECT_TRUE(output->Write("lost", 4).status().IsInvalid());
    EXPECT_TRUE(output->Flush().ok());
    EXPECT_TRUE(output->Close().ok());
    EXPECT_EQ(1, _fs->faults->close_calls);
    EXPECT_EQ(0, _fs->faults->flush_calls);
    EXPECT_TRUE(_adapter->AtomicStore(path("metadata"), "not atomic").IsNotImplemented());
    expect_content("object", "data");
}

TEST_F(PaimonFileSystemWriteTest, PublicationIsValidatedAllOrNothingAndProtectsFiles) {
    ASSERT_TRUE(_adapter->WriteFile(path("published"), "keep", false).ok());
    auto result = _adapter->Create(path("unpublished"), false);
    ASSERT_TRUE(result.ok());
    auto output = std::move(result).value();
    EXPECT_FALSE(_tracker->mark_published({path("published"), path("unpublished")}).ok());
    EXPECT_EQ(2, _tracker->owned_files().size());
    ASSERT_TRUE(output->Close().ok());
    ASSERT_OK(_tracker->mark_published({path("published")}));
    ASSERT_OK(_tracker->mark_published({path("published")}));
    EXPECT_TRUE(_adapter->Delete(path("published"), true).IsInvalid());
    EXPECT_TRUE(_adapter->Create(path("published"), true).status().IsExist());
    EXPECT_TRUE(_adapter->Rename(path("published"), path("moved")).IsInvalid());
    ASSERT_OK(_tracker->rollback());
    expect_content("published", "keep");
    EXPECT_TRUE(_local_fs->path_exists(path("unpublished")).is_not_found());
    EXPECT_FALSE(_tracker->mark_published({path("published")}).ok());
}

TEST_F(PaimonFileSystemWriteTest, RenamePreservesOwnershipAndNeverReplacesExistingPaths) {
    create_preexisting("old", "keep");
    ASSERT_TRUE(_adapter->WriteFile(path("source"), "move me", false).ok());
    EXPECT_TRUE(_adapter->Rename(path("old"), path("other")).IsInvalid());
    EXPECT_TRUE(_adapter->Rename(path("source"), path("old")).IsExist());
    _fs->rename_error = true;
    auto status = _adapter->Rename(path("source"), path("target"));
    EXPECT_TRUE(status.IsIOError());
    EXPECT_NE(std::string::npos, status.ToString().find("injected rename failure"));
    EXPECT_EQ(std::vector<std::string>{path("source")}, _tracker->owned_files());
    _fs->rename_error = false;
    ASSERT_TRUE(_adapter->Rename(path("source"), path("target")).ok());
    EXPECT_EQ(std::vector<std::string>{path("target")}, _tracker->owned_files());
    EXPECT_TRUE(_adapter->Rename(path("target"), path("target")).ok());
    ASSERT_TRUE(_adapter->Delete(path("source"), true).ok());
    expect_content("target", "move me");
    ASSERT_OK(_tracker->rollback());
    EXPECT_TRUE(_local_fs->path_exists(path("target")).is_not_found());
    expect_content("old", "keep");
}

TEST_F(PaimonFileSystemWriteTest, DeleteAndRollbackAreBoundedRetryableAndIdempotent) {
    create_preexisting("old", "keep");
    ASSERT_TRUE(_adapter->WriteFile(path("a-failed"), "a", false).ok());
    ASSERT_TRUE(_adapter->WriteFile(path("z-deleted"), "z", false).ok());
    EXPECT_TRUE(_adapter->Delete(_root, true).IsInvalid());
    EXPECT_TRUE(_adapter->Delete(path("old"), true).IsInvalid());
    _fs->delete_error_path = path("a-failed");
    EXPECT_TRUE(_adapter->Delete(path("a-failed"), true).IsIOError());
    auto status = _tracker->rollback();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("injected delete failure"));
    EXPECT_EQ(std::vector<std::string>{path("a-failed")}, _tracker->owned_files());
    EXPECT_TRUE(_local_fs->path_exists(path("z-deleted")).is_not_found());
    _fs->delete_error_path.clear();
    ASSERT_OK(_tracker->rollback());
    auto delete_calls = _fs->delete_calls;
    ASSERT_OK(_tracker->rollback());
    EXPECT_TRUE(_adapter->Delete(path("a-failed"), true).ok());
    EXPECT_EQ(delete_calls, _fs->delete_calls);
    EXPECT_EQ(0, _fs->recursive_delete_calls);
    expect_content("old", "keep");
}

TEST_F(PaimonFileSystemWriteTest, OpenOutputBlocksCleanupAndPublication) {
    auto result = _adapter->Create(path("active"), false);
    ASSERT_TRUE(result.ok());
    auto output = std::move(result).value();
    EXPECT_TRUE(_adapter->Delete(path("active"), false).IsInvalid());
    EXPECT_FALSE(_tracker->mark_published({path("active")}).ok());
    EXPECT_FALSE(_tracker->rollback().ok());
    EXPECT_EQ(0, _fs->delete_calls);
    ASSERT_TRUE(output->Close().ok());
    ASSERT_OK(_tracker->rollback());
    EXPECT_EQ(1, _fs->delete_calls);
}

TEST_F(PaimonFileSystemWriteTest, CreationAndExistenceErrorsDoNotAcquireOwnership) {
    _fs->exists_error = true;
    auto result = _adapter->Create(path("exists-error"), false);
    EXPECT_TRUE(result.status().IsIOError());
    EXPECT_EQ(0, _fs->create_calls);
    _fs->exists_error = false;
    _fs->create_error = true;
    result = _adapter->Create(path("create-error"), false);
    EXPECT_TRUE(result.status().IsIOError());
    EXPECT_NE(std::string::npos, result.status().ToString().find("injected create failure"));
    EXPECT_TRUE(_tracker->owned_files().empty());
    EXPECT_TRUE(_adapter->Delete(path("create-error"), false).IsInvalid());
    _fs->mkdir_error = true;
    EXPECT_TRUE(_adapter->Mkdirs(path("dir")).IsIOError());
    EXPECT_EQ(0, _fs->delete_calls);
}

TEST_F(PaimonFileSystemWriteTest, FailedOwnedOverwriteCannotPublishEarlierClosedVersion) {
    ASSERT_TRUE(_adapter->WriteFile(path("owned"), "old data", false).ok());
    _fs->create_error = true;
    EXPECT_TRUE(_adapter->Create(path("owned"), true).status().IsIOError());
    EXPECT_FALSE(_tracker->mark_published({path("owned")}).ok());
    EXPECT_EQ(std::vector<std::string>{path("owned")}, _tracker->owned_files());
    ASSERT_OK(_tracker->rollback());
    EXPECT_TRUE(_local_fs->path_exists(path("owned")).is_not_found());
}

TEST_F(PaimonFileSystemWriteTest, CleanupTreatsAlreadyMissingOwnedFilesAsSuccess) {
    ASSERT_TRUE(_adapter->WriteFile(path("sdk-delete"), "data", false).ok());
    ASSERT_TRUE(_adapter->WriteFile(path("rollback-delete"), "data", false).ok());
    ASSERT_OK(_local_fs->delete_file(path("sdk-delete")));
    ASSERT_OK(_local_fs->delete_file(path("rollback-delete")));
    EXPECT_TRUE(_adapter->Delete(path("sdk-delete"), true).ok());
    ASSERT_OK(_tracker->rollback());
    EXPECT_TRUE(_tracker->owned_files().empty());
}

TEST_F(PaimonFileSystemWriteTest, SharedTrackerRejectsDuplicateAndCannotResurrectDeletedPath) {
    auto second_adapter = PaimonFileSystem::create_for_write(_tracker);
    ASSERT_NE(nullptr, second_adapter);
    ASSERT_TRUE(_adapter->WriteFile(path("shared"), "data", false).ok());
    EXPECT_TRUE(second_adapter->Create(path("shared"), false).status().IsExist());
    ASSERT_TRUE(second_adapter->Delete(path("shared"), false).ok());
    create_preexisting("shared", "different owner");
    EXPECT_TRUE(_adapter->Delete(path("shared"), false).ok());
    EXPECT_TRUE(_adapter->Create(path("shared"), true).status().IsExist());
    expect_content("shared", "different owner");
    ASSERT_OK(_tracker->rollback());
    expect_content("shared", "different owner");
}

TEST_F(PaimonFileSystemWriteTest, DirectoryStatusDoesNotRequestFileSize) {
    auto result = _adapter->GetFileStatus(_root);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_TRUE(result.value()->IsDir());
    EXPECT_EQ(0, result.value()->GetLen());
    EXPECT_EQ(0, _fs->directory_size_calls);
    EXPECT_EQ(nullptr, PaimonFileSystem::create_for_write(nullptr));
    EXPECT_EQ(nullptr, PaimonFileSystem::create_for_write(std::make_shared<PaimonWriteFileTracker>(nullptr)));
}

} // namespace
} // namespace starrocks
