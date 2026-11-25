// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <filesystem>
#include <ranges>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "common/system/cpu_info.h"
#include "common/util/thrift_client.h"
#include "exec/exec_env.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "io/string_input_stream.h"

#define private public // hack complier
#define protected public

#include "data_workflows/snapshot/snapshot_loader.h"

#undef protected
#undef private

namespace starrocks {

class SnapshotLoaderTest : public testing::Test {
public:
    SnapshotLoaderTest() = default;

private:
    ExecEnv* _exec_env;
};

TEST_F(SnapshotLoaderTest, NormalCase) {
    SnapshotLoader loader(_exec_env, 1L, 2L);

    ASSERT_TRUE(loader._end_with("abt.dat", ".dat"));
    ASSERT_FALSE(loader._end_with("abt.dat", ".da"));

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    Status st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678", &tablet_id, &schema_hash);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1234, tablet_id);
    ASSERT_EQ(5678, schema_hash);

    st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678/", &tablet_id, &schema_hash);
    ASSERT_FALSE(st.ok());

    std::filesystem::remove_all("./ss_test/");
    std::map<std::string, std::string> src_to_dest;
    src_to_dest["./ss_test/"] = "./ss_test";
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    ASSERT_FALSE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    ASSERT_FALSE(st.ok());

    std::filesystem::create_directory("./ss_test/");
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    ASSERT_TRUE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    ASSERT_TRUE(st.ok());
    std::filesystem::remove_all("./ss_test/");

    std::filesystem::create_directory("./ss_test/");
    std::vector<std::string> files;
    st = loader._get_existing_files_from_local("./ss_test/", &files);
    ASSERT_EQ(0, files.size());
    std::filesystem::remove_all("./ss_test/");

    std::string snapshot_file;
    std::string tablet_file;
    loader._assemble_file_name("/snapshot/path", "/tablet/path", 1234, 2, 5, 12345, 1, ".dat", &snapshot_file,
                               &tablet_file);
    ASSERT_EQ("/snapshot/path/1234_2_5_12345_1.dat", snapshot_file);
    ASSERT_EQ("/tablet/path/1234_2_5_12345_1.dat", tablet_file);

    std::string new_name;
    st = loader._replace_tablet_id("12345.hdr", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("5678.hdr", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.dat", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.dat", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.upt", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.upt", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.cols", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.cols", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.idx", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.idx", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.xxx", 5678, &new_name);
    ASSERT_FALSE(st.ok());

    st = loader._get_tablet_id_from_remote_path("/__tbl_10004/__part_10003/__idx_10004/__10005", &tablet_id);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(10005, tablet_id);
}

TEST_F(SnapshotLoaderTest, SnapshotManifestReadWrite) {
    SnapshotLoader loader(_exec_env, 1L, 2L);
    FileSystem* fs = FileSystem::Default();

    const std::string dir = "./ss_manifest_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);

    // no manifest in the dir: reading it is an error, callers check the listing first
    {
        std::vector<std::string> names;
        ASSERT_FALSE(loader._read_snapshot_manifest(fs, dir, &names).ok());
    }

    // write a manifest then read it back verbatim, order preserved
    {
        std::vector<std::string> to_write = {"0_0_0.dat", "0.hdr", "0_0_1.dat"};
        Status st = loader._write_snapshot_manifest(fs, dir, to_write);
        ASSERT_TRUE(st.ok()) << st.message();

        std::vector<std::string> names;
        st = loader._read_snapshot_manifest(fs, dir, &names);
        ASSERT_TRUE(st.ok()) << st.message();
        ASSERT_EQ(to_write, names);
    }

    // the manifest file name carries no '.', so the remote listing (which keys files by the part
    // before the last '.') never mistakes it for a data file
    ASSERT_EQ(std::string::npos, std::string("__starrocks_snapshot_manifest").find('.'));

    // an empty file list round-trips as a present-but-empty manifest
    {
        Status st = loader._write_snapshot_manifest(fs, dir, {});
        ASSERT_TRUE(st.ok()) << st.message();

        std::vector<std::string> names;
        st = loader._read_snapshot_manifest(fs, dir, &names);
        ASSERT_TRUE(st.ok()) << st.message();
        ASSERT_TRUE(names.empty());
    }

    // a manifest whose last line has no trailing newline still parses every name
    {
        const std::string manifest_path = dir + "/__starrocks_snapshot_manifest";
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wf_or = fs->new_writable_file(opts, manifest_path);
        ASSERT_TRUE(wf_or.ok()) << wf_or.status().message();
        auto wf = std::move(wf_or.value());
        ASSERT_TRUE(wf->append(Slice("a.dat\nb.dat")).ok());
        ASSERT_TRUE(wf->close().ok());

        std::vector<std::string> names;
        Status st = loader._read_snapshot_manifest(fs, dir, &names);
        ASSERT_TRUE(st.ok()) << st.message();
        ASSERT_EQ((std::vector<std::string>{"a.dat", "b.dat"}), names);
    }

    std::filesystem::remove_all(dir);
}

TEST_F(SnapshotLoaderTest, VerifySnapshotManifest) {
    SnapshotLoader loader(_exec_env, 1L, 2L);
    FileSystem* fs = FileSystem::Default();

    const std::string dir = "./ss_verify_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);

    std::map<std::string, FileStat> remote_files;
    remote_files.emplace("0_0_0.dat", FileStat{"0_0_0.dat", "0123456789abcdef0123456789abcdef", 100});
    remote_files.emplace("0.hdr", FileStat{"0.hdr", "fedcba9876543210fedcba9876543210", 50});

    // no manifest present -> ok (legacy snapshot, nothing to verify)
    {
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files, false);
        ASSERT_TRUE(st.ok()) << st.message();
    }

    // manifest lists exactly the files present in the remote listing -> ok
    {
        ASSERT_TRUE(loader._write_snapshot_manifest(fs, dir, {"0_0_0.dat", "0.hdr"}).ok());
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files, true);
        ASSERT_TRUE(st.ok()) << st.message();
    }

    // manifest lists a file missing from the remote listing -> error
    {
        ASSERT_TRUE(loader._write_snapshot_manifest(fs, dir, {"0_0_0.dat", "0.hdr", "0_0_1.dat"}).ok());
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files, true);
        ASSERT_FALSE(st.ok());
        ASSERT_NE(st.message().find("missing"), std::string::npos);
    }

    // an empty manifest records no files -> nothing to check, ok
    {
        ASSERT_TRUE(loader._write_snapshot_manifest(fs, dir, {}).ok());
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files, true);
        ASSERT_TRUE(st.ok()) << st.message();
    }

    std::filesystem::remove_all(dir);
}

// Opening a missing file is NotFound only on a local filesystem. HDFS and S3 open lazily and fail on the
// first read (RemoteFileNotFound / IOError); a broker fails the open with a storage service error.
class RemoteRepoFileSystem final : public MemoryFileSystem {
public:
    enum class Mode { kFailOnRead, kFailOnOpen };

    explicit RemoteRepoFileSystem(Mode mode) : _mode(mode) {}

    using MemoryFileSystem::new_sequential_file;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override {
        ++open_count;
        if (path_exists(path).ok()) {
            return MemoryFileSystem::new_sequential_file(opts, path);
        }
        if (_mode == Mode::kFailOnOpen) {
            return Status::InternalError("Broker storage service error, could not open file " + path);
        }
        return SequentialFile::from(std::make_unique<FailOnReadStream>(), path, FileEncryptionInfo());
    }

    int open_count = 0;

private:
    class FailOnReadStream final : public io::StringInputStream {
    public:
        FailOnReadStream() : io::StringInputStream("") {}
        StatusOr<int64_t> read(void* /*data*/, int64_t /*count*/) override {
            return Status::RemoteFileNotFound("hdfsOpenFile failed");
        }
    };

    Mode _mode;
};

// Restoring a snapshot taken before the manifest existed lists the tablet dir, finds no manifest and keeps
// the legacy behavior without trying to open it; a manifest in the listing is read and enforced.
TEST_F(SnapshotLoaderTest, VerifyManifestOnRemoteRepository) {
    SnapshotLoader loader(_exec_env, 1L, 2L);
    for (auto mode : {RemoteRepoFileSystem::Mode::kFailOnRead, RemoteRepoFileSystem::Mode::kFailOnOpen}) {
        auto* repo = new RemoteRepoFileSystem(mode);
        std::unique_ptr<FileSystem> fs(repo);
        const std::string dir = "/repo/__ss_content/__10651";
        ASSERT_OK(fs->create_dir_recursive(dir));
        // uploaded files carry their md5 after the last '.'
        ASSERT_OK(repo->append_file(dir + "/0.hdr.fedcba9876543210fedcba9876543210", "hdr"));
        ASSERT_OK(repo->append_file(dir + "/0_0_0.dat.0123456789abcdef0123456789abcdef", "dat"));

        // legacy snapshot: the listing has no manifest, so verification passes without opening it
        std::map<std::string, FileStat> remote_files;
        bool has_manifest = true;
        ASSERT_OK(loader._get_existing_files_from_remote_without_broker(fs, dir, &remote_files, &has_manifest));
        ASSERT_FALSE(has_manifest);
        ASSERT_EQ(2, remote_files.size());
        ASSERT_OK(loader._verify_snapshot_manifest(fs.get(), dir, remote_files, has_manifest));
        ASSERT_EQ(0, repo->open_count);
        // probing the missing manifest by name is not NotFound here, which is why the listing decides
        std::vector<std::string> names;
        Status probe = loader._read_snapshot_manifest(fs.get(), dir, &names);
        ASSERT_FALSE(probe.ok());
        ASSERT_FALSE(probe.is_not_found());

        // new snapshot: the listing shows the manifest (not as a data file) and every recorded file is there
        ASSERT_OK(loader._write_snapshot_manifest(fs.get(), dir, {"0.hdr", "0_0_0.dat"}));
        remote_files.clear();
        ASSERT_OK(loader._get_existing_files_from_remote_without_broker(fs, dir, &remote_files, &has_manifest));
        ASSERT_TRUE(has_manifest);
        ASSERT_EQ(2, remote_files.size());
        ASSERT_OK(loader._verify_snapshot_manifest(fs.get(), dir, remote_files, has_manifest));

        // a file recorded in the manifest is gone from the repository
        ASSERT_OK(fs->delete_file(dir + "/0_0_0.dat.0123456789abcdef0123456789abcdef"));
        remote_files.clear();
        ASSERT_OK(loader._get_existing_files_from_remote_without_broker(fs, dir, &remote_files, &has_manifest));
        Status st = loader._verify_snapshot_manifest(fs.get(), dir, remote_files, has_manifest);
        ASSERT_FALSE(st.ok());
        ASSERT_NE(st.message().find("missing"), std::string::npos);

        // the listing showed the manifest but it cannot be read: an error, not a legacy snapshot
        ASSERT_OK(fs->delete_file(dir + "/__starrocks_snapshot_manifest"));
        ASSERT_FALSE(loader._verify_snapshot_manifest(fs.get(), dir, remote_files, true).ok());
    }
}

// A broker that answers every listPath with `files`.
class ListingBrokerClient final : public TFileBrokerServiceClient {
public:
    using TBinaryProtocol = apache::thrift::protocol::TBinaryProtocol;
    using TBufferedTransport = apache::thrift::transport::TBufferedTransport;
    using TSocket = apache::thrift::transport::TSocket;

    ListingBrokerClient()
            : TFileBrokerServiceClient(std::make_shared<TBinaryProtocol>(
                      std::make_shared<TBufferedTransport>(std::make_shared<TSocket>("127.0.0.1", 12345)))) {}

    void listPath(TBrokerListResponse& response, const TBrokerListPathRequest& /*request*/) override {
        response.opStatus.__set_statusCode(TBrokerOperationStatusCode::OK);
        response.__set_files(files);
    }

    void add_file(const std::string& name) {
        TBrokerFileStatus file;
        file.__set_path(name);
        file.__set_isDir(false);
        file.__set_size(3);
        files.push_back(file);
    }

    std::vector<TBrokerFileStatus> files;
};

// Through a broker, the listing reports the manifest, and never as a data file.
TEST_F(SnapshotLoaderTest, ListManifestThroughBroker) {
    SnapshotLoader loader(_exec_env, 1L, 2L);
    ListingBrokerClient broker;
    BrokerServiceConnection client;
    client._client = &broker;
    // the test-only connection has no client cache to release the client to
    DeferOp detach([&] { client._client = nullptr; });

    const std::string dir = "/repo/__ss_content/__10651";
    broker.add_file("0.hdr.fedcba9876543210fedcba9876543210");
    broker.add_file("0_0_0.dat.0123456789abcdef0123456789abcdef");
    std::map<std::string, FileStat> remote_files;
    bool has_manifest = true;
    ASSERT_OK(loader._get_existing_files_from_remote(client, dir, {}, &remote_files, &has_manifest));
    ASSERT_FALSE(has_manifest);
    ASSERT_EQ(2, remote_files.size());

    broker.add_file("__starrocks_snapshot_manifest");
    remote_files.clear();
    ASSERT_OK(loader._get_existing_files_from_remote(client, dir, {}, &remote_files, &has_manifest));
    ASSERT_TRUE(has_manifest);
    ASSERT_EQ(2, remote_files.size());
    ASSERT_EQ(0, remote_files.count("__starrocks_snapshot_manifest"));
}

// Restoring without a broker, download() enforces the manifest of the listed tablet dir before it touches the
// local tablet.
TEST_F(SnapshotLoaderTest, DownloadRejectsFileMissingFromManifestWithoutBroker) {
    SnapshotLoader loader(_exec_env, 1L, 2L);
    const std::string root = std::filesystem::absolute("./ss_download_test").string();
    std::filesystem::remove_all(root);
    DeferOp cleanup([&] { std::filesystem::remove_all(root); });
    const std::string local_dir = root + "/local/10651/12345";
    const std::string remote_dir = root + "/repo/__ss_content/__10651";
    std::filesystem::create_directories(local_dir);
    std::filesystem::create_directories(remote_dir);

    FileSystem* fs = FileSystem::Default();
    for (const char* name : {"0.hdr.fedcba9876543210fedcba9876543210", "0_0_0.dat.0123456789abcdef0123456789abcdef"}) {
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(remote_dir + "/" + name));
        ASSERT_OK(file->append("x"));
        ASSERT_OK(file->close());
    }
    ASSERT_OK(loader._write_snapshot_manifest(fs, remote_dir, {"0.hdr", "0_0_0.dat", "0_0_1.dat"}));

    TDownloadReq req;
    req.__set_use_broker(false);
    std::vector<int64_t> downloaded_tablet_ids;
    Status st = loader.download({{remote_dir, local_dir}}, req, &downloaded_tablet_ids);
    ASSERT_FALSE(st.ok());
    ASSERT_NE(st.message().find("missing"), std::string::npos) << st.message();
}

} // namespace starrocks
