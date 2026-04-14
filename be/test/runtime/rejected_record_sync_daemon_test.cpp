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

#include "runtime/rejected_record_sync_daemon.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/status.h"

namespace starrocks {

namespace {

// Test-only subclass that overrides the two methods that talk to the
// filesystem / network so we can exercise the batching logic in isolation.
class TestSyncDaemon : public RejectedRecordSyncDaemon {
public:
    TestSyncDaemon() : RejectedRecordSyncDaemon(/*env=*/nullptr) {}

    // Inject a fixed list of files; production scan_once() walks ExecEnv's
    // store paths which we don't have in a pure unit test.
    void set_scan_result(std::vector<std::string> files) { _scan_result = std::move(files); }

    std::vector<std::string>& captured_payloads() { return _payloads; }

    // Simulate post success (default) or failure.
    void set_post_should_succeed(bool ok) { _post_should_succeed = ok; }

    // Expose flush_batch for direct testing.
    using RejectedRecordSyncDaemon::flush_batch;
    using RejectedRecordSyncDaemon::scan_once;

protected:
    std::vector<std::string> scan_once() override { return _scan_result; }

    Status post_to_stream_load(const std::string& payload) override {
        _payloads.push_back(payload);
        if (!_post_should_succeed) {
            return Status::InternalError("simulated post failure");
        }
        return Status::OK();
    }

private:
    std::vector<std::string> _scan_result;
    std::vector<std::string> _payloads;
    bool _post_should_succeed = true;
};

// Scoped temp directory that cleans itself up at destruction.
class TempDir {
public:
    TempDir() {
        std::error_code ec;
        _path = std::filesystem::temp_directory_path(ec) /
                ("rr_sync_daemon_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(_path, ec);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(_path, ec);
    }

    std::string write_file(const std::string& name, const std::string& content) {
        auto p = _path / name;
        std::ofstream out(p);
        out << content;
        out.close();
        return p.string();
    }

    const std::filesystem::path& path() const { return _path; }

private:
    std::filesystem::path _path;
};

} // namespace

TEST(RejectedRecordSyncDaemonCoreTest, FlushBatchConcatenatesJsonlFilesAndDeletesOnSuccess) {
    TempDir dir;
    std::string f1 = dir.write_file("a.jsonl", "{\"id\":\"u1\"}\n{\"id\":\"u2\"}\n");
    std::string f2 = dir.write_file("b.jsonl", "{\"id\":\"u3\"}\n");

    TestSyncDaemon daemon;
    auto st = daemon.flush_batch({f1, f2});
    EXPECT_TRUE(st.ok()) << st.message();

    ASSERT_EQ(1u, daemon.captured_payloads().size());
    const auto& payload = daemon.captured_payloads()[0];
    // All three lines present, in file order, separated by newlines.
    EXPECT_NE(std::string::npos, payload.find("u1"));
    EXPECT_NE(std::string::npos, payload.find("u2"));
    EXPECT_NE(std::string::npos, payload.find("u3"));
    // Successful flush must delete the source files so the next tick
    // doesn't re-ship them.
    EXPECT_FALSE(std::filesystem::exists(f1));
    EXPECT_FALSE(std::filesystem::exists(f2));
}

TEST(RejectedRecordSyncDaemonCoreTest, FlushBatchKeepsFilesWhenPostFails) {
    TempDir dir;
    std::string f1 = dir.write_file("a.jsonl", "{\"id\":\"u1\"}\n");

    TestSyncDaemon daemon;
    daemon.set_post_should_succeed(false);
    auto st = daemon.flush_batch({f1});
    EXPECT_FALSE(st.ok());

    // File must NOT be deleted so the next tick retries.
    EXPECT_TRUE(std::filesystem::exists(f1));
}

TEST(RejectedRecordSyncDaemonCoreTest, FlushBatchSkipsEmptyFilesWithoutPosting) {
    TempDir dir;
    std::string empty = dir.write_file("empty.jsonl", "");

    TestSyncDaemon daemon;
    auto st = daemon.flush_batch({empty});
    EXPECT_TRUE(st.ok());
    // No payload posted because the batch resolved to zero rows.
    EXPECT_TRUE(daemon.captured_payloads().empty());
    // Empty file is still removed so we don't rescan it forever.
    EXPECT_FALSE(std::filesystem::exists(empty));
}

TEST(RejectedRecordSyncDaemonCoreTest, FlushBatchIgnoresBlankLines) {
    TempDir dir;
    // Blank lines between records should not be shipped as rows.
    std::string f = dir.write_file("mixed.jsonl", "{\"id\":\"u1\"}\n\n\n{\"id\":\"u2\"}\n");

    TestSyncDaemon daemon;
    auto st = daemon.flush_batch({f});
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(1u, daemon.captured_payloads().size());
    // Exactly two lines in the payload (one \n per record, final newline
    // for the second).
    const auto& payload = daemon.captured_payloads()[0];
    int newlines = 0;
    for (char c : payload) {
        if (c == '\n') ++newlines;
    }
    EXPECT_EQ(2, newlines);
}

TEST(RejectedRecordSyncDaemonCoreTest, FlushBatchTreatsMissingFileAsSoftError) {
    TestSyncDaemon daemon;
    auto st = daemon.flush_batch({"/tmp/does/not/exist/rr_sync_test.jsonl"});
    // Missing files resolve to zero rows, which is a successful no-op --
    // we don't want one bad path to poison a whole batch.
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(daemon.captured_payloads().empty());
}

} // namespace starrocks
