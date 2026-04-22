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

#include "io/core/io_instrumentation.h"

#include <gtest/gtest.h>

namespace starrocks::io {
namespace {

struct HookCapture {
    int read_calls = 0;
    int write_calls = 0;
    int sync_calls = 0;
    int64_t read_bytes = 0;
    int64_t read_latency_ns = 0;
    int64_t write_bytes = 0;
    int64_t write_latency_ns = 0;
    int64_t sync_latency_ns = 0;
};

HookCapture* g_capture = nullptr;

void capture_read(int64_t bytes, int64_t latency_ns) noexcept {
    g_capture->read_calls += 1;
    g_capture->read_bytes = bytes;
    g_capture->read_latency_ns = latency_ns;
}

void capture_write(int64_t bytes, int64_t latency_ns) noexcept {
    g_capture->write_calls += 1;
    g_capture->write_bytes = bytes;
    g_capture->write_latency_ns = latency_ns;
}

void capture_sync(int64_t latency_ns) noexcept {
    g_capture->sync_calls += 1;
    g_capture->sync_latency_ns = latency_ns;
}

const IOEventHooks kCaptureHooks{capture_read, capture_write, capture_sync};

class IOInstrumentationTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_hooks = IOInstrumentation::set_hooks(nullptr);
        g_capture = nullptr;
    }

    void TearDown() override {
        g_capture = nullptr;
        (void)IOInstrumentation::set_hooks(_saved_hooks);
    }

private:
    const IOEventHooks* _saved_hooks = nullptr;
};

} // namespace

TEST_F(IOInstrumentationTest, DefaultStateUsesNoopHooks) {
    const IOEventHooks* default_hooks = IOInstrumentation::get_hooks();
    EXPECT_EQ(default_hooks, IOInstrumentation::set_hooks(nullptr));
    EXPECT_EQ(default_hooks, IOInstrumentation::get_hooks());
}

TEST_F(IOInstrumentationTest, NoopHooksAcceptEvents) {
    EXPECT_EQ(IOInstrumentation::get_hooks(), IOInstrumentation::set_hooks(nullptr));
    IOInstrumentation::record_read(1, 2);
    IOInstrumentation::record_write(3, 4);
    IOInstrumentation::record_sync(5);
}

TEST_F(IOInstrumentationTest, InstalledHooksReceiveEvents) {
    HookCapture capture;
    g_capture = &capture;

    const IOEventHooks* previous = IOInstrumentation::set_hooks(&kCaptureHooks);
    EXPECT_NE(previous, &kCaptureHooks);
    EXPECT_EQ(IOInstrumentation::get_hooks(), &kCaptureHooks);

    IOInstrumentation::record_read(11, 12);
    IOInstrumentation::record_write(21, 22);
    IOInstrumentation::record_sync(31);

    EXPECT_EQ(capture.read_calls, 1);
    EXPECT_EQ(capture.read_bytes, 11);
    EXPECT_EQ(capture.read_latency_ns, 12);
    EXPECT_EQ(capture.write_calls, 1);
    EXPECT_EQ(capture.write_bytes, 21);
    EXPECT_EQ(capture.write_latency_ns, 22);
    EXPECT_EQ(capture.sync_calls, 1);
    EXPECT_EQ(capture.sync_latency_ns, 31);
}

TEST_F(IOInstrumentationTest, RestoringNullRestoresNoopHooks) {
    HookCapture capture;
    g_capture = &capture;

    (void)IOInstrumentation::set_hooks(&kCaptureHooks);

    const IOEventHooks* restored = IOInstrumentation::set_hooks(nullptr);
    EXPECT_EQ(restored, &kCaptureHooks);

    IOInstrumentation::record_read(41, 42);
    EXPECT_EQ(capture.read_calls, 0);
}

} // namespace starrocks::io
