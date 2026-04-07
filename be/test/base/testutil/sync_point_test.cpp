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

#include "base/testutil/sync_point.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace starrocks {

#if defined(BE_TEST)
TEST(SyncPointTest, CallbackExceptionDoesNotCorruptLock) {
    auto* sp = SyncPoint::GetInstance();
    sp->ClearAllCallBacks();
    sp->EnableProcessing();

    sp->SetCallBack("sync_point_exception", [](void*) { throw std::runtime_error("boom"); });

    bool caught_expected = false;
    try {
        sp->Process("sync_point_exception");
        FAIL() << "Expected exception not thrown";
    } catch (const std::runtime_error&) {
        caught_expected = true;
    } catch (...) {
        FAIL() << "Unexpected exception type";
    }

    if (caught_expected) {
        sp->ClearAllCallBacks();
    }
    sp->DisableProcessing();
}
#endif

} // namespace starrocks
