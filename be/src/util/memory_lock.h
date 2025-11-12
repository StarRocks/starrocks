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

#pragma once

namespace starrocks {
// This function is used to lock the memory of specified modules.
// When memory is locked, it will not be swapped out to disk.
// Therefore, when available memory is low, these executable pages are not frequently swapped out.
// This can prevent the operating system from freezing.
//
// The modules to be locked can be configured through the configuration item "config::sys_mlock_modules".
// These code segments of the modules will be locked in memory.
//
// Under Release mode, they will lock approximately 170MB of memory,
// with no significant memory consumption.
void mlock_modules();
} // namespace starrocks