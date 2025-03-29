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

#include <atomic>

namespace starrocks {

// set the process exit flag.
// returns:
//  - true: the exit flag is set from `false` to `true`
//  - false: the exit flag is already `true`
bool set_process_exit();

// set the process quick exit flag.
// returns:
//  - true: the quick exit flag is set from `false` to `true`
//  - false: the quick exit flag is already `true`
bool set_process_quick_exit();

// whether the process is in the progress of exiting
// returns:
//  - true: either in exit or quick exit
//  - false: neither exit nor quick exit
bool process_exit_in_progress();

// whether the process is in the progress of quick exiting
// returns:
//  - true: process is in quick exit
//  - false: process is not in quick exit
bool process_quick_exit_in_progress();

} // namespace starrocks
