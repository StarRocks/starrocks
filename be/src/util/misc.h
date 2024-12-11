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

<<<<<<< HEAD
=======
#include <cstdint>
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#include <functional>

namespace starrocks {

// take a sleep with small intervals until time out by `sleep_secs` or the `stop_condition()` is true
<<<<<<< HEAD
void nap_sleep(int32_t sleep_secs, std::function<bool()> stop_condition);
=======
void nap_sleep(int32_t sleep_secs, const std::function<bool()>& stop_condition);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

} // namespace starrocks
