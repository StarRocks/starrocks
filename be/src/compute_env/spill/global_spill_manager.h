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
#include <cstddef>

namespace starrocks::spill {

class GlobalSpillManager {
public:
    size_t spillable_operators() const { return _spillable_operators; }

    void increase_spillable_operators() { _spillable_operators++; }
    void decrease_spillable_operators() { _spillable_operators--; }

    size_t spill_expected_reserved_bytes() const { return _expected_reserved_bytes; }
    void inc_reserve_bytes(size_t bytes) { _expected_reserved_bytes += bytes; }
    void dec_reserve_bytes(size_t bytes) { _expected_reserved_bytes -= bytes; }

private:
    std::atomic_size_t _spillable_operators = 0;
    std::atomic_size_t _expected_reserved_bytes = 0;
};

} // namespace starrocks::spill
