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

#include "base/utility/pretty_printer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "gutil/sysinfo.h"

namespace starrocks {
namespace {

int64_t cycles_per_ms_for_test() {
    int64_t cycles_per_ms = static_cast<int64_t>(base::CyclesPerSecond() / 1000.0);
    if (cycles_per_ms <= 0) {
        cycles_per_ms = 1;
    }
    return cycles_per_ms;
}

} // namespace

TEST(PrettyPrinterTest, PrintBytes) {
    EXPECT_EQ("0", PrettyPrinter::print(static_cast<int64_t>(0), TUnit::BYTES));
    EXPECT_EQ("1.00 KB", PrettyPrinter::print(static_cast<int64_t>(1024), TUnit::BYTES));
    EXPECT_EQ("1.00 MB", PrettyPrinter::print(static_cast<int64_t>(1024 * 1024), TUnit::BYTES));
}

TEST(PrettyPrinterTest, PrintUnit) {
    EXPECT_EQ("1.00K", PrettyPrinter::print(static_cast<int64_t>(1000), TUnit::UNIT));
    EXPECT_EQ("1.00 K/sec", PrettyPrinter::print(static_cast<int64_t>(1000), TUnit::UNIT_PER_SECOND));
}

TEST(PrettyPrinterTest, PrintTimeNs) {
    EXPECT_EQ("1.234ms", PrettyPrinter::print(static_cast<int64_t>(1234567), TUnit::TIME_NS));
}

TEST(PrettyPrinterTest, PrintTimeMs) {
    EXPECT_EQ("1s234ms", PrettyPrinter::print(static_cast<int64_t>(1234), TUnit::TIME_MS));
}

TEST(PrettyPrinterTest, PrintCpuTicks) {
    EXPECT_EQ("0.00K clock cycles", PrettyPrinter::print(static_cast<int64_t>(0), TUnit::CPU_TICKS));

    const int64_t cycles_per_ms = cycles_per_ms_for_test();
    EXPECT_EQ("1ms", PrettyPrinter::print(cycles_per_ms, TUnit::CPU_TICKS));
}

TEST(PrettyPrinterTest, PrintUniqueIntListRange) {
    const std::vector<int> values{1, 2, 3, 5, 7, 8, 9};
    EXPECT_EQ("1-3,5,7-9", PrettyPrinter::print_unique_int_list_range(values));
}

} // namespace starrocks
