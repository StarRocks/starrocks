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

#include <cstdint>
#include <string_view>

namespace starrocks {

class TestingAlternative {
public:
    enum Alternative : uint8_t {
        // The initial value of TestingAlternative is Unknown,
        // which should be considered as an error
        // if we encounter a TestingAlternative with a value of Unknown.
        Unknown = 0,
        TwoSided,
        Less,
        Greater,
    };

    TestingAlternative(Alternative alternative = Unknown) : _alternative(alternative) {}
    TestingAlternative(uint8_t alternative) : _alternative(static_cast<Alternative>(alternative)) {}

    static TestingAlternative from_str(std::string_view alternative_str) {
        TestingAlternative alternative;
        if (alternative_str == "two-sided") {
            alternative._alternative = TwoSided;
        } else if (alternative_str == "less") {
            alternative._alternative = Less;
        } else if (alternative_str == "greater") {
            alternative._alternative = Greater;
        } else {
            alternative._alternative = Unknown;
        }
        return alternative;
    }

    friend bool operator==(TestingAlternative const& lhs, TestingAlternative const& rhs) {
        return lhs._alternative == rhs._alternative;
    }

    friend bool operator==(TestingAlternative const& lhs, Alternative const& rhs) { return lhs._alternative == rhs; }

    uint8_t value() const { return _alternative; }

private:
    Alternative _alternative{Unknown};
};

} // namespace starrocks
