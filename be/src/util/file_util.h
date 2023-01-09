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

#include <fstream>
#include <string>
#include <tuple>

namespace starrocks {
class FileUtil {
public:
    // Return true if it is correctly assigned, otherwise return false
    static bool read_whole_content(const std::string& path, std::string& dest);

    // Return true if all values are correctly assigned, otherwise return false
    template <typename... Args>
    static bool read_contents(const std::string& path, Args&... values) {
        std::ifstream ifs;
        ifs.open(path);

        auto tvalues = std::forward_as_tuple(values...);
        bool ok = ifs.good();
        _constexpr_for<0, sizeof...(values), 1>([&ifs, &tvalues, &ok](auto i) {
            ok &= ifs.good();
            if (!ok) {
                return;
            }
            ifs >> std::get<i>(tvalues);
        });

        if (ifs.is_open()) {
            ifs.close();
        }
        return ok;
    }

private:
    template <auto Start, auto End, auto Inc, typename F>
    static constexpr void _constexpr_for(F&& f) {
        if constexpr (Start < End) {
            f(std::integral_constant<decltype(Start), Start>());
            _constexpr_for<Start + Inc, End, Inc>(f);
        }
    }
};
}; // namespace starrocks
