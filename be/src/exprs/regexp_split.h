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

#include <re2/re2.h>

namespace starrocks {

struct Match {
    std::string::size_type offset;
    std::string::size_type length;
};

class RegexpSplit {
public:
    void init(re2::RE2* re2, int32_t max_splits);
    void set(const char* pos, const char* end);
    bool get(const char*& token_begin, const char*& token_end);

private:
    const char* _pos;
    const char* _end;

    std::int32_t _max_splits;
    std::vector<Match> _matches;
    int32_t _splits;
    re2::RE2* _re2 = nullptr;
    unsigned _number_of_subpatterns;

    unsigned match(const char* subject, size_t subject_size, std::vector<starrocks::Match>& matches,
                   unsigned limit) const;
};

} // namespace starrocks
