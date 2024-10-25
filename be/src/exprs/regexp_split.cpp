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

#include "exprs/regexp_split.h"

#include <re2/re2.h>

using namespace starrocks;

namespace starrocks {

unsigned RegexpSplit::match(const char* subject, size_t subject_size, std::vector<starrocks::Match>& matches,
                            unsigned limit) const {
    matches.clear();

    if (limit == 0) {
        return 0;
    }

    limit = std::min(limit, _number_of_subpatterns + 1);

    re2::StringPiece pieces[limit];

    if (!_re2->Match({subject, subject_size}, 0, subject_size, re2::RE2::UNANCHORED, pieces, limit)) {
        return 0;
    } else {
        matches.resize(limit);
        for (size_t i = 0; i < limit; ++i) {
            if (pieces[i].empty()) {
                matches[i].offset = std::string::npos;
                matches[i].length = 0;
            } else {
                matches[i].offset = pieces[i].data() - subject;
                matches[i].length = pieces[i].length();
            }
        }
        return limit;
    }
}

void RegexpSplit::init(re2::RE2* re2, int32_t max_splits) {
    _max_splits = max_splits;
    _re2 = re2;
    if (_re2) {
        _number_of_subpatterns = _re2->NumberOfCapturingGroups();
    }
}

// Called for each next string.
void RegexpSplit::set(const char* pos, const char* end) {
    _pos = pos;
    _end = end;
    _splits = 0;
}

// Get the next token, if any, or return false.
bool RegexpSplit::get(const char*& token_begin, const char*& token_end) {
    if (!_re2) {
        if (_pos == _end) {
            return false;
        }

        token_begin = _pos;

        if (_max_splits != -1) {
            if (_splits == _max_splits - 1) {
                token_end = _end;
                _pos = nullptr;
                return true;
            }
        }

        _pos += 1;
        token_end = _pos;
        ++_splits;
    } else {
        if (!_pos || _pos > _end) {
            return false;
        }

        token_begin = _pos;

        if (_max_splits != -1) {
            if (_splits == _max_splits - 1) {
                token_end = _end;
                _pos = nullptr;
                return true;
            }
        }

        if (!match(_pos, _end - _pos, _matches, _number_of_subpatterns + 1) || !_matches[0].length) {
            token_end = _end;
            _pos = _end + 1;
        } else {
            token_end = _pos + _matches[0].offset;
            _pos = token_end + _matches[0].length;
            ++_splits;
        }
    }

    return true;
}

} // namespace starrocks
