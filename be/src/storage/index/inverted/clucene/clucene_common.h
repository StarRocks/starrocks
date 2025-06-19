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

#include "CLucene.h"

namespace starrocks {

struct DirectoryDeleter {
    void operator()(lucene::store::Directory* ptr) const { _CLDECDELETE(ptr); }
};

using InvertedIndexDirectoryMap = std::map<int64_t, std::shared_ptr<lucene::store::Directory>>;

class ReaderFileEntry final : LUCENE_BASE {
public:
    std::string file_name{};
    int64_t offset;
    int64_t length;
    ReaderFileEntry() {
        offset = 0;
        length = 0;
    }
    ~ReaderFileEntry() override = default;
};

using EntriesType =
        lucene::util::CLHashMap<char*, ReaderFileEntry*, lucene::util::Compare::Char, lucene::util::Equals::Char,
                                lucene::util::Deletor::acArray, lucene::util::Deletor::Object<ReaderFileEntry>>;

} // namespace starrocks
