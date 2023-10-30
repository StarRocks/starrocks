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

#include "storage/lake/sstable/lake_persistent_index_sst.h"

#include "fs/fs.h"
#include "storage/lake/sstable/table_builder.h"

namespace starrocks {

namespace lake {

Status LakePersistentIndexSstable::build_sstable(phmap::btree_map<std::string, IndexValue>& memtable, WritableFile* wf,
                                                 uint64_t* filesz) {
    sstable::Options options;
    sstable::TableBuilder builder(options, wf);
    for (const auto& pair : memtable) {
        builder.Add(Slice(pair.first), Slice(pair.second.v, 8));
    }
    RETURN_IF_ERROR(builder.Finish());
    *filesz = builder.FileSize();
    return Status::OK();
}

} // namespace lake
} // namespace starrocks