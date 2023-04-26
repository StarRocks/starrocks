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

#include "storage/chunk_iterator.h"
#include "storage/tablet.h"

namespace starrocks {

class ColumnPredicate;
class PTabletReaderMultiGetRequest;
class PTabletReaderMultiGetResult;

// A tablet reader supports multiple get and scan snapshot reads
// this class is used for remote accessible TableReader, do not confuse with TabletReader
class LocalTabletReader {
public:
    LocalTabletReader();

    Status init(TabletSharedPtr& tablet, int64_t version);

    Status close();

    // for detail document, see TableReader::scan
    Status multi_get(const Chunk& keys, const std::vector<std::string>& value_columns, std::vector<bool>& found,
                     Chunk& values);

    // for detail document, see TableReader::scan
    Status multi_get(const Chunk& keys, const std::vector<uint32_t>& value_column_ids, std::vector<bool>& found,
                     Chunk& values);

    // for detail document, see TableReader::scan
    StatusOr<ChunkIteratorPtr> scan(const std::vector<std::string>& value_columns,
                                    const std::vector<const ColumnPredicate*>& predicates);

private:
    TabletSharedPtr _tablet;
    int64_t _version{0};
};

Status handle_tablet_multi_get_rpc(const PTabletReaderMultiGetRequest& request, PTabletReaderMultiGetResult& result);

// Manage remotely opened LocalTabletReaders
class LocalTabletReaderManager {
public:
    LocalTabletReaderManager();
};

} // namespace starrocks
