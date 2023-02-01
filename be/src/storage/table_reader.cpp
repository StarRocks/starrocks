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

#include "storage/table_reader.h"

#include "storage/local_tablet_reader.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"

namespace starrocks {

TableReader::TableReader() {}

TableReader::~TableReader() {}

Status TableReader::init(const LocalTableReaderParams& local_params) {
    if (_local_params || _params) {
        return Status::InternalError("TableReader already initialized");
    }
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(local_params.tablet_id);
    if (tablet == nullptr) {
        return Status::InternalError(strings::Substitute("tablet $0 not found", local_params.tablet_id));
    }
    auto local_tablet_reader = std::make_unique<LocalTabletReader>();
    RETURN_IF_ERROR(local_tablet_reader->init(tablet, local_params.version));
    _local_params = std::make_unique<LocalTableReaderParams>(local_params);
    _local_tablet_reader = std::move(local_tablet_reader);
    return Status::OK();
}

Status TableReader::init(const TableReaderParams& params) {
    return Status::NotSupported("remote table reader not supported");
}

Status TableReader::multi_get(const Chunk& keys, const std::vector<std::string>& value_columns,
                              std::vector<bool>& found, Chunk& values) {
    if (_local_tablet_reader) {
        return _local_tablet_reader->multi_get(keys, value_columns, found, values);
    }
    return Status::NotSupported("multi_get for remote table reader not supported");
}

StatusOr<ChunkIteratorPtr> TableReader::scan(const std::vector<std::string>& value_columns,
                                             const std::vector<const ColumnPredicate*>& predicates) {
    if (_local_tablet_reader) {
        return _local_tablet_reader->scan(value_columns, predicates);
    }
    return Status::NotSupported("scan for remote table reader not supported");
}

} // namespace starrocks
