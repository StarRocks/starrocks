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

#include "exec/lake_meta_scanner.h"

#include "exec/lake_meta_scan_node.h"
#include "testutil/sync_point.h"

namespace starrocks {

LakeMetaScanner::LakeMetaScanner(LakeMetaScanNode* parent) : _parent(parent) {}

Status LakeMetaScanner::init(RuntimeState* runtime_state, const MetaScannerParams& params) {
    return _lazy_init(runtime_state, params);
}

Status LakeMetaScanner::_lazy_init(RuntimeState* runtime_state, const MetaScannerParams& params) {
    _runtime_state = runtime_state;
    _tablet_id = params.scan_range->tablet_id;
    _version = strtoul(params.scan_range->version.c_str(), nullptr, 10);
    return Status::OK();
}

Status LakeMetaScanner::_real_init() {
    LakeMetaReaderParams reader_params;
    reader_params.tablet_id = _tablet_id;
    reader_params.version = Version(0, _version);
    reader_params.runtime_state = _runtime_state;
    reader_params.chunk_size = _runtime_state->chunk_size();
    reader_params.id_to_names = &_parent->_meta_scan_node.id_to_names;
    reader_params.desc_tbl = &_parent->_desc_tbl;

    _reader = std::make_unique<LakeMetaReader>();
    TEST_SYNC_POINT_CALLBACK("lake_meta_scanner:open_mock_reader", &_reader);
    // possible invoke heavy remote IO operations if local cache missed
    RETURN_IF_ERROR(_reader->init(reader_params));
    return Status::OK();
}

Status LakeMetaScanner::get_chunk(RuntimeState* state, ChunkPtr* chunk) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state of LakeMetaScanner");
    }

    if (!_is_open) {
        return Status::InternalError("LakeMetaScanner::open() has not been called or has failed");
    }
    return _reader->do_get_next(chunk);
}

Status LakeMetaScanner::open(RuntimeState* state) {
    DCHECK(!_is_closed);
    if (!_is_open) {
        if (!_reader) {
            RETURN_IF_ERROR(_real_init());
        }
        RETURN_IF_ERROR(_reader->open());
        _is_open = true;
    }
    return Status::OK();
}

void LakeMetaScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }
    _reader.reset();
    _is_closed = true;
}

bool LakeMetaScanner::has_more() {
    return _reader->has_more();
}

} // namespace starrocks
