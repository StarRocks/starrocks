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

namespace starrocks {

LakeMetaScanner::LakeMetaScanner(LakeMetaScanNode* parent) : _parent(parent) {}

Status LakeMetaScanner::init(RuntimeState* runtime_state, const MetaScannerParams& params) {
    _runtime_state = runtime_state;
    RETURN_IF_ERROR(_get_tablet(params.scan_range));
    RETURN_IF_ERROR(_init_meta_reader_params());
    _reader = std::make_shared<LakeMetaReader>();

    if (_reader == nullptr) {
        return Status::InternalError("Failed to allocate meta reader.");
    }

    RETURN_IF_ERROR(_reader->init(_reader_params));
    return Status::OK();
}

Status LakeMetaScanner::_init_meta_reader_params() {
    _reader_params.tablet = _tablet;
    _reader_params.tablet_schema = _tablet_schema;
    _reader_params.version = Version(0, _version);
    _reader_params.runtime_state = _runtime_state;
    _reader_params.chunk_size = _runtime_state->chunk_size();
    _reader_params.id_to_names = &_parent->_meta_scan_node.id_to_names;
    _reader_params.desc_tbl = &_parent->_desc_tbl;

    return Status::OK();
}

Status LakeMetaScanner::get_chunk(RuntimeState* state, ChunkPtr* chunk) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }

    if (!_is_open) {
        return Status::InternalError("OlapMetaScanner Not open.");
    }
    return _reader->do_get_next(chunk);
}

Status LakeMetaScanner::open(RuntimeState* state) {
    DCHECK(!_is_closed);
    if (!_is_open) {
        _is_open = true;
        RETURN_IF_ERROR(_reader->open());
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

Status LakeMetaScanner::_get_tablet(const TInternalScanRange* scan_range) {
    _version = strtoul(scan_range->version.c_str(), nullptr, 10);
    ASSIGN_OR_RETURN(_tablet, ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet(scan_range->tablet_id));
    ASSIGN_OR_RETURN(_tablet_schema, _tablet->get_schema());
    return Status::OK();
}

} // namespace starrocks