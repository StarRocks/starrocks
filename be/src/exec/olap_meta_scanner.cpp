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

#include "exec/olap_meta_scanner.h"

#include "exec/olap_meta_scan_node.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"

namespace starrocks {

OlapMetaScanner::OlapMetaScanner(OlapMetaScanNode* parent) : _parent(parent) {}

Status OlapMetaScanner::init(RuntimeState* runtime_state, const MetaScannerParams& params) {
    _runtime_state = runtime_state;
    RETURN_IF_ERROR(_get_tablet(params.scan_range));
    RETURN_IF_ERROR(_init_meta_reader_params());
    _reader = std::make_shared<OlapMetaReader>();

    if (_reader == nullptr) {
        return Status::InternalError("Failed to allocate meta reader.");
    }

    RETURN_IF_ERROR(_reader->init(_reader_params));
    return Status::OK();
}

Status OlapMetaScanner::_init_meta_reader_params() {
    _reader_params.tablet_id = _tablet->tablet_id();
    _reader_params.tablet = _tablet;
    _reader_params.version = Version(0, _version);
    _reader_params.runtime_state = _runtime_state;
    _reader_params.chunk_size = _runtime_state->chunk_size();
    _reader_params.id_to_names = &_parent->_meta_scan_node.id_to_names;
    _reader_params.low_card_threshold = _parent->_meta_scan_node.__isset.low_cardinality_threshold
                                                ? _parent->_meta_scan_node.low_cardinality_threshold
                                                : DICT_DECODE_MAX_SIZE;
    if (_parent->_meta_scan_node.__isset.columns && !_parent->_meta_scan_node.columns.empty() &&
        _parent->_meta_scan_node.columns[0].col_unique_id > 0) {
        _reader_params.tablet_schema = TabletSchema::copy(*_tablet->tablet_schema(), _parent->_meta_scan_node.columns);
    } else {
        _reader_params.tablet_schema = _tablet->tablet_schema();
    }
    _reader_params.desc_tbl = &_parent->_desc_tbl;

    return Status::OK();
}

Status OlapMetaScanner::get_chunk(RuntimeState* state, ChunkPtr* chunk) {
    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }

    if (!_is_open) {
        return Status::InternalError("OlapMetaScanner Not open.");
    }
    return _reader->do_get_next(chunk);
}

Status OlapMetaScanner::open(RuntimeState* state) {
    DCHECK(!_is_closed);
    if (!_is_open) {
        _is_open = true;
        RETURN_IF_ERROR(_reader->open());
    }
    return Status::OK();
}

void OlapMetaScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }
    _tablet.reset();
    _reader.reset();
    _is_closed = true;
}

bool OlapMetaScanner::has_more() {
    return _reader->has_more();
}

Status OlapMetaScanner::_get_tablet(const TInternalScanRange* scan_range) {
    TTabletId tablet_id = scan_range->tablet_id;
    SchemaHash schema_hash = strtoul(scan_range->schema_hash.c_str(), nullptr, 10);
    _version = strtoul(scan_range->version.c_str(), nullptr, 10);

    std::string err;
    _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
    if (!_tablet) {
        std::stringstream ss;
        ss << "failed to get tablet. tablet_id=" << tablet_id << ", with schema_hash=" << schema_hash
           << ", reason=" << err;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

} // namespace starrocks
