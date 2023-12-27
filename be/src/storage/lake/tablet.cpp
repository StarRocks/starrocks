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

#include "storage/lake/tablet.h"

#include "column/schema.h"
#include "fs/fs.h"
#include "runtime/exec_env.h"
#include "storage/lake/filenames.h"
#include "storage/lake/general_tablet_writer.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/pk_tablet_writer.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"

namespace starrocks::lake {

Status Tablet::put_metadata(const TabletMetadata& metadata) {
    return _mgr->put_tablet_metadata(metadata);
}

Status Tablet::put_metadata(TabletMetadataPtr metadata) {
    return _mgr->put_tablet_metadata(std::move(metadata));
}

StatusOr<TabletMetadataPtr> Tablet::get_metadata(int64_t version) {
    return _mgr->get_tablet_metadata(_id, version);
}

Status Tablet::delete_metadata(int64_t version) {
    return _mgr->delete_tablet_metadata(_id, version);
}

Status Tablet::put_txn_log(const TxnLog& log) {
    return _mgr->put_txn_log(log);
}

Status Tablet::put_txn_log(TxnLogPtr log) {
    return _mgr->put_txn_log(std::move(log));
}

StatusOr<TxnLogPtr> Tablet::get_txn_log(int64_t txn_id) {
    return _mgr->get_txn_log(_id, txn_id);
}

StatusOr<TxnLogPtr> Tablet::get_txn_vlog(int64_t version) {
    return _mgr->get_txn_vlog(_id, version);
}

Status Tablet::delete_txn_log(int64_t txn_id) {
    return _mgr->delete_txn_log(_id, txn_id);
}

Status Tablet::delete_txn_vlog(int64_t version) {
    return _mgr->delete_txn_vlog(_id, version);
}

Status Tablet::put_tablet_metadata_lock(int64_t version, int64_t expire_time) {
    return _mgr->put_tablet_metadata_lock(_id, version, expire_time);
}

Status Tablet::delete_tablet_metadata_lock(int64_t version, int64_t expire_time) {
    return _mgr->delete_tablet_metadata_lock(_id, version, expire_time);
}

StatusOr<std::unique_ptr<TabletWriter>> Tablet::new_writer(WriterType type, int64_t txn_id,
                                                           uint32_t max_rows_per_segment) {
    ASSIGN_OR_RETURN(auto tablet_schema, get_schema());
    if (tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        if (type == kHorizontal) {
            return std::make_unique<HorizontalPkTabletWriter>(*this, tablet_schema, txn_id);
        } else {
            DCHECK(type == kVertical);
            return std::make_unique<VerticalPkTabletWriter>(*this, tablet_schema, txn_id, max_rows_per_segment);
        }
    } else {
        if (type == kHorizontal) {
            return std::make_unique<HorizontalGeneralTabletWriter>(*this, tablet_schema, txn_id);
        } else {
            DCHECK(type == kVertical);
            return std::make_unique<VerticalGeneralTabletWriter>(*this, tablet_schema, txn_id, max_rows_per_segment);
        }
    }
}

StatusOr<std::shared_ptr<TabletReader>> Tablet::new_reader(int64_t version, Schema schema) {
    return std::make_shared<TabletReader>(*this, version, std::move(schema));
}

StatusOr<std::shared_ptr<const TabletSchema>> Tablet::get_schema() {
    return _mgr->get_tablet_schema(_id, &_version_hint);
}

StatusOr<std::vector<RowsetPtr>> Tablet::get_rowsets(int64_t version) {
    ASSIGN_OR_RETURN(auto tablet_metadata, get_metadata(version));
    std::vector<RowsetPtr> rowsets;
    rowsets.reserve(tablet_metadata->rowsets_size());
    for (int i = 0, size = tablet_metadata->rowsets_size(); i < size; ++i) {
        const auto& rowset_metadata = tablet_metadata->rowsets(i);
        auto rowset = std::make_shared<Rowset>(this, std::make_shared<const RowsetMetadata>(rowset_metadata), i);
        rowsets.emplace_back(std::move(rowset));
    }
    return rowsets;
}

StatusOr<std::vector<RowsetPtr>> Tablet::get_rowsets(const TabletMetadata& metadata) {
    std::vector<RowsetPtr> rowsets;
    rowsets.reserve(metadata.rowsets_size());
    for (int i = 0, size = metadata.rowsets_size(); i < size; ++i) {
        const auto& rowset_metadata = metadata.rowsets(i);
        auto rowset = std::make_shared<Rowset>(this, std::make_shared<const RowsetMetadata>(rowset_metadata), i);
        rowsets.emplace_back(std::move(rowset));
    }
    return rowsets;
}

StatusOr<SegmentPtr> Tablet::load_segment(std::string_view segment_name, int seg_id, size_t* footer_size_hint,
                                          bool fill_cache) {
    auto location = segment_location(segment_name);
    auto segment = _mgr->lookup_segment(location);
    if (segment != nullptr) {
        return segment;
    }
    ASSIGN_OR_RETURN(auto tablet_schema, get_schema());
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(location));
    ASSIGN_OR_RETURN(segment, Segment::open(fs, location, seg_id, std::move(tablet_schema), footer_size_hint, nullptr,
                                            !fill_cache));
    if (fill_cache) {
        _mgr->cache_segment(location, segment);
    }
    return segment;
}

std::string Tablet::metadata_location(int64_t version) const {
    return _mgr->tablet_metadata_location(_id, version);
}

std::string Tablet::metadata_root_location() const {
    return _mgr->tablet_metadata_root_location(_id);
}

std::string Tablet::txn_log_location(int64_t txn_id) const {
    return _mgr->txn_log_location(_id, txn_id);
}

std::string Tablet::txn_vlog_location(int64_t version) const {
    return _mgr->txn_vlog_location(_id, version);
}

std::string Tablet::segment_location(std::string_view segment_name) const {
    return _mgr->segment_location(_id, segment_name);
}

std::string Tablet::del_location(std::string_view del_name) const {
    return _mgr->del_location(_id, del_name);
}

std::string Tablet::delvec_location(std::string_view delvec_name) const {
    return _mgr->delvec_location(_id, delvec_name);
}

std::string Tablet::root_location() const {
    return _mgr->tablet_root_location(_id);
}

Status Tablet::delete_data(int64_t txn_id, const DeletePredicatePB& delete_predicate) {
    auto txn_log = std::make_shared<lake::TxnLog>();
    txn_log->set_tablet_id(_id);
    txn_log->set_txn_id(txn_id);
    auto op_write = txn_log->mutable_op_write();
    auto rowset = op_write->mutable_rowset();
    rowset->set_overlapped(false);
    rowset->set_num_rows(0);
    rowset->set_data_size(0);
    rowset->mutable_delete_predicate()->CopyFrom(delete_predicate);
    return put_txn_log(std::move(txn_log));
}

StatusOr<bool> Tablet::has_delete_predicates(int64_t version) {
    ASSIGN_OR_RETURN(auto metadata, get_metadata(version));
    for (const auto& rowset : metadata->rowsets()) {
        if (rowset.has_delete_predicate()) {
            return true;
        }
    }
    return false;
}

} // namespace starrocks::lake
