// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/tablet.h"

#include "column/schema.h"
#include "storage/lake/general_tablet_writer.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/txn_log.h"

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

Status Tablet::delete_txn_log(int64_t txn_id) {
    return _mgr->delete_txn_log(_id, txn_id);
}

StatusOr<std::unique_ptr<TabletWriter>> Tablet::new_writer() {
    // TODO: check tablet type
    return std::make_unique<GeneralTabletWriter>(*this);
}

StatusOr<std::shared_ptr<TabletReader>> Tablet::new_reader(int64_t version, vectorized::Schema schema) {
    return std::make_shared<TabletReader>(*this, version, std::move(schema));
}

StatusOr<std::shared_ptr<const TabletSchema>> Tablet::get_schema() {
    return _mgr->get_tablet_schema(_id);
}

StatusOr<std::vector<RowsetPtr>> Tablet::get_rowsets(int64_t version) {
    ASSIGN_OR_RETURN(auto tablet_metadata, get_metadata(version));
    ASSIGN_OR_RETURN(auto tablet_schema, get_schema());
    std::vector<RowsetPtr> rowsets;
    rowsets.reserve(tablet_metadata->rowsets_size());
    for (const auto& rowset_metadata : tablet_metadata->rowsets()) {
        auto rowset = std::make_shared<Rowset>(this, std::make_shared<const RowsetMetadata>(rowset_metadata));
        rowsets.emplace_back(std::move(rowset));
    }
    return rowsets;
}

std::string Tablet::metadata_location(int64_t version) const {
    return _mgr->tablet_metadata_location(_id, version);
}

std::string Tablet::txn_log_location(int64_t txn_id) const {
    return _mgr->txn_log_location(_id, txn_id);
}

std::string Tablet::segment_location(std::string_view segment_name) const {
    return _mgr->segment_location(_id, segment_name);
}

std::string Tablet::root_location() const {
    return _mgr->tablet_root_location(_id);
}

} // namespace starrocks::lake
