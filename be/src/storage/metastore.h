// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"
#include "gen_cpp/Types_types.h"
#include "storage/olap_common.h"

namespace starrocks {

class RowsetMeta;
class TabletMeta;
using RowsetMetaSharedPtr = std::shared_ptr<RowsetMeta>;
using TabletMetaSharedPtr = std::shared_ptr<TabletMeta>;

// Metastore provides abstract interface for meta operations, such as tablet meta and rowset meta.
class Metastore {
public:
    virtual ~Metastore() = default;

    // Add a new tablet meta.
    virtual Status add_tablet_meta(const TabletMeta& tablet_meta) = 0;
    // Update the tablet meta that already exist.
    virtual Status update_tablet_meta(const TabletMeta& tablet_meta) = 0;
    // Get a tablet meta by tablet id and schema hash.
    virtual StatusOr<TabletMetaSharedPtr> get_tablet_meta(TTabletId tablet_id, TSchemaHash schema_hash) = 0;
    // Remove the tablet meta by tablet id and schema hash.
    virtual Status remove_tablet_meta(TTabletId tablet_id, TSchemaHash schema_hash) = 0;

    // Add a new rowset meta.
    virtual Status add_rowset_meta(const RowsetMeta& rowset_meta) = 0;
    // Update the rowset meta that already exist.
    virtual Status update_rowset_meta(const RowsetMeta& rowset_meta) = 0;
    // Get a rowset meta by tablet uid and rowset id.
    virtual StatusOr<RowsetMetaSharedPtr> get_rowset_meta(const TabletUid& tablet_uid, const RowsetId& rowset_id) = 0;
    // Get all rowset metas by tablet uid.
    virtual StatusOr<std::vector<RowsetMetaSharedPtr>> get_rowset_metas(const TabletUid& tablet_uid) = 0;
    // Remove the rowset meta by tablet uid and rowset id.
    virtual Status remove_rowset_meta(const TabletUid& tablet_uid, const RowsetId& rowset_id) = 0;
};

} // namespace starrocks