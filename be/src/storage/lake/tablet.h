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

#include <memory>
#include <string>
#include <string_view>

#include "common/statusor.h"
#include "gen_cpp/types.pb.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"

namespace starrocks {
class TabletSchema;
}

namespace starrocks {
class Schema;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;
class TabletReader;
class TabletWriter;
template <typename T>
class MetadataIterator;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;
class UpdateManager;

class Tablet {
public:
    explicit Tablet(TabletManager* mgr, int64_t id) : _mgr(mgr), _id(id) {}

    ~Tablet() = default;

    // Default copy and assign
    Tablet(const Tablet&) = default;
    Tablet& operator=(const Tablet&) = default;

    // Default move copy and move assign
    Tablet(Tablet&&) = default;
    Tablet& operator=(Tablet&&) = default;

    [[nodiscard]] int64_t id() const { return _id; }

    [[nodiscard]] std::string root_location() const;

    Status put_metadata(const TabletMetadata& metadata);

    Status put_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_metadata(int64_t version);

    Status delete_metadata(int64_t version);

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_vlog(int64_t version);

    Status delete_txn_log(int64_t txn_id);

    Status delete_txn_vlog(int64_t version);

    Status put_tablet_metadata_lock(int64_t version, int64_t expire_time);

    Status delete_tablet_metadata_lock(int64_t version, int64_t expire_time);

    StatusOr<std::unique_ptr<TabletWriter>> new_writer();

    StatusOr<std::shared_ptr<TabletReader>> new_reader(int64_t version, Schema schema);

    StatusOr<std::shared_ptr<const TabletSchema>> get_schema();

    StatusOr<std::vector<RowsetPtr>> get_rowsets(int64_t version);

    StatusOr<std::vector<RowsetPtr>> get_rowsets(const TabletMetadata& metadata);

    StatusOr<SegmentPtr> load_segment(std::string_view segment_name, int seg_id, size_t* footer_size_hint,
                                      bool fill_cache);

    [[nodiscard]] std::string metadata_location(int64_t version) const;

    [[nodiscard]] std::string metadata_root_location() const;

    [[nodiscard]] std::string txn_log_location(int64_t txn_id) const;

    [[nodiscard]] std::string txn_vlog_location(int64_t version) const;

    [[nodiscard]] std::string segment_location(std::string_view segment_name) const;

    [[nodiscard]] std::string del_location(std::string_view del_name) const;

    [[nodiscard]] std::string delvec_location(int64_t version) const;

    Status delete_data(int64_t txn_id, const DeletePredicatePB& delete_predicate);

    StatusOr<bool> has_delete_predicates(int64_t version);

    UpdateManager* update_mgr() { return _mgr->update_mgr(); }

    TabletManager* tablet_mgr() { return _mgr; }

private:
    TabletManager* _mgr;
    int64_t _id;
};

} // namespace starrocks::lake
