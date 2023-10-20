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
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/types_fwd.h"

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
enum WriterType : int;

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

    [[nodiscard]] Status put_metadata(const TabletMetadata& metadata);

    [[nodiscard]] Status put_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_metadata(int64_t version);

    [[nodiscard]] Status delete_metadata(int64_t version);

    bool get_enable_persistent_index(int64_t version);

    StatusOr<PersistentIndexTypePB> get_persistent_index_type(int64_t version);

    [[nodiscard]] Status put_txn_log(const TxnLog& log);

    [[nodiscard]] Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_vlog(int64_t version);

    // `segment_max_rows` is used in vertical writer
    // NOTE: This method may update the version hint
    StatusOr<std::unique_ptr<TabletWriter>> new_writer(WriterType type, int64_t txn_id,
                                                       uint32_t max_rows_per_segment = 0);

    StatusOr<std::shared_ptr<TabletReader>> new_reader(int64_t version, Schema schema);

    // NOTE: This method may update the version hint
    StatusOr<std::shared_ptr<const TabletSchema>> get_schema();

    StatusOr<std::shared_ptr<const TabletSchema>> get_schema_by_index_id(int64_t index_id);

    StatusOr<std::vector<RowsetPtr>> get_rowsets(int64_t version);

    StatusOr<std::vector<RowsetPtr>> get_rowsets(const TabletMetadata& metadata);

    StatusOr<SegmentPtr> load_segment(std::string_view segment_name, int seg_id, size_t* footer_size_hint,
                                      bool fill_data_cache, bool fill_metadata_cache);

    [[nodiscard]] std::string metadata_location(int64_t version) const;

    [[nodiscard]] std::string metadata_root_location() const;

    [[nodiscard]] std::string txn_log_location(int64_t txn_id) const;

    [[nodiscard]] std::string txn_vlog_location(int64_t version) const;

    [[nodiscard]] std::string segment_location(std::string_view segment_name) const;

    [[nodiscard]] std::string del_location(std::string_view del_name) const;

    [[nodiscard]] std::string delvec_location(std::string_view delvec_name) const;

    [[nodiscard]] Status delete_data(int64_t txn_id, const DeletePredicatePB& delete_predicate);

    StatusOr<bool> has_delete_predicates(int64_t version);

    UpdateManager* update_mgr() { return _mgr->update_mgr(); }

    TabletManager* tablet_mgr() { return _mgr; }

    // Many tablet operations need to fetch the tablet schema information
    // stored in the object storage, if the cache does not hit. In order to
    // reduce the costly listDirectory/listObject operations, you can specify
    // an existing tablet metadata version, so you can directly obtain the schema
    // information by reading the metadata of that version, without listObject.
    //
    // NOTE: set this value to a non-positive value means clear the version hint.
    // NOTE: Some methods of Tablet will internally update this value automatically.
    void set_version_hint(int64_t version_hint) { _version_hint = version_hint; }

    int64_t version_hint() const { return _version_hint; }

    int64_t data_size();

private:
    TabletManager* _mgr;
    int64_t _id;
    int64_t _version_hint = 0;
};

} // namespace starrocks::lake
