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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace starrocks {

class PublishPropertyPB;

// What a publish request carries for the table, when it carries anything. std::optional cannot hold a
// reference, so the reference travels wrapped; the wrapper itself is never empty, which leaves the
// optional as the one place "the request brought nothing" is expressed.
using PublishPropertyPBRef = std::reference_wrapper<const PublishPropertyPB>;

// What one table has set for the primary key work a publish does, and the revision naming that set.
//
// Every member here is a value the table itself set. A member left empty is not a default sitting in
// for one -- it means the table set nothing, and the accessor answers with this node's config, read
// at the moment of the call. Holding the config value in the member instead would freeze it: an
// instance outlives many publishes, so a later ADMIN SET would reach every tablet that had yet to
// build one and no tablet that already had.
//
// Callers therefore never ask whether a property was set. They call an accessor and get the value to
// run with, and the name of the config behind it appears in this file and nowhere else.
//
// An instance is never edited once built. update() hands back a new one when a publish brings a
// newer revision, so a caller already holding a pointer -- including asynchronous work a publish
// started -- goes on reading the values that publish began with.
class PkPublishConfig {
public:
    // A table that has set nothing: every accessor answers with this node's config.
    PkPublishConfig() = default;

    // The set `current` should be replaced by, given what a publish request carries.
    //
    // Returns `current` itself unless `publish_property` names a strictly higher revision, so a
    // request that overtook a newer one on the way here cannot roll a table back, and a retry of the
    // revision already held costs no parsing. `current` must not be null.
    static std::shared_ptr<const PkPublishConfig> update(const std::shared_ptr<const PkPublishConfig>& current,
                                                         int64_t tablet_id, const PublishPropertyPB& publish_property);

    int64_t revision() const { return _revision; }

    // The properties the table set, named as the user writes them, paired with the values this
    // tablet accepted. Declaration order, so two readings of the same set read the same.
    //
    // A property the table left unset is absent, and so is one whose value this node refused. Both
    // are answered by the accessors from this node's config, which is not something this object
    // holds and so not something it can report.
    std::vector<std::pair<std::string_view, int64_t>> properties() const;

    // At most this many index memtables may be waiting on a flush before one is flushed inline.
    int32_t memtable_max_count() const;
    // How large one index memtable grows before it is flushed.
    int64_t memtable_max_bytes() const;
    // How many files, and how many rows, a future index rebuild may have to replay before a publish
    // spends an extra flush to shorten it. Not positive means the trigger is off.
    int32_t rebuild_files_threshold() const;
    int64_t rebuild_rows_threshold() const;
    // Below this many rows, splitting the work across threads costs more than it saves; it is also
    // the row count at which a batch of primary keys is cut.
    int64_t parallel_execution_min_rows() const;
    // How many bytes of primary key column data one batch accumulates before it is handed on.
    int64_t column_read_batch_bytes() const;
    // Reading the compaction rows mapper: how many reads stay in flight, and how large each is.
    int32_t rows_mapper_read_parallelism() const;
    int64_t rows_mapper_read_batch_bytes() const;
    // How many rows accumulate before one replace call into the primary key index.
    int32_t compaction_replace_batch_rows() const;

private:
    PkPublishConfig(int64_t tablet_id, const PublishPropertyPB& publish_property);

    int64_t _revision = 0;

    std::optional<int32_t> _memtable_max_count;
    std::optional<int64_t> _memtable_max_bytes;
    std::optional<int32_t> _rebuild_files_threshold;
    std::optional<int64_t> _rebuild_rows_threshold;
    std::optional<int64_t> _parallel_execution_min_rows;
    std::optional<int64_t> _column_read_batch_bytes;
    std::optional<int32_t> _rows_mapper_read_parallelism;
    std::optional<int64_t> _rows_mapper_read_batch_bytes;
    std::optional<int32_t> _compaction_replace_batch_rows;
};

using PkPublishConfigPtr = std::shared_ptr<const PkPublishConfig>;

} // namespace starrocks
