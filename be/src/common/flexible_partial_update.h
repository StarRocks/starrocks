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
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"

namespace starrocks {

// Flexible partial update: in a JSON stream load, each row updates only the columns present in it and
// the columns it omits keep their current value.
//
// The exact set of columns a row declares is its "column set". The JSON scanner interns the distinct
// column sets of a load into a per-load dictionary, which hands out a dense set-id per set, and writes
// each row's set-id into the hidden "__cset__" column. FE injects that column into the plan immediately
// before "__op", so "__op" stays the last column. The delta writer keeps "__cset__" in the written
// segments under a reserved unique id and records the dictionary (translated to column unique ids) in
// RowsetTxnMetaPB.distinct_column_sets, and the publish decodes each row's column set from the two.
//
// The scanner-side dictionary holds column NAMES: the scanner only knows slot names, and the writer,
// which owns the tablet schema, translates names to unique ids.

// Name of the hidden per-row column-set id column. Mirrors FE Load.LOAD_CSET_COLUMN.
inline const std::string LOAD_CSET_COLUMN = "__cset__";

// Unique id under which the delta writer stores "__cset__" in the segments it writes. It must not
// collide with a real column unique id (those start at 0 and grow) or with other reserved ids.
inline constexpr int32_t kCsetReservedColumnUid = 0x7FFFFFFD; // INT32_MAX - 2

// "__cset__" is a SMALLINT slot that the scanner writes and the publish reads as a signed int16, so a
// set-id must fit [0, 32767] and a load may carry at most 2^15 distinct column sets. The scanner fails the
// load when that is exceeded.
using ColumnSetId = uint16_t;
inline constexpr ColumnSetId kInvalidColumnSetId = static_cast<ColumnSetId>(-1);
inline constexpr size_t kMaxColumnSets = 1u << 15;

// A per-load dictionary of distinct column sets, index == set-id.
//
// A set is a sorted, de-duplicated list of column names, keyed by its canonical joined form, so the
// same set gets the same id regardless of the order of the keys in the JSON object.
//
// Thread-safe: several scanner threads of one load may intern concurrently.
class ColumnSetDict {
public:
    ColumnSetDict() = default;

    // Interns a set of column names and returns its set-id. Returns kInvalidColumnSetId when the set is
    // new and the dictionary already holds kMaxColumnSets entries.
    ColumnSetId intern(std::vector<std::string> names);

    size_t size() const {
        std::lock_guard<std::mutex> l(_mu);
        return _sets.size();
    }

    // A copy of the dictionary, index == set-id.
    std::vector<std::vector<std::string>> snapshot() const {
        std::lock_guard<std::mutex> l(_mu);
        return _sets;
    }

    // Merges a snapshot that the load's sender shipped over the wire. Set-ids are assigned by position, so
    // they match the sender's exactly; the receiver never interns. The sender only ever appends to its
    // dictionary, so every snapshot it sends extends the previous ones: entries this dictionary already
    // holds must match the snapshot position by position, and the snapshot's extra entries are appended.
    // Several senders' eos requests may carry snapshots in any order, which is why this merges instead of
    // replacing. A mismatch means two dictionaries with different set-id spaces met, and is an error.
    Status merge_snapshot(const std::vector<std::vector<std::string>>& sets);

private:
    static std::string canonical_key(const std::vector<std::string>& sorted_names);
    static void canonicalize(std::vector<std::string>* names);

    mutable std::mutex _mu;
    // index == set-id
    std::vector<std::vector<std::string>> _sets;
    std::unordered_map<std::string, ColumnSetId> _index;
};

using ColumnSetDictPtr = std::shared_ptr<ColumnSetDict>;

// Process-wide registry from txn_id to the load's ColumnSetDict. It carries the dictionary from the JSON
// scanner, which interns into it, to the sink, which ships it to the tablet writers on the eos request,
// and on each writer node from the tablets channel, which receives it, to the delta writers, which record
// it in the rowset. txn_id is the key because it is the one load identifier that the scanner
// (TBrokerScanRangeParams.txn_id), the sink and the writers all see.
//
// An entry is reference-counted by its holders, because several pipelines of one node share a txn_id and
// none of them can tell it is the last: every json reader of the load, the sink of each plan (which must
// outlive the readers because it ships the dictionary on eos), and every tablets channel that received
// the dictionary (its writers read it when they finish). Each holder calls retain() when it starts using
// the entry and release() when it is done, and the entry is dropped with the last release().
class FlexiblePartialUpdateRegistry {
public:
    static FlexiblePartialUpdateRegistry* instance();

    // Takes a reference to the load's dictionary, creating it if absent. Every retain() must be balanced by
    // exactly one release(txn_id) from the same holder.
    ColumnSetDictPtr retain(int64_t txn_id);

    // Gives back a reference taken by retain(). The entry is dropped when no reference is left. A release
    // without a matching retain is a no-op.
    void release(int64_t txn_id);

    // Returns the load's dictionary, creating it if absent, WITHOUT taking a reference: the entry lives only
    // as long as some holder keeps it retained.
    ColumnSetDictPtr get_or_create(int64_t txn_id);

    // Returns the load's dictionary, or nullptr if there is none.
    ColumnSetDictPtr get(int64_t txn_id);

    // Drops the load's dictionary regardless of outstanding references. For tests.
    void erase(int64_t txn_id);

private:
    struct Entry {
        ColumnSetDictPtr dict;
        int64_t refs = 0;
    };
    std::mutex _mu;
    std::unordered_map<int64_t, Entry> _by_txn;
};

} // namespace starrocks
