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

#include "exec/file_scanner/avro_datum_path.h"

#include <avrocpp/GenericDatum.hh>
#include <avrocpp/Types.hh>

#include "common/status.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

static void replace_all(std::string& str, const std::string& from, const std::string& to) {
    size_t pos = 0;
    while ((pos = str.find(from, pos)) != std::string::npos) {
        str.replace(pos, from.size(), to);
        pos += to.size();
    }
}

std::string avro_preprocess_jsonpaths(std::string jsonpaths) {
    replace_all(jsonpaths, ".*.", ".");
    replace_all(jsonpaths, ".*", "");
    return jsonpaths;
}

// A nullable Avro field is encoded as a union; the decoded GenericDatum holds the selected branch.
// Peel unions until a non-union (possibly null) branch is reached. Avro forbids unions of unions, so
// the loop normally runs at most once; it stays a loop only for defensiveness.
static const avro::GenericDatum* unwrap_union(const avro::GenericDatum* datum) {
    while (datum->type() == avro::AVRO_UNION) {
        datum = &datum->value<avro::GenericUnion>().datum();
    }
    return datum;
}

static StatusOr<const avro::GenericDatum*> array_element(const avro::GenericDatum* datum, int idx) {
    const auto& elements = datum->value<avro::GenericArray>().value();
    // idx < 0 covers the wildcard [*] (idx == -2), which is intentionally unsupported.
    if (idx < 0 || static_cast<size_t>(idx) >= elements.size()) {
        return Status::InternalError(
                strings::Substitute("array index $0 out of range (size $1)", idx, elements.size()));
    }
    return &elements[idx];
}

// Looks up `key` in a record (by field name) or a map (linear scan of key/value pairs). avrocpp's
// GenericMap has no by-key accessor, hence the linear scan; map lookups via jsonpath are rare.
static StatusOr<const avro::GenericDatum*> lookup(const avro::GenericDatum* datum, const std::string& key) {
    if (datum->type() == avro::AVRO_RECORD) {
        const auto& record = datum->value<avro::GenericRecord>();
        if (!record.hasField(key)) {
            return Status::NotFound(strings::Substitute("field not found: $0", key));
        }
        return &record.field(key);
    }
    // AVRO_MAP
    for (const auto& entry : datum->value<avro::GenericMap>().value()) {
        if (entry.first == key) {
            return &entry.second;
        }
    }
    return Status::NotFound(strings::Substitute("map key not found: $0", key));
}

StatusOr<const avro::GenericDatum*> avro_extract_field(const avro::GenericDatum& root,
                                                       const std::vector<SimpleJsonPath>& path) {
    const avro::GenericDatum* cur = &root;

    // Whole-datum selector "$": return it (after peeling a top-level union).
    if (path.size() == 1 && path[0].key == "$" && path[0].idx == -1) {
        return unwrap_union(cur);
    }

    // Peel a top-level union before the root checks, so a nullable top-level array still takes the
    // root-index branch below; a null root value makes every path NULL.
    cur = unwrap_union(cur);
    if (cur->type() == avro::AVRO_NULL) {
        return cur;
    }

    if (cur->type() == avro::AVRO_ARRAY) {
        // Root is an array: only $[i] selects into it.
        if (path[0].key != "$" || path[0].idx < 0) {
            return Status::InternalError("the avro root type is an array, select a specific array element");
        }
        ASSIGN_OR_RETURN(cur, array_element(cur, path[0].idx));
    } else if (path[0].idx != -1) {
        // Deliberate divergence from the legacy reader, which silently ignores a root array index on
        // a non-array root and returns the whole datum (e.g. $[0] on a record yields the record).
        return Status::InternalError("jsonpath has a root array index but the avro root value is not an array");
    }

    // path[0] is "$"; descend through the remaining segments.
    for (size_t i = 1; i < path.size(); ++i) {
        cur = unwrap_union(cur);
        if (cur->type() == avro::AVRO_NULL) {
            return cur;
        }
        const auto type = cur->type();
        if (type != avro::AVRO_RECORD && type != avro::AVRO_MAP) {
            // Deliberate divergence from the legacy AvroScanner::_extract_field, which on the LAST
            // segment returns the scalar itself (e.g. $.id.x on a long `id` silently yields `id`).
            // The native reader is opt-in, so a path that overshoots a leaf fails loudly instead of
            // loading the parent value into the wrong column.
            return Status::InternalError(strings::Substitute(
                    "jsonpath segment '$0' descends into a non-record/map avro value", path[i].key));
        }
        ASSIGN_OR_RETURN(cur, lookup(cur, path[i].key));

        if (path[i].idx != -1) {
            cur = unwrap_union(cur);
            if (cur->type() == avro::AVRO_NULL) {
                return cur;
            }
            if (cur->type() != avro::AVRO_ARRAY) {
                return Status::InternalError("a non-array type was found where an array index was expected");
            }
            ASSIGN_OR_RETURN(cur, array_element(cur, path[i].idx));
        }
    }

    return unwrap_union(cur);
}

} // namespace starrocks
