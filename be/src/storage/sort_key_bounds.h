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

#include <cstddef>
#include <string>

#include "base/string/slice.h"
#include "common/status.h"
#include "storage/datum_variant.h"
#include "storage/variant_tuple.h"

namespace starrocks {

class SegmentMetadataPB;

// Compact, self-describing encoding of a segment's sort key bound (its minimum or maximum sort
// key), stored in SegmentMetadataPB.sort_key_min_encoded / sort_key_max_encoded.
//
// Format version 1:
//
//   bound  := header value{count}
//   header := version   u8, always 1
//             count     u16, big-endian; 1 <= count <= number of bytes after it
//             column{count}
//   column := type      u8, the column's LogicalType
//             precision u8 + scale u8, only for TYPE_DECIMAL32/64/128/256, taken from the
//                       column's TypeInfo; 1 <= precision <= 9/18/38/76, scale <= precision
//   value  := 0x00                NULL
//           | 0x01 payload        non-NULL, payload by column type:
//
//   CHAR                the value cut at its first 0x00 byte (the same normalization the
//                       TuplePB decode applies), then escaped like VARCHAR
//   VARCHAR, VARBINARY  every 0x00 byte written as 0x00 0x01, then the terminator 0x00 0x00
//   DATE, DATETIME      the value the TuplePB text round trip yields, in the KeyCoder encoding;
//                       a value the text form cannot express, such as the zero date, comes back
//                       as 1400-01-01 there, so it is encoded as 1400-01-01 here
//   BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DECIMALV2, DECIMAL32/64/128/256
//                       the KeyCoder full ascending encoding (fixed width)
//
// No other type is supported. A decoded bound has the same logical types, decimal
// precision/scale and values as the TuplePB round trip of the tuple it was encoded from.
// For two bounds with identical headers, memcmp of the encoded bytes orders them the way
// VariantTuple::compare orders the decoded tuples (NULL first).

// Replaces *out with the encoding of `tuple`. Returns InvalidArgument for an empty tuple and
// NotSupported for more than 65535 columns, a column of an unsupported type, or a decimal column
// whose precision/scale is out of range. *out is unchanged on error.
Status encode_sort_key_bound(const VariantTuple& tuple, std::string* out);

// Decodes a bound written by encode_sort_key_bound into *out. Returns Corruption for any input
// that does not follow the format exactly, trailing bytes included. *out is unchanged on error.
Status decode_sort_key_bound(const Slice& encoded, VariantTuple* out);

// Helpers for the bounds of a SegmentMetadataPB, which stores them either as the encoded pair
// (sort_key_min_encoded / sort_key_max_encoded) or as the deprecated TuplePB pair.
//
// Read precedence:
//   1. If both deprecated fields are present, they are used. A deprecated pair whose tuples have no
//      values means "no bounds" and hides any encoded pair.
//   2. Otherwise, if both encoded fields are present and both start with format version 1, they are used.
//   3. Otherwise the segment has no bounds.
// An encoded pair in a format version this binary does not know (written by a later version before a
// rollback) therefore counts as no bounds. Damage anywhere else makes the decoders below return Corruption,
// and callers treat a bound that does not decode as no bounds for that segment: metadata pruning keeps the
// segment, range-split compaction falls back, and tablet split falls back to an identical tablet.
// New writers always clear the deprecated pair when they write the encoded one, so both pairs being
// present means an older binary refreshed the deprecated pair after the encoded pair was written
// (the encoded fields survive in it as unknown fields). The deprecated pair is then the newer one.

// Stores the bounds as the encoded pair and clears the deprecated pair. If either tuple is empty,
// clears all four fields. If a tuple cannot be encoded (unsupported column type), stores the
// deprecated pair instead, clears the encoded pair and logs a rate-limited warning.
// Meant for the declaration of a newly written segment. Tablet merge requires all declarations of
// one physical segment to be byte-identical (physical_base_key in tablet_merger.cpp), so the bounds
// of an existing declaration must not be rewritten in the other representation.
void set_segment_sort_key_bounds(const VariantTuple& min, const VariantTuple& max, SegmentMetadataPB* meta);

// True if the representation chosen by the read precedence holds non-empty bounds. Only the format
// version of an encoded bound is read, so an encoded bound accepted here may still fail to decode.
bool has_segment_sort_key_bounds(const SegmentMetadataPB& meta);

// True if both bounds of the chosen representation have exactly `arity` columns. For the encoded
// form only the column count in the header is read.
bool segment_sort_key_bounds_have_arity(const SegmentMetadataPB& meta, size_t arity);

// Decodes both bounds. A segment without bounds yields two empty tuples and OK. Returns Corruption
// for an encoded bound whose header or values are malformed. *min and *max are unchanged on error.
Status decode_segment_sort_key_bounds(const SegmentMetadataPB& meta, VariantTuple* min, VariantTuple* max);

// Decodes only the first column of each bound. Returns NotFound if the segment has no bounds and
// Corruption for an encoded bound whose header or first value is malformed. *min and *max are
// unchanged on error.
Status decode_segment_leading_sort_key_bounds(const SegmentMetadataPB& meta, DatumVariant* min, DatumVariant* max);

} // namespace starrocks
