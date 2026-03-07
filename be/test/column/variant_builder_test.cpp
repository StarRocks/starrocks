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

#include "column/variant_builder.h"

#include <gtest/gtest.h>

#include "base/testutil/parallel_test.h"
#include "column/variant_encoder.h"

namespace starrocks {

// Helper: parse a non-empty shredded-path string into a VariantPath for use in Overlay literals.
static VariantPath MakePath(const char* s) {
    return VariantPathParser::parse_shredded_path(std::string_view(s)).value();
}

// Verifies overlays without base can assemble one object row.
PARALLEL_TEST(VariantBuilderTest, build_from_overlays_only) {
    VariantBuilder builder;
    auto a = VariantEncoder::encode_json_text_to_variant("99");
    ASSERT_TRUE(a.ok());
    auto b = VariantEncoder::encode_json_text_to_variant("\"x\"");
    ASSERT_TRUE(b.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a"), .value = std::move(a.value())},
            {.path = MakePath("b"), .value = std::move(b.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":99,"b":"x"})", json.value());
}

// Verifies overlays with common prefixes can be rebuilt without base.
PARALLEL_TEST(VariantBuilderTest, build_from_overlays_common_prefix) {
    VariantBuilder builder;
    auto b = VariantEncoder::encode_json_text_to_variant("1");
    ASSERT_TRUE(b.ok());
    auto c = VariantEncoder::encode_json_text_to_variant("2");
    ASSERT_TRUE(c.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a.b"), .value = std::move(b.value())},
            {.path = MakePath("a.c"), .value = std::move(c.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"b":1,"c":2}})", json.value());
}

// Verifies overlapped paths can be merged in one result tree.
PARALLEL_TEST(VariantBuilderTest, build_with_overlapped_paths) {
    VariantBuilder builder;
    auto a = VariantEncoder::encode_json_text_to_variant(R"({"x":1})");
    ASSERT_TRUE(a.ok());
    auto b = VariantEncoder::encode_json_text_to_variant("2");
    ASSERT_TRUE(b.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a"), .value = std::move(a.value())},
            {.path = MakePath("a.b"), .value = std::move(b.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"b":2,"x":1}})", json.value());
}

// Verifies explicit skip overlay keeps base value.
PARALLEL_TEST(VariantBuilderTest, null_overlay_keeps_base_value) {
    VariantBuilder base_builder;
    auto base_value = VariantEncoder::encode_json_text_to_variant(R"({"a":1,"b":2})");
    ASSERT_TRUE(base_value.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = VariantPath{}, .value = std::move(base_value.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base = base_builder.build();
    ASSERT_TRUE(base.ok());

    VariantBuilder builder(&base.value());
    // No overlays: typed null for "a" means skip — caller simply omits the overlay.
    ASSERT_TRUE(builder.set_overlays({}).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":1,"b":2})", json.value());
}

// Verifies NULL_TYPE overlay with default mode writes explicit JSON null.
PARALLEL_TEST(VariantBuilderTest, null_overlay_sets_json_null) {
    VariantBuilder base_builder;
    auto base_value = VariantEncoder::encode_json_text_to_variant(R"({"a":1,"b":2})");
    ASSERT_TRUE(base_value.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = VariantPath{}, .value = std::move(base_value.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base = base_builder.build();
    ASSERT_TRUE(base.ok());

    VariantBuilder builder(&base.value());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a"), .value = VariantRowValue::from_null()},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":null,"b":2})", json.value());
}

// Verifies assigning parent path after child path overrides prior subtree.
PARALLEL_TEST(VariantBuilderTest, parent_overlay_overrides_child_overlay) {
    VariantBuilder builder;
    auto child = VariantEncoder::encode_json_text_to_variant("1");
    ASSERT_TRUE(child.ok());
    auto parent = VariantEncoder::encode_json_text_to_variant(R"({"x":2})");
    ASSERT_TRUE(parent.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a.b"), .value = std::move(child.value())},
            {.path = MakePath("a"), .value = std::move(parent.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"x":2}})", json.value());
}

// Verifies object child overlay can overwrite a scalar parent from base row.
PARALLEL_TEST(VariantBuilderTest, object_overlay_replaces_scalar_base) {
    VariantBuilder base_builder;
    auto base_value = VariantEncoder::encode_json_text_to_variant(R"({"a":1})");
    ASSERT_TRUE(base_value.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = VariantPath{}, .value = std::move(base_value.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base = base_builder.build();
    ASSERT_TRUE(base.ok());

    VariantBuilder builder(&base.value());
    auto overlay_value = VariantEncoder::encode_json_text_to_variant("3");
    ASSERT_TRUE(overlay_value.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a.b"), .value = std::move(overlay_value.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"b":3}})", json.value());
}

// Verifies root path overlay replaces whole root.
PARALLEL_TEST(VariantBuilderTest, root_overlay_replaces_whole_root) {
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"a":1})");
    ASSERT_TRUE(base.ok());
    VariantBuilder builder(&base.value());
    auto root = VariantEncoder::encode_json_text_to_variant(R"({"x":[1,2]})");
    ASSERT_TRUE(root.ok());

    std::vector<VariantBuilder::Overlay> overlays{
            {.path = VariantPath{}, .value = std::move(root.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());
    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"x":[1,2]})", json.value());
}

// Verifies overlays on same path use last-wins semantics.
PARALLEL_TEST(VariantBuilderTest, same_path_last_overlay_wins) {
    VariantBuilder builder;
    auto first = VariantEncoder::encode_json_text_to_variant("1");
    ASSERT_TRUE(first.ok());
    auto second = VariantEncoder::encode_json_text_to_variant("2");
    ASSERT_TRUE(second.ok());

    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a"), .value = std::move(first.value())},
            {.path = MakePath("a"), .value = std::move(second.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());
    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":2})", json.value());
}

// Verifies typed variant overlay is remapped by semantic decode/encode, not raw dict-id reuse.
PARALLEL_TEST(VariantBuilderTest, variant_overlay_dict_namespace_remap) {
    VariantBuilder base_builder;
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"p":0})");
    ASSERT_TRUE(base.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = VariantPath{}, .value = std::move(base.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base_row = base_builder.build();
    ASSERT_TRUE(base_row.ok());

    VariantBuilder builder(&base_row.value());
    auto overlay_obj = VariantEncoder::encode_json_text_to_variant(R"({"x":1,"y":2})");
    ASSERT_TRUE(overlay_obj.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("a"), .value = std::move(overlay_obj.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":{"x":1,"y":2},"p":0})", json.value());
}

// Verifies base object key order is preserved and new overlay keys append in encounter order.
PARALLEL_TEST(VariantBuilderTest, object_key_order_stable_with_overlay_append) {
    VariantBuilder base_builder;
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"b":1,"a":2})");
    ASSERT_TRUE(base.ok());
    std::vector<VariantBuilder::Overlay> base_overlays{
            {.path = VariantPath{}, .value = std::move(base.value())},
    };
    ASSERT_TRUE(base_builder.set_overlays(std::move(base_overlays)).ok());
    auto base_row = base_builder.build();
    ASSERT_TRUE(base_row.ok());

    VariantBuilder builder(&base_row.value());
    auto c = VariantEncoder::encode_json_text_to_variant("3");
    ASSERT_TRUE(c.ok());
    auto d = VariantEncoder::encode_json_text_to_variant("4");
    ASSERT_TRUE(d.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("c"), .value = std::move(c.value())},
            {.path = MakePath("d"), .value = std::move(d.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"a":2,"b":1,"c":3,"d":4})", json.value());
}

PARALLEL_TEST(VariantBuilderTest, deep_overlay_with_array_and_object) {
    VariantBuilder builder;
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"root":{"arr":[{"x":1}]}})");
    ASSERT_TRUE(base.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = VariantPath{}, .value = std::move(base.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());
    auto base_row = builder.build();
    ASSERT_TRUE(base_row.ok());

    VariantBuilder patch_builder(&base_row.value());
    auto arr = VariantEncoder::encode_json_text_to_variant(R"([{"x":1,"y":2}])");
    ASSERT_TRUE(arr.ok());
    auto z = VariantEncoder::encode_json_text_to_variant(R"({"k":"v"})");
    ASSERT_TRUE(z.ok());
    std::vector<VariantBuilder::Overlay> patch{
            {.path = MakePath("root.arr"), .value = std::move(arr.value())},
            {.path = MakePath("root.obj"), .value = std::move(z.value())},
    };
    ASSERT_TRUE(patch_builder.set_overlays(std::move(patch)).ok());

    auto out = patch_builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"root":{"arr":[{"x":1,"y":2}],"obj":{"k":"v"}}})", json.value());
}


// =============================================================================
// Tests for array overlay scenarios
// Covers the logic exercised by _rebuild_array_overlay and
// _collect_overlays_for_array_element in complex_column_reader.cpp.
// Those functions are private statics and cannot be called directly; these
// tests validate the VariantBuilder / VariantArrayBuilder building blocks they
// delegate to.
// =============================================================================

// BUG-2 scenario: _collect_overlays_for_array_element previously ignored
// ShreddedTypedKind::ARRAY child nodes. After the fix it calls
// _rebuild_array_overlay and passes the resulting VariantRowValue as an overlay
// at the field sub-path.  These tests verify that VariantBuilder correctly
// handles such array-value overlays.

// Verifies that an array Variant can be applied as an overlay at a sub-path.
PARALLEL_TEST(VariantBuilderTest, array_value_overlay_at_subpath) {
    VariantBuilder builder;
    auto tags = VariantEncoder::encode_json_text_to_variant("[1,2,3]");
    ASSERT_TRUE(tags.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("tags"), .value = std::move(tags.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"({"tags":[1,2,3]})", json.value());
}

// Verifies that an array overlay at a sub-path is correctly merged with a
// non-shredded base object (the "value" binary that holds leftover fields).
// Simulates: object element where "name" is non-shredded and "tags" is a
// shredded array reconstructed by _rebuild_array_overlay via the BUG-2 fix.
PARALLEL_TEST(VariantBuilderTest, array_value_overlay_merged_with_partial_shred_base) {
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"name":"foo"})");
    ASSERT_TRUE(base.ok());
    VariantBuilder builder(&base.value());

    auto tags = VariantEncoder::encode_json_text_to_variant("[10,20,30]");
    ASSERT_TRUE(tags.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("tags"), .value = std::move(tags.value())},
    };
    ASSERT_TRUE(builder.set_overlays(std::move(overlays)).ok());

    auto out = builder.build();
    ASSERT_TRUE(out.ok());
    auto json = out->to_json();
    ASSERT_TRUE(json.ok());
    // "name" from base + "tags" from the reconstructed array overlay.
    ASSERT_EQ(R"({"name":"foo","tags":[10,20,30]})", json.value());
}

// Verifies nested array-of-objects where each element contains an array field.
// Represents: array element like {"id":1,"scores":[90,85]} where "scores" is a
// shredded array reconstructed by the BUG-2 fix and applied as a sub-path overlay.
PARALLEL_TEST(VariantBuilderTest, array_value_overlay_inside_object_element) {
    // Build element: {id:1} base + scores:[90,85] overlay (shredded array sub-field).
    auto base = VariantEncoder::encode_json_text_to_variant(R"({"id":1})");
    ASSERT_TRUE(base.ok());
    VariantBuilder elem_builder(&base.value());

    auto scores = VariantEncoder::encode_json_text_to_variant("[90,85]");
    ASSERT_TRUE(scores.ok());
    std::vector<VariantBuilder::Overlay> overlays{
            {.path = MakePath("scores"), .value = std::move(scores.value())},
    };
    ASSERT_TRUE(elem_builder.set_overlays(std::move(overlays)).ok());
    auto elem = elem_builder.build();
    ASSERT_TRUE(elem.ok());

    // Assemble into an array-of-objects using VariantArrayBuilder.
    VariantArrayBuilder array_builder;
    array_builder.add(std::move(elem.value()));
    auto result = array_builder.build();
    ASSERT_TRUE(result.ok());
    auto json = result->to_json();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(R"([{"id":1,"scores":[90,85]}])", json.value());
}

// =============================================================================
// BUG-1 scenario: _rebuild_array_overlay Path 1 previously dropped base_element
// on VariantBuilder::build() failure, calling array_builder.add_null() instead
// of array_builder.add(base_element->to_owned()).
//
// The fix is in a private static function and cannot be called directly.
// The two tests below verify:
//   (a) the CORRECT output (with base_element preserved instead of null), and
//   (b) what the BUGGY output looked like (null in place of the base element),
// so reviewers can see exactly what data was lost before the fix.
// =============================================================================

// Verifies that VariantArrayBuilder preserves all valid base elements.
// After BUG-1 fix, _rebuild_array_overlay adds base_element->to_owned() when
// VariantBuilder::build() fails, rather than add_null().
PARALLEL_TEST(VariantBuilderTest, array_builder_base_element_preserved_not_null) {
    VariantArrayBuilder builder;

    auto e0 = VariantEncoder::encode_json_text_to_variant(R"({"type":"A"})");
    ASSERT_TRUE(e0.ok());
    builder.add(std::move(e0.value()));

    // Represents the fallback base element that BUG-1 fix now uses instead of null.
    auto e1_base = VariantEncoder::encode_json_text_to_variant(R"({"type":"B","extra":99})");
    ASSERT_TRUE(e1_base.ok());
    builder.add(std::move(e1_base.value())); // fixed: not add_null()

    auto e2 = VariantEncoder::encode_json_text_to_variant(R"({"type":"C"})");
    ASSERT_TRUE(e2.ok());
    builder.add(std::move(e2.value()));

    auto result = builder.build();
    ASSERT_TRUE(result.ok());
    auto json = result->to_json();
    ASSERT_TRUE(json.ok());
    // Fixed output: base element preserved.  Buggy output was [.., null, ..].
    ASSERT_EQ(R"([{"type":"A"},{"extra":99,"type":"B"},{"type":"C"}])", json.value());
}

// Shows the data loss that occurred BEFORE the BUG-1 fix: when the per-element
// VariantBuilder::build() failed, the entire element was replaced with null,
// even though a valid base_element binary was available.
PARALLEL_TEST(VariantBuilderTest, array_builder_null_element_demonstrates_bug1_data_loss) {
    VariantArrayBuilder builder;

    auto e0 = VariantEncoder::encode_json_text_to_variant(R"({"type":"A"})");
    ASSERT_TRUE(e0.ok());
    builder.add(std::move(e0.value()));

    builder.add_null(); // what the old buggy code did: dropped base_element

    auto e2 = VariantEncoder::encode_json_text_to_variant(R"({"type":"C"})");
    ASSERT_TRUE(e2.ok());
    builder.add(std::move(e2.value()));

    auto result = builder.build();
    ASSERT_TRUE(result.ok());
    auto json = result->to_json();
    ASSERT_TRUE(json.ok());
    // This is the buggy output: element[1] is null, data lost.
    ASSERT_EQ(R"([{"type":"A"},null,{"type":"C"}])", json.value());
}

} // namespace starrocks
