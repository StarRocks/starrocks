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

// Differential test of the two parquet schema resolvers:
// - files() inference: get_parquet_type() (connector/file/scanner/parquet_schema_builder.cpp) on Arrow schema nodes;
// - native reader: SchemaDescriptor::from_thrift() (formats/parquet/schema.cpp) on the thrift footer.
// For every *.parquet under be/test/ and test/, the nested shape of every top-level column (ARRAY / MAP / STRUCT /
// VARIANT structure and struct field names) must agree. Scalar types may differ: column_converter maps the file type
// to the table type. Intentional differences are listed in kKnownDifferences; anything else fails and prints both
// shapes plus a line to paste into the list.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/util/thrift_util.h"
#include "connector/file/scanner/parquet_schema_builder.h"
#include "formats/parquet/schema.h"
#include "gen_cpp/parquet_types.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

// Key: "<path relative to STARROCKS_HOME>#<top-level column>", or "<path>#*" when one side cannot resolve the file.
const std::map<std::string, std::string> kKnownDifferences = {
        // Legacy two-level list whose repeated group is itself named "array" or "<name>_tuple": native follows the
        // parquet-format backward-compatibility rule (and Arrow) and reads the repeated group as a struct element;
        // inference reads a LIST-annotated repeated group with a repeated child as a nested list.
        {"be/test/exec/test_data/jni_scanner/test_hudi_mor_ams/"
         "0df0196b-f46f-43f5-8cf0-06fad7143af3-0_0-27-35_20230110191854854.parquet#c",
         "legacy list: repeated group named 'array'"},
        {"be/test/exec/test_data/parquet_data/hudi_mor_two_level_nested_array.parquet#c",
         "legacy list: repeated group named 'array'"},
        {"test/sql/test_files/parquet_format/list_legacy_encoding_complex.parquet#field1",
         "legacy list: repeated group named '<name>_tuple'"},
        {"test/sql/test_files/parquet_format/list_legacy_encoding_nested.parquet#a",
         "legacy list: repeated group named 'array'"},
        // MAP-annotated group with two children (written by Hudi). Native rejects the file; inference DCHECKs the
        // shape instead of returning an error, so the test does not call it (see check_inference_preconditions).
        {"be/test/exec/test_data/parquet_scanner/hudi_array_map.parquet#*",
         "malformed MAP group: native rejects, inference only DCHECKs"},
};

std::string native_shape(const parquet::ParquetField& field) {
    switch (field.type) {
    case parquet::ColumnType::SCALAR:
        return "S";
    case parquet::ColumnType::VARIANT:
        return "VARIANT";
    case parquet::ColumnType::ARRAY:
        return "ARRAY<" + native_shape(field.children[0]) + ">";
    case parquet::ColumnType::MAP:
        return "MAP<" + native_shape(field.children[0]) + "," + native_shape(field.children[1]) + ">";
    case parquet::ColumnType::STRUCT: {
        std::string out = "STRUCT<";
        for (size_t i = 0; i < field.children.size(); ++i) {
            if (i > 0) out += ",";
            out += field.children[i].name + ":" + native_shape(field.children[i]);
        }
        return out + ">";
    }
    }
    return "?";
}

std::string inferred_shape(const TypeDescriptor& type) {
    switch (type.type) {
    case TYPE_VARIANT:
        return "VARIANT";
    case TYPE_ARRAY:
        return "ARRAY<" + inferred_shape(type.children[0]) + ">";
    case TYPE_MAP:
        return "MAP<" + inferred_shape(type.children[0]) + "," + inferred_shape(type.children[1]) + ">";
    case TYPE_STRUCT: {
        std::string out = "STRUCT<";
        for (size_t i = 0; i < type.children.size(); ++i) {
            if (i > 0) out += ",";
            out += type.field_names[i] + ":" + inferred_shape(type.children[i]);
        }
        return out + ">";
    }
    default:
        return "S";
    }
}

// get_parquet_type() only DCHECKs the MAP layout; report a malformed MAP instead of calling it.
Status check_inference_preconditions(const ::parquet::schema::NodePtr& node) {
    if (!node->is_group()) return Status::OK();
    auto group = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    if (node->logical_type()->is_map()) {
        if (group->field_count() != 1 || !group->field(0)->is_group() ||
            std::static_pointer_cast<::parquet::schema::GroupNode>(group->field(0))->field_count() != 2) {
            return Status::InvalidArgument("malformed MAP group " + node->name() + " (inference would DCHECK)");
        }
    }
    for (int i = 0; i < group->field_count(); ++i) {
        RETURN_IF_ERROR(check_inference_preconditions(group->field(i)));
    }
    return Status::OK();
}

using Shapes = std::vector<std::pair<std::string, std::string>>; // (column name, shape)

StatusOr<Shapes> resolve_native(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (data.size() < 12 || data.compare(data.size() - 4, 4, "PAR1") != 0) {
        return Status::Corruption("not a parquet file");
    }
    uint32_t metadata_length = 0;
    memcpy(&metadata_length, data.data() + data.size() - 8, 4);
    if (metadata_length > data.size() - 8) return Status::Corruption("bad footer length");
    tparquet::FileMetaData t_metadata;
    RETURN_IF_ERROR(
            deserialize_thrift_msg(reinterpret_cast<const uint8_t*>(data.data()) + data.size() - 8 - metadata_length,
                                   &metadata_length, TProtocolType::COMPACT, &t_metadata));
    parquet::SchemaDescriptor schema;
    RETURN_IF_ERROR(schema.from_thrift(t_metadata.schema, true));
    Shapes shapes;
    for (const auto& field : schema.get_parquet_fields()) {
        shapes.emplace_back(field.name, native_shape(field));
    }
    return shapes;
}

StatusOr<Shapes> resolve_inferred(const std::string& path) {
    std::shared_ptr<::parquet::FileMetaData> metadata;
    try {
        metadata = ::parquet::ParquetFileReader::OpenFile(path, false)->metadata();
    } catch (const std::exception& e) {
        return Status::Corruption(std::string("arrow: ") + e.what());
    }
    const auto* root = metadata->schema()->group_node();
    Shapes shapes;
    for (int i = 0; i < root->field_count(); ++i) {
        const auto& node = root->field(i);
        RETURN_IF_ERROR(check_inference_preconditions(node));
        TypeDescriptor type;
        RETURN_IF_ERROR(get_parquet_type(node, &type));
        shapes.emplace_back(node->name(), inferred_shape(type));
    }
    return shapes;
}

std::vector<std::string> collect_parquet_files(const std::filesystem::path& home) {
    std::vector<std::string> files;
    for (const char* dir : {"be/test", "test"}) {
        std::error_code ec;
        for (auto it = std::filesystem::recursive_directory_iterator(home / dir, ec);
             !ec && it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
            if (it->is_regular_file() && it->path().extension() == ".parquet") {
                files.emplace_back(std::filesystem::relative(it->path(), home).string());
            }
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

} // namespace

TEST(ParquetSchemaConsistencyTest, InferenceMatchesNativeResolution) {
    const char* home_env = getenv("STARROCKS_HOME");
    ASSERT_NE(nullptr, home_env);
    const std::filesystem::path home(home_env);
    const auto files = collect_parquet_files(home);
    ASSERT_GT(files.size(), 100) << "expected the parquet test data under be/test and test/";

    std::set<std::string> seen_known;
    auto report = [&](const std::string& key, const std::string& native, const std::string& inferred) {
        if (kKnownDifferences.count(key) > 0) {
            seen_known.insert(key);
            return;
        }
        ADD_FAILURE() << "parquet schema resolvers disagree on " << key << "\n  native:    " << native
                      << "\n  inference: " << inferred << "\n  allowlist: {\"" << key << "\", \"<reason>\"},";
    };

    for (const auto& file : files) {
        auto native = resolve_native((home / file).string());
        auto inferred = resolve_inferred((home / file).string());
        if (!native.ok() && !inferred.ok()) {
            continue; // not a readable parquet file for either side (e.g. empty.parquet)
        }
        if (!native.ok() || !inferred.ok()) {
            report(file + "#*", native.ok() ? "ok" : native.status().to_string(),
                   inferred.ok() ? "ok" : inferred.status().to_string());
            continue;
        }
        if (native->size() != inferred->size()) {
            report(file + "#*", std::to_string(native->size()) + " columns",
                   std::to_string(inferred->size()) + " columns");
            continue;
        }
        for (size_t i = 0; i < native->size(); ++i) {
            const auto& [native_name, native_shape_str] = (*native)[i];
            const auto& [inferred_name, inferred_shape_str] = (*inferred)[i];
            if (native_name != inferred_name || native_shape_str != inferred_shape_str) {
                report(file + "#" + native_name, native_name + ": " + native_shape_str,
                       inferred_name + ": " + inferred_shape_str);
            }
        }
    }

    // An entry that no longer differs should be removed (e.g. after a resolver fix); not a failure, so that this
    // test does not block the fix itself.
    for (const auto& [key, reason] : kKnownDifferences) {
        if (seen_known.count(key) == 0) {
            LOG(WARNING) << "stale known parquet schema difference, remove it: " << key << " (" << reason << ")";
        }
    }
}

} // namespace starrocks
