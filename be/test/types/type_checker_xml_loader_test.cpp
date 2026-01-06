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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "types/checker/type_checker_xml_loader.h"

namespace starrocks {

class TypeCheckerXMLLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory for temporary XML files
        test_dir_ = "/tmp/type_checker_test";
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        // Clean up test files
        std::filesystem::remove_all(test_dir_);
    }

    void create_test_xml(const std::string& filename, const std::string& content) {
        std::ofstream file(filename);
        file << content;
        file.close();
    }

    std::string test_dir_;
};

// Test loading valid XML configuration
TEST_F(TypeCheckerXMLLoaderTest, LoadValidXML) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Integer" display_name="Integer">
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Boolean" display_name="Boolean">
    <type-rule allowed_type="TYPE_BOOLEAN" return_type="TYPE_BOOLEAN"/>
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/valid.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_TRUE(result.ok());

    const auto& mappings = result.value();
    ASSERT_EQ(mappings.size(), 3);

    EXPECT_EQ(mappings[0].java_class, "java.lang.String");
    EXPECT_EQ(mappings[0].display_name, "String");
    EXPECT_EQ(mappings[0].rules.size(), 1);

    EXPECT_EQ(mappings[1].java_class, "java.lang.Integer");
    EXPECT_EQ(mappings[1].display_name, "Integer");
    EXPECT_EQ(mappings[1].rules.size(), 1);

    EXPECT_EQ(mappings[2].java_class, "java.lang.Boolean");
    EXPECT_EQ(mappings[2].display_name, "Boolean");
    EXPECT_EQ(mappings[2].rules.size(), 1);
}

// Test loading XML with comments
TEST_F(TypeCheckerXMLLoaderTest, LoadXMLWithComments) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<!-- This is a comment -->
<type-checkers>
  <!-- Another comment -->
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
  <!-- Yet another comment -->
  <type-mapping java_class="java.lang.Integer" display_name="Integer">
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/with_comments.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_TRUE(result.ok());

    const auto& mappings = result.value();
    ASSERT_EQ(mappings.size(), 2);
}

// Test loading XML with whitespace and formatting variations
TEST_F(TypeCheckerXMLLoaderTest, LoadXMLWithWhitespace) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
    <type-mapping java_class="java.lang.String"     display_name="String">
      <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
    </type-mapping>
  <type-mapping   java_class="java.lang.Integer"   display_name="Integer">
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
  </type-mapping>
<type-mapping java_class="java.lang.Boolean" display_name="Boolean">
  <type-rule allowed_type="TYPE_BOOLEAN" return_type="TYPE_BOOLEAN"/>
</type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/whitespace.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_TRUE(result.ok());

    const auto& mappings = result.value();
    ASSERT_EQ(mappings.size(), 3);
}

// Test loading non-existent XML file
TEST_F(TypeCheckerXMLLoaderTest, LoadNonExistentFile) {
    auto result = TypeCheckerXMLLoader::load_from_xml("/tmp/type_checker_test/nonexistent.xml");
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_found());
}

// Test loading malformed XML (missing required attributes)
TEST_F(TypeCheckerXMLLoaderTest, LoadMalformedXMLMissingAttributes) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.String">
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
  <type-mapping display_name="Integer">
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/malformed_attrs.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

// Test loading empty XML file
TEST_F(TypeCheckerXMLLoaderTest, LoadEmptyXML) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/empty.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

// Test creating checker from mapping
TEST_F(TypeCheckerXMLLoaderTest, CreateCheckerFromMapping) {
    TypeCheckerXMLLoader::TypeMapping mapping;
    mapping.java_class = "java.lang.Integer";
    mapping.display_name = "Integer";
    mapping.is_configurable = true;
    mapping.rules = {
        {TYPE_TINYINT, TYPE_INT},
        {TYPE_SMALLINT, TYPE_INT},
        {TYPE_INT, TYPE_INT},
        {TYPE_BIGINT, TYPE_INT}
    };

    auto checker = TypeCheckerXMLLoader::create_checker_from_mapping(mapping);
    ASSERT_NE(checker, nullptr);
}

// Test creating checker from mapping with multiple rules
TEST_F(TypeCheckerXMLLoaderTest, CreateCheckerWithMultipleRules) {
    TypeCheckerXMLLoader::TypeMapping mapping;
    mapping.java_class = "com.clickhouse.data.value.UnsignedByte";
    mapping.display_name = "UnsignedByte";
    mapping.is_configurable = true;
    mapping.rules = {
        {TYPE_SMALLINT, TYPE_SMALLINT},
        {TYPE_INT, TYPE_SMALLINT},
        {TYPE_BIGINT, TYPE_SMALLINT}
    };

    auto checker = TypeCheckerXMLLoader::create_checker_from_mapping(mapping);
    ASSERT_NE(checker, nullptr);
}

// Test loading comprehensive XML with all type mappings
TEST_F(TypeCheckerXMLLoaderTest, LoadComprehensiveXML) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.Byte" display_name="Byte">
    <type-rule allowed_type="TYPE_TINYINT" return_type="TYPE_TINYINT"/>
    <type-rule allowed_type="TYPE_BOOLEAN" return_type="TYPE_TINYINT"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Short" display_name="Short">
    <type-rule allowed_type="TYPE_TINYINT" return_type="TYPE_SMALLINT"/>
    <type-rule allowed_type="TYPE_SMALLINT" return_type="TYPE_SMALLINT"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Integer" display_name="Integer">
    <type-rule allowed_type="TYPE_TINYINT" return_type="TYPE_INT"/>
    <type-rule allowed_type="TYPE_SMALLINT" return_type="TYPE_INT"/>
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Long" display_name="Long">
    <type-rule allowed_type="TYPE_BIGINT" return_type="TYPE_BIGINT"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Boolean" display_name="Boolean">
    <type-rule allowed_type="TYPE_BOOLEAN" return_type="TYPE_BOOLEAN"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Float" display_name="Float">
    <type-rule allowed_type="TYPE_FLOAT" return_type="TYPE_FLOAT"/>
  </type-mapping>
  <type-mapping java_class="java.lang.Double" display_name="Double">
    <type-rule allowed_type="TYPE_DOUBLE" return_type="TYPE_DOUBLE"/>
  </type-mapping>
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_CHAR" return_type="TYPE_VARCHAR"/>
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
  <type-mapping java_class="java.sql.Timestamp" display_name="Timestamp">
    <type-rule allowed_type="TYPE_DATETIME" return_type="TYPE_DATETIME"/>
  </type-mapping>
  <type-mapping java_class="java.sql.Date" display_name="Date">
    <type-rule allowed_type="TYPE_DATE" return_type="TYPE_DATE"/>
  </type-mapping>
  <type-mapping java_class="java.sql.Time" display_name="Time">
    <type-rule allowed_type="TYPE_TIME" return_type="TYPE_TIME"/>
  </type-mapping>
  <type-mapping java_class="byte[]" display_name="ByteArray">
    <type-rule allowed_type="TYPE_VARBINARY" return_type="TYPE_VARBINARY"/>
  </type-mapping>
  <type-mapping java_class="com.clickhouse.data.value.UnsignedByte" display_name="UnsignedByte">
    <type-rule allowed_type="TYPE_SMALLINT" return_type="TYPE_SMALLINT"/>
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_SMALLINT"/>
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/comprehensive.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_TRUE(result.ok());

    const auto& mappings = result.value();
    ASSERT_EQ(mappings.size(), 13);
    
    // Verify the configurable mapping
    bool found_clickhouse = false;
    for (const auto& mapping : mappings) {
        if (mapping.java_class == "com.clickhouse.data.value.UnsignedByte") {
            found_clickhouse = true;
            ASSERT_TRUE(mapping.is_configurable);
            ASSERT_EQ(mapping.display_name, "UnsignedByte");
            ASSERT_EQ(mapping.rules.size(), 2);
        }
    }
    ASSERT_TRUE(found_clickhouse);
}

// Test XML with special characters in class names
TEST_F(TypeCheckerXMLLoaderTest, LoadXMLWithSpecialCharacters) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="[B" display_name="ByteArray">
    <type-rule allowed_type="TYPE_VARBINARY" return_type="TYPE_VARBINARY"/>
  </type-mapping>
  <type-mapping java_class="byte[]" display_name="ByteArray">
    <type-rule allowed_type="TYPE_VARBINARY" return_type="TYPE_VARBINARY"/>
  </type-mapping>
  <type-mapping java_class="oracle.jdbc.OracleBlob" display_name="OracleBlob">
    <type-rule allowed_type="TYPE_VARBINARY" return_type="TYPE_VARBINARY"/>
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/special_chars.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_TRUE(result.ok());

    const auto& mappings = result.value();
    ASSERT_EQ(mappings.size(), 3);

    EXPECT_EQ(mappings[0].java_class, "[B");
    EXPECT_EQ(mappings[1].java_class, "byte[]");
}

// Test XML with missing type-rule elements
TEST_F(TypeCheckerXMLLoaderTest, LoadXMLMissingTypeRules) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.String" display_name="String">
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/missing_rules.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

// Test XML with invalid type names
TEST_F(TypeCheckerXMLLoaderTest, LoadXMLInvalidTypeNames) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_INVALID" return_type="TYPE_VARCHAR"/>
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/invalid_types.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

// Test XML with malformed structure
TEST_F(TypeCheckerXMLLoaderTest, LoadMalformedXMLStructure) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
  <invalid-element/>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/malformed_structure.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    // Should still succeed, ignoring unknown elements
    ASSERT_TRUE(result.ok());
    const auto& mappings = result.value();
    ASSERT_EQ(mappings.size(), 1);
}

// Test loading XML with all mappings being configurable
TEST_F(TypeCheckerXMLLoaderTest, AllMappingsAreConfigurable) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.Integer" display_name="Integer">
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
  </type-mapping>
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
</type-checkers>)";

    std::string xml_file = "/tmp/type_checker_test/all_configurable.xml";
    create_test_xml(xml_file, xml_content);

    auto result = TypeCheckerXMLLoader::load_from_xml(xml_file);
    ASSERT_TRUE(result.ok());

    const auto& mappings = result.value();
    // All mappings should be marked as configurable
    for (const auto& mapping : mappings) {
        EXPECT_TRUE(mapping.is_configurable);
        EXPECT_FALSE(mapping.display_name.empty());
        EXPECT_GT(mapping.rules.size(), 0);
    }
}

} // namespace starrocks
