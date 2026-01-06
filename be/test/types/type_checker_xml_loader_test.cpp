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
  <type-mapping java_class="java.lang.Integer" checker="IntegerTypeChecker"/>
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
    <type-mapping java_class="java.lang.String"     checker="StringTypeChecker"   />
  <type-mapping   java_class="java.lang.Integer"   checker="IntegerTypeChecker"/>
<type-mapping java_class="java.lang.Boolean" checker="BooleanTypeChecker" />
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
  <type-mapping java_class="java.lang.String"/>
  <type-mapping checker="IntegerTypeChecker"/>
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

// Test creating checker instances
TEST_F(TypeCheckerXMLLoaderTest, CreateCheckerInstances) {
    auto byte_checker = TypeCheckerXMLLoader::create_checker("ByteTypeChecker");
    ASSERT_NE(byte_checker, nullptr);

    auto string_checker = TypeCheckerXMLLoader::create_checker("StringTypeChecker");
    ASSERT_NE(string_checker, nullptr);

    auto integer_checker = TypeCheckerXMLLoader::create_checker("IntegerTypeChecker");
    ASSERT_NE(integer_checker, nullptr);

    auto boolean_checker = TypeCheckerXMLLoader::create_checker("BooleanTypeChecker");
    ASSERT_NE(boolean_checker, nullptr);

    auto unknown_checker = TypeCheckerXMLLoader::create_checker("UnknownTypeChecker");
    ASSERT_EQ(unknown_checker, nullptr);
}

// Test all supported checker types
TEST_F(TypeCheckerXMLLoaderTest, CreateAllSupportedCheckers) {
    std::vector<std::string> checker_names = {
            "ByteTypeChecker",
            "ShortTypeChecker",
            "IntegerTypeChecker",
            "StringTypeChecker",
            "LongTypeChecker",
            "BigIntegerTypeChecker",
            "BooleanTypeChecker",
            "FloatTypeChecker",
            "DoubleTypeChecker",
            "TimestampTypeChecker",
            "DateTypeChecker",
            "TimeTypeChecker",
            "LocalDateTimeTypeChecker",
            "LocalDateTypeChecker",
            "BigDecimalTypeChecker",
            "OracleTimestampClassTypeChecker",
            "SqlServerDateTimeOffsetTypeChecker",
            "ByteArrayTypeChecker",
            "DefaultTypeChecker",
    };

    for (const auto& name : checker_names) {
        auto checker = TypeCheckerXMLLoader::create_checker(name);
        ASSERT_NE(checker, nullptr) << "Failed to create checker: " << name;
    }
}

// Test loading comprehensive XML with all type mappings
TEST_F(TypeCheckerXMLLoaderTest, LoadComprehensiveXML) {
    std::string xml_content = R"(<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.Byte" checker="ByteTypeChecker"/>
  <type-mapping java_class="java.lang.Short" checker="ShortTypeChecker"/>
  <type-mapping java_class="java.lang.Integer" checker="IntegerTypeChecker"/>
  <type-mapping java_class="java.lang.Long" checker="LongTypeChecker"/>
  <type-mapping java_class="java.lang.Boolean" checker="BooleanTypeChecker"/>
  <type-mapping java_class="java.lang.Float" checker="FloatTypeChecker"/>
  <type-mapping java_class="java.lang.Double" checker="DoubleTypeChecker"/>
  <type-mapping java_class="java.lang.String" checker="StringTypeChecker"/>
  <type-mapping java_class="java.sql.Timestamp" checker="TimestampTypeChecker"/>
  <type-mapping java_class="java.sql.Date" checker="DateTypeChecker"/>
  <type-mapping java_class="java.sql.Time" checker="TimeTypeChecker"/>
  <type-mapping java_class="byte[]" checker="ByteArrayTypeChecker"/>
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
  <type-mapping java_class="[B" checker="ByteArrayTypeChecker"/>
  <type-mapping java_class="byte[]" checker="ByteArrayTypeChecker"/>
  <type-mapping java_class="oracle.jdbc.OracleBlob" checker="ByteArrayTypeChecker"/>
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

} // namespace starrocks
