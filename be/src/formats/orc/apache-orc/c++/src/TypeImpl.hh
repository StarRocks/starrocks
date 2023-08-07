/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <vector>

#include "Adaptor.hh"
#include "orc/Type.hh"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {

class TypeImpl : public Type {
private:
    TypeImpl* parent;
    mutable int64_t columnId;
    mutable int64_t maximumColumnId;
    TypeKind kind;
    std::vector<std::unique_ptr<Type>> subTypes;
    std::vector<std::string> fieldNames;
    mutable std::map<uint64_t, uint64_t> columnIdToPos;
    uint64_t subtypeCount;
    uint64_t maxLength;
    uint64_t precision;
    uint64_t scale;
    std::map<std::string, std::string> attributes;

public:
    /**
     * Create most of the logical types.
     */
    TypeImpl(TypeKind kind);

    /**
     * Create char and varchar type.
     */
    TypeImpl(TypeKind kind, uint64_t maxLength);

    /**
     * Create decimal type.
     */
    TypeImpl(TypeKind kind, uint64_t precision, uint64_t scale);

    uint64_t getColumnId() const override;

    uint64_t getMaximumColumnId() const override;

    TypeKind getKind() const override;

    uint64_t getSubtypeCount() const override;

    const Type* getSubtype(uint64_t i) const override;

    const Type* getSubtypeByColumnId(uint64_t columnId) const override;

    const std::string& getFieldName(uint64_t i) const override;

    uint64_t getFieldNamesCount() const override;

    uint64_t getMaximumLength() const override;

    uint64_t getPrecision() const override;

    uint64_t getScale() const override;

    Type& setAttribute(const std::string& key, const std::string& value) override;

    bool hasAttributeKey(const std::string& key) const override;

    Type& removeAttribute(const std::string& key) override;

    std::vector<std::string> getAttributeKeys() const override;

    std::string getAttributeValue(const std::string& key) const override;

    std::string toString() const override;

    Type* addStructField(const std::string& fieldName, std::unique_ptr<Type> fieldType) override;
    Type* addUnionChild(std::unique_ptr<Type> fieldType) override;

    std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size, MemoryPool& memoryPool,
                                                      bool encoded = false) const override;

    /**
     * Explicitly set the column ids. Only for internal usage.
     */
    void setIds(uint64_t columnId, uint64_t maxColumnId);

    /**
     * Add a child type.
     */
    void addChildType(std::unique_ptr<Type> childType);

    static std::pair<ORC_UNIQUE_PTR<Type>, size_t> parseType(const std::string& input, size_t start, size_t end);

private:
    /**
     * Assign ids to this node and its children giving this
     * node rootId.
     * @param rootId the column id that should be assigned to this node.
     */
    uint64_t assignIds(uint64_t rootId) const;

    /**
     * Ensure that ids are assigned to all of the nodes.
     */
    void ensureIdAssigned() const;

    /**
     * Build column id mapping to position in children.
     */
    void buildColumnIdToPosMap() const;

    /**
     * Parse array type from string
     * @param input the input string of an array type
     * @param start start position of the input string
     * @param end end position of the input string
     */
    static std::unique_ptr<Type> parseArrayType(const std::string& input, size_t start, size_t end);

    /**
     * Parse map type from string
     * @param input the input string of a map type
     * @param start start position of the input string
     * @param end end position of the input string
     */
    static std::unique_ptr<Type> parseMapType(const std::string& input, size_t start, size_t end);

    /**
     * Parse struct type from string
     * @param input the input string of a struct type
     * @param start start position of the input string
     * @param end end position of the input string
     */
    static std::unique_ptr<Type> parseStructType(const std::string& input, size_t start, size_t end);

    /**
     * Parse union type from string
     * @param input the input string of an union type
     * @param start start position of the input string
     * @param end end position of the input string
     */
    static std::unique_ptr<Type> parseUnionType(const std::string& input, size_t start, size_t end);

    /**
     * Parse field name from string
     * @param input the input string of a field name
     * @param start start position of the input string
     * @param end end position of the input string
     */
    static std::pair<std::string, size_t> parseName(const std::string& input, const size_t start, const size_t end);

    /**
     * Parse decimal type from string
     * @param input the input string of a decimal type
     * @param start start position of the input string
     * @param end end position of the input string
     */
    static std::unique_ptr<Type> parseDecimalType(const std::string& input, size_t start, size_t end);

    /**
     * Parse type for a category
     * @param category type name
     * @param input the input string of the category
     * @param start start position of the input string
     * @param end end position of the input string
     */
    static std::unique_ptr<Type> parseCategory(const std::string& category, const std::string& input, size_t start,
                                               size_t end);
};

std::unique_ptr<Type> convertType(const proto::Type& type, const proto::Footer& footer);

/**
   * Build a clone of the file type, projecting columns from the selected
   * vector. This routine assumes that the parent of any selected column
   * is also selected.
   * @param fileType the type in the file
   * @param selected is each column by id selected
   * @return a clone of the fileType filtered by the selection array
   */
std::unique_ptr<Type> buildSelectedType(const Type* fileType, const std::vector<bool>& selected);
} // namespace orc
