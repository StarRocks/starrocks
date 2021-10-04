// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/src/TypeImpl.cc

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

#include "TypeImpl.hh"

#include <iostream>
#include <sstream>

#include "Adaptor.hh"
#include "orc/Exceptions.hh"

namespace orc {

Type::~Type() {
    // PASS
}

TypeImpl::TypeImpl(TypeKind _kind) {
    parent = nullptr;
    columnId = -1;
    maximumColumnId = -1;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
}

TypeImpl::TypeImpl(TypeKind _kind, uint64_t _maxLength) {
    parent = nullptr;
    columnId = -1;
    maximumColumnId = -1;
    kind = _kind;
    maxLength = _maxLength;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
}

TypeImpl::TypeImpl(TypeKind _kind, uint64_t _precision, uint64_t _scale) {
    parent = nullptr;
    columnId = -1;
    maximumColumnId = -1;
    kind = _kind;
    maxLength = 0;
    precision = _precision;
    scale = _scale;
    subtypeCount = 0;
}

uint64_t TypeImpl::assignIds(uint64_t root) const {
    columnId = static_cast<int64_t>(root);
    uint64_t current = root + 1;
    for (uint64_t i = 0; i < subtypeCount; ++i) {
        current = dynamic_cast<TypeImpl*>(subTypes[i].get())->assignIds(current);
    }
    maximumColumnId = static_cast<int64_t>(current) - 1;
    return current;
}

void TypeImpl::ensureIdAssigned() const {
    if (columnId == -1) {
        const TypeImpl* root = this;
        while (root->parent != nullptr) {
            root = root->parent;
        }
        root->assignIds(0);
    }
}

uint64_t TypeImpl::getColumnId() const {
    ensureIdAssigned();
    return static_cast<uint64_t>(columnId);
}

uint64_t TypeImpl::getMaximumColumnId() const {
    ensureIdAssigned();
    return static_cast<uint64_t>(maximumColumnId);
}

TypeKind TypeImpl::getKind() const {
    return kind;
}

uint64_t TypeImpl::getSubtypeCount() const {
    return subtypeCount;
}

const Type* TypeImpl::getSubtype(uint64_t i) const {
    return subTypes[i].get();
}

const std::string& TypeImpl::getFieldName(uint64_t i) const {
    return fieldNames[i];
}

uint64_t TypeImpl::getFieldNamesCount() const {
    return fieldNames.size();
}

uint64_t TypeImpl::getMaximumLength() const {
    return maxLength;
}

uint64_t TypeImpl::getPrecision() const {
    return precision;
}

uint64_t TypeImpl::getScale() const {
    return scale;
}

Type& TypeImpl::setAttribute(const std::string& key, const std::string& value) {
    attributes[key] = value;
    return *this;
}

bool TypeImpl::hasAttributeKey(const std::string& key) const {
    return attributes.find(key) != attributes.end();
}

Type& TypeImpl::removeAttribute(const std::string& key) {
    auto it = attributes.find(key);
    if (it == attributes.end()) {
        throw std::range_error("Key not found: " + key);
    }
    attributes.erase(it);
    return *this;
}

std::vector<std::string> TypeImpl::getAttributeKeys() const {
    std::vector<std::string> ret;
    ret.reserve(attributes.size());
    for (auto& attribute : attributes) {
        ret.push_back(attribute.first);
    }
    return ret;
}

std::string TypeImpl::getAttributeValue(const std::string& key) const {
    auto it = attributes.find(key);
    if (it == attributes.end()) {
        throw std::range_error("Key not found: " + key);
    }
    return it->second;
}

void TypeImpl::setIds(uint64_t _columnId, uint64_t _maxColumnId) {
    columnId = static_cast<int64_t>(_columnId);
    maximumColumnId = static_cast<int64_t>(_maxColumnId);
}

void TypeImpl::addChildType(std::unique_ptr<Type> childType) {
    TypeImpl* child = dynamic_cast<TypeImpl*>(childType.get());
    subTypes.push_back(std::move(childType));
    if (child != nullptr) {
        child->parent = this;
    }
    subtypeCount += 1;
}

Type* TypeImpl::addStructField(const std::string& fieldName, std::unique_ptr<Type> fieldType) {
    addChildType(std::move(fieldType));
    fieldNames.push_back(fieldName);
    return this;
}

Type* TypeImpl::addUnionChild(std::unique_ptr<Type> fieldType) {
    addChildType(std::move(fieldType));
    return this;
}

std::string TypeImpl::toString() const {
    switch (static_cast<int64_t>(kind)) {
    case BOOLEAN:
        return "boolean";
    case BYTE:
        return "tinyint";
    case SHORT:
        return "smallint";
    case INT:
        return "int";
    case LONG:
        return "bigint";
    case FLOAT:
        return "float";
    case DOUBLE:
        return "double";
    case STRING:
        return "string";
    case BINARY:
        return "binary";
    case TIMESTAMP:
        return "timestamp";
    case TIMESTAMP_INSTANT:
        return "timestamp with local time zone";
    case LIST:
        return "array<" + (subTypes[0] ? subTypes[0]->toString() : "void") + ">";
    case MAP:
        return "map<" + (subTypes[0] ? subTypes[0]->toString() : "void") + "," +
               (subTypes[1] ? subTypes[1]->toString() : "void") + ">";
    case STRUCT: {
        std::string result = "struct<";
        for (size_t i = 0; i < subTypes.size(); ++i) {
            if (i != 0) {
                result += ",";
            }
            result += fieldNames[i];
            result += ":";
            result += subTypes[i]->toString();
        }
        result += ">";
        return result;
    }
    case UNION: {
        std::string result = "uniontype<";
        for (size_t i = 0; i < subTypes.size(); ++i) {
            if (i != 0) {
                result += ",";
            }
            result += subTypes[i]->toString();
        }
        result += ">";
        return result;
    }
    case DECIMAL: {
        std::stringstream result;
        result << "decimal(" << precision << "," << scale << ")";
        return result.str();
    }
    case DATE:
        return "date";
    case VARCHAR: {
        std::stringstream result;
        result << "varchar(" << maxLength << ")";
        return result.str();
    }
    case CHAR: {
        std::stringstream result;
        result << "char(" << maxLength << ")";
        return result.str();
    }
    default:
        throw NotImplementedYet("Unknown type");
    }
}

std::unique_ptr<ColumnVectorBatch> TypeImpl::createRowBatch(uint64_t capacity, MemoryPool& memoryPool,
                                                            bool encoded) const {
    switch (static_cast<int64_t>(kind)) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case DATE:
        return std::unique_ptr<ColumnVectorBatch>(new LongVectorBatch(capacity, memoryPool));

    case FLOAT:
    case DOUBLE:
        return std::unique_ptr<ColumnVectorBatch>(new DoubleVectorBatch(capacity, memoryPool));

    case STRING:
    case BINARY:
    case CHAR:
    case VARCHAR:
        return encoded ? std::unique_ptr<ColumnVectorBatch>(new EncodedStringVectorBatch(capacity, memoryPool))
                       : std::unique_ptr<ColumnVectorBatch>(new StringVectorBatch(capacity, memoryPool));

    case TIMESTAMP:
    case TIMESTAMP_INSTANT:
        return std::unique_ptr<ColumnVectorBatch>(new TimestampVectorBatch(capacity, memoryPool));

    case STRUCT: {
        StructVectorBatch* result = new StructVectorBatch(capacity, memoryPool);
        std::unique_ptr<ColumnVectorBatch> return_value = std::unique_ptr<ColumnVectorBatch>(result);
        for (uint64_t i = 0; i < getSubtypeCount(); ++i) {
            result->fields.push_back(getSubtype(i)->createRowBatch(capacity, memoryPool, encoded).release());
        }
        return return_value;
    }

    case LIST: {
        ListVectorBatch* result = new ListVectorBatch(capacity, memoryPool);
        std::unique_ptr<ColumnVectorBatch> return_value = std::unique_ptr<ColumnVectorBatch>(result);
        if (getSubtype(0) != nullptr) {
            result->elements = getSubtype(0)->createRowBatch(capacity, memoryPool, encoded);
        }
        return return_value;
    }

    case MAP: {
        MapVectorBatch* result = new MapVectorBatch(capacity, memoryPool);
        std::unique_ptr<ColumnVectorBatch> return_value = std::unique_ptr<ColumnVectorBatch>(result);
        if (getSubtype(0) != nullptr) {
            result->keys = getSubtype(0)->createRowBatch(capacity, memoryPool, encoded);
        }
        if (getSubtype(1) != nullptr) {
            result->elements = getSubtype(1)->createRowBatch(capacity, memoryPool, encoded);
        }
        return return_value;
    }

    case DECIMAL: {
        if (getPrecision() == 0 || getPrecision() > 18) {
            return std::unique_ptr<ColumnVectorBatch>(new Decimal128VectorBatch(capacity, memoryPool));
        } else {
            return std::unique_ptr<ColumnVectorBatch>(new Decimal64VectorBatch(capacity, memoryPool));
        }
    }

    case UNION: {
        UnionVectorBatch* result = new UnionVectorBatch(capacity, memoryPool);
        std::unique_ptr<ColumnVectorBatch> return_value = std::unique_ptr<ColumnVectorBatch>(result);
        for (uint64_t i = 0; i < getSubtypeCount(); ++i) {
            result->children.push_back(getSubtype(i)->createRowBatch(capacity, memoryPool, encoded).release());
        }
        return return_value;
    }

    default:
        throw NotImplementedYet("not supported yet");
    }
}

std::unique_ptr<Type> createPrimitiveType(TypeKind kind) {
    return std::unique_ptr<Type>(new TypeImpl(kind));
}

std::unique_ptr<Type> createCharType(TypeKind kind, uint64_t maxLength) {
    return std::unique_ptr<Type>(new TypeImpl(kind, maxLength));
}

std::unique_ptr<Type> createDecimalType(uint64_t precision, uint64_t scale) {
    return std::unique_ptr<Type>(new TypeImpl(DECIMAL, precision, scale));
}

std::unique_ptr<Type> createStructType() {
    return std::unique_ptr<Type>(new TypeImpl(STRUCT));
}

std::unique_ptr<Type> createListType(std::unique_ptr<Type> elements) {
    TypeImpl* result = new TypeImpl(LIST);
    std::unique_ptr<Type> return_value = std::unique_ptr<Type>(result);
    result->addChildType(std::move(elements));
    return return_value;
}

std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key, std::unique_ptr<Type> value) {
    TypeImpl* result = new TypeImpl(MAP);
    std::unique_ptr<Type> return_value = std::unique_ptr<Type>(result);
    result->addChildType(std::move(key));
    result->addChildType(std::move(value));
    return return_value;
}

std::unique_ptr<Type> createUnionType() {
    return std::unique_ptr<Type>(new TypeImpl(UNION));
}

std::string printProtobufMessage(const google::protobuf::Message& message);
std::unique_ptr<Type> convertType(const proto::Type& type, const proto::Footer& footer) {
    std::unique_ptr<Type> ret;
    switch (static_cast<int64_t>(type.kind())) {
    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_SHORT:
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_STRING:
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_TIMESTAMP:
    case proto::Type_Kind_TIMESTAMP_INSTANT:
    case proto::Type_Kind_DATE:
        ret = std::unique_ptr<Type>(new TypeImpl(static_cast<TypeKind>(type.kind())));
        break;

    case proto::Type_Kind_CHAR:
    case proto::Type_Kind_VARCHAR:
        ret = std::unique_ptr<Type>(new TypeImpl(static_cast<TypeKind>(type.kind()), type.maximumlength()));
        break;

    case proto::Type_Kind_DECIMAL:
        ret = std::unique_ptr<Type>(new TypeImpl(DECIMAL, type.precision(), type.scale()));
        break;

    case proto::Type_Kind_LIST:
    case proto::Type_Kind_MAP:
    case proto::Type_Kind_UNION: {
        TypeImpl* result = new TypeImpl(static_cast<TypeKind>(type.kind()));
        ret = std::unique_ptr<Type>(result);
        if (type.kind() == proto::Type_Kind_LIST && type.subtypes_size() != 1)
            throw ParseError("Illegal LIST type that doesn't contain one subtype");
        if (type.kind() == proto::Type_Kind_MAP && type.subtypes_size() != 2)
            throw ParseError("Illegal MAP type that doesn't contain two subtypes");
        if (type.kind() == proto::Type_Kind_UNION && type.subtypes_size() == 0)
            throw ParseError("Illegal UNION type that doesn't contain any subtypes");
        for (int i = 0; i < type.subtypes_size(); ++i) {
            result->addUnionChild(convertType(footer.types(static_cast<int>(type.subtypes(i))), footer));
        }
        break;
    }

    case proto::Type_Kind_STRUCT: {
        TypeImpl* result = new TypeImpl(STRUCT);
        ret = std::unique_ptr<Type>(result);
        for (int i = 0; i < type.subtypes_size(); ++i) {
            result->addStructField(type.fieldnames(i),
                                   convertType(footer.types(static_cast<int>(type.subtypes(i))), footer));
        }
        break;
    }
    default:
        throw NotImplementedYet("Unknown type kind");
    }
    for (int i = 0; i < type.attributes_size(); ++i) {
        const auto& attribute = type.attributes(i);
        ret->setAttribute(attribute.key(), attribute.value());
    }
    return ret;
}

/**
   * Build a clone of the file type, projecting columns from the selected
   * vector. This routine assumes that the parent of any selected column
   * is also selected. The column ids are copied from the fileType.
   * @param fileType the type in the file
   * @param selected is each column by id selected
   * @return a clone of the fileType filtered by the selection array
   */
std::unique_ptr<Type> buildSelectedType(const Type* fileType, const std::vector<bool>& selected) {
    if (fileType == nullptr || !selected[fileType->getColumnId()]) {
        return std::unique_ptr<Type>();
    }

    TypeImpl* result;
    switch (static_cast<int>(fileType->getKind())) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case BINARY:
    case TIMESTAMP:
    case TIMESTAMP_INSTANT:
    case DATE:
        result = new TypeImpl(fileType->getKind());
        break;

    case DECIMAL:
        result = new TypeImpl(fileType->getKind(), fileType->getPrecision(), fileType->getScale());
        break;

    case VARCHAR:
    case CHAR:
        result = new TypeImpl(fileType->getKind(), fileType->getMaximumLength());
        break;

    case LIST:
        result = new TypeImpl(fileType->getKind());
        result->addChildType(buildSelectedType(fileType->getSubtype(0), selected));
        break;

    case MAP:
        result = new TypeImpl(fileType->getKind());
        result->addChildType(buildSelectedType(fileType->getSubtype(0), selected));
        result->addChildType(buildSelectedType(fileType->getSubtype(1), selected));
        break;

    case STRUCT: {
        result = new TypeImpl(fileType->getKind());
        for (uint64_t child = 0; child < fileType->getSubtypeCount(); ++child) {
            std::unique_ptr<Type> childType = buildSelectedType(fileType->getSubtype(child), selected);
            if (childType != nullptr) {
                result->addStructField(fileType->getFieldName(child), std::move(childType));
            }
        }
        break;
    }

    case UNION: {
        result = new TypeImpl(fileType->getKind());
        for (uint64_t child = 0; child < fileType->getSubtypeCount(); ++child) {
            std::unique_ptr<Type> childType = buildSelectedType(fileType->getSubtype(child), selected);
            if (childType != nullptr) {
                result->addUnionChild(std::move(childType));
            }
        }
        break;
    }

    default:
        throw NotImplementedYet("Unknown type kind");
    }
    result->setIds(fileType->getColumnId(), fileType->getMaximumColumnId());
    for (auto& key : fileType->getAttributeKeys()) {
        const auto& value = fileType->getAttributeValue(key);
        result->setAttribute(key, value);
    }
    return std::unique_ptr<Type>(result);
}

ORC_UNIQUE_PTR<Type> Type::buildTypeFromString(const std::string& input) {
    std::vector<std::pair<std::string, ORC_UNIQUE_PTR<Type>>> res = TypeImpl::parseType(input, 0, input.size());
    if (res.size() != 1) {
        throw std::logic_error("Invalid type string.");
    }
    return std::move(res[0].second);
}

std::unique_ptr<Type> TypeImpl::parseArrayType(const std::string& input, size_t start, size_t end) {
    TypeImpl* arrayType = new TypeImpl(LIST);
    std::unique_ptr<Type> return_value = std::unique_ptr<Type>(arrayType);
    std::vector<std::pair<std::string, ORC_UNIQUE_PTR<Type>>> v = TypeImpl::parseType(input, start, end);
    if (v.size() != 1) {
        throw std::logic_error("Array type must contain exactly one sub type.");
    }
    arrayType->addChildType(std::move(v[0].second));
    return return_value;
}

std::unique_ptr<Type> TypeImpl::parseMapType(const std::string& input, size_t start, size_t end) {
    TypeImpl* mapType = new TypeImpl(MAP);
    std::unique_ptr<Type> return_value = std::unique_ptr<Type>(mapType);
    std::vector<std::pair<std::string, ORC_UNIQUE_PTR<Type>>> v = TypeImpl::parseType(input, start, end);
    if (v.size() != 2) {
        throw std::logic_error("Map type must contain exactly two sub types.");
    }
    mapType->addChildType(std::move(v[0].second));
    mapType->addChildType(std::move(v[1].second));
    return return_value;
}

std::unique_ptr<Type> TypeImpl::parseStructType(const std::string& input, size_t start, size_t end) {
    TypeImpl* structType = new TypeImpl(STRUCT);
    std::unique_ptr<Type> return_value = std::unique_ptr<Type>(structType);
    std::vector<std::pair<std::string, ORC_UNIQUE_PTR<Type>>> v = TypeImpl::parseType(input, start, end);
    if (v.size() == 0) {
        throw std::logic_error("Struct type must contain at least one sub type.");
    }
    for (auto& i : v) {
        structType->addStructField(i.first, std::move(i.second));
    }
    return return_value;
}

std::unique_ptr<Type> TypeImpl::parseUnionType(const std::string& input, size_t start, size_t end) {
    TypeImpl* unionType = new TypeImpl(UNION);
    std::unique_ptr<Type> return_value = std::unique_ptr<Type>(unionType);
    std::vector<std::pair<std::string, ORC_UNIQUE_PTR<Type>>> v = TypeImpl::parseType(input, start, end);
    if (v.size() == 0) {
        throw std::logic_error("Union type must contain at least one sub type.");
    }
    for (auto& i : v) {
        unionType->addChildType(std::move(i.second));
    }
    return return_value;
}

std::unique_ptr<Type> TypeImpl::parseDecimalType(const std::string& input, size_t start, size_t end) {
    size_t sep = input.find(',', start);
    if (sep + 1 >= end || sep == std::string::npos) {
        throw std::logic_error("Decimal type must specify precision and scale.");
    }
    uint64_t precision = static_cast<uint64_t>(atoi(input.substr(start, sep - start).c_str()));
    uint64_t scale = static_cast<uint64_t>(atoi(input.substr(sep + 1, end - sep - 1).c_str()));
    return std::unique_ptr<Type>(new TypeImpl(DECIMAL, precision, scale));
}

std::unique_ptr<Type> TypeImpl::parseCategory(const std::string& category, const std::string& input, size_t start,
                                              size_t end) {
    if (category == "boolean") {
        return std::unique_ptr<Type>(new TypeImpl(BOOLEAN));
    } else if (category == "tinyint") {
        return std::unique_ptr<Type>(new TypeImpl(BYTE));
    } else if (category == "smallint") {
        return std::unique_ptr<Type>(new TypeImpl(SHORT));
    } else if (category == "int") {
        return std::unique_ptr<Type>(new TypeImpl(INT));
    } else if (category == "bigint") {
        return std::unique_ptr<Type>(new TypeImpl(LONG));
    } else if (category == "float") {
        return std::unique_ptr<Type>(new TypeImpl(FLOAT));
    } else if (category == "double") {
        return std::unique_ptr<Type>(new TypeImpl(DOUBLE));
    } else if (category == "string") {
        return std::unique_ptr<Type>(new TypeImpl(STRING));
    } else if (category == "binary") {
        return std::unique_ptr<Type>(new TypeImpl(BINARY));
    } else if (category == "timestamp") {
        return std::unique_ptr<Type>(new TypeImpl(TIMESTAMP));
    } else if (category == "timestamp with local time zone") {
        return std::unique_ptr<Type>(new TypeImpl(TIMESTAMP_INSTANT));
    } else if (category == "array") {
        return parseArrayType(input, start, end);
    } else if (category == "map") {
        return parseMapType(input, start, end);
    } else if (category == "struct") {
        return parseStructType(input, start, end);
    } else if (category == "uniontype") {
        return parseUnionType(input, start, end);
    } else if (category == "decimal") {
        return parseDecimalType(input, start, end);
    } else if (category == "date") {
        return std::unique_ptr<Type>(new TypeImpl(DATE));
    } else if (category == "varchar") {
        uint64_t maxLength = static_cast<uint64_t>(atoi(input.substr(start, end - start).c_str()));
        return std::unique_ptr<Type>(new TypeImpl(VARCHAR, maxLength));
    } else if (category == "char") {
        uint64_t maxLength = static_cast<uint64_t>(atoi(input.substr(start, end - start).c_str()));
        return std::unique_ptr<Type>(new TypeImpl(CHAR, maxLength));
    } else {
        throw std::logic_error("Unknown type " + category);
    }
}

std::vector<std::pair<std::string, ORC_UNIQUE_PTR<Type>>> TypeImpl::parseType(const std::string& input, size_t start,
                                                                              size_t end) {
    std::vector<std::pair<std::string, ORC_UNIQUE_PTR<Type>>> res;
    size_t pos = start;

    while (pos < end) {
        size_t endPos = pos;
        while (endPos < end && (isalnum(input[endPos]) || input[endPos] == '_')) {
            ++endPos;
        }

        std::string fieldName;
        if (input[endPos] == ':') {
            fieldName = input.substr(pos, endPos - pos);
            pos = ++endPos;
            while (endPos < end && (isalpha(input[endPos]) || input[endPos] == ' ')) {
                ++endPos;
            }
        }

        size_t nextPos = endPos + 1;
        if (input[endPos] == '<') {
            int count = 1;
            while (nextPos < end) {
                if (input[nextPos] == '<') {
                    ++count;
                } else if (input[nextPos] == '>') {
                    --count;
                }
                if (count == 0) {
                    break;
                }
                ++nextPos;
            }
            if (nextPos == end) {
                throw std::logic_error("Invalid type string. Cannot find closing >");
            }
        } else if (input[endPos] == '(') {
            while (nextPos < end && input[nextPos] != ')') {
                ++nextPos;
            }
            if (nextPos == end) {
                throw std::logic_error("Invalid type string. Cannot find closing )");
            }
        } else if (input[endPos] != ',' && endPos != end) {
            throw std::logic_error("Unrecognized character.");
        }

        std::string category = input.substr(pos, endPos - pos);
        res.emplace_back(fieldName, parseCategory(category, input, endPos + 1, nextPos));

        if (nextPos < end && (input[nextPos] == ')' || input[nextPos] == '>')) {
            pos = nextPos + 2;
        } else {
            pos = nextPos;
        }
    }

    return res;
}

} // namespace orc
