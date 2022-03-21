// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "common/status.h"
#include "exprs/vectorized/json_functions.h"
#include "simdjson.h"

namespace starrocks::vectorized {

class JsonParser {
public:
    JsonParser() = default;
    virtual ~JsonParser() = default;
    // parse initiates the parser. The inner iterator would point to the first object to be returned.
    virtual Status parse(uint8_t* data, size_t len, size_t allocated) = 0;
    // get returns the object pointed by the inner iterator.
    virtual Status get_current(simdjson::ondemand::object* row) = 0;
    // next forwards the inner iterator.
    virtual Status advance() = 0;
};

class JsonDocumentStreamParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) override;
    Status get_current(simdjson::ondemand::object* row) override;
    Status advance() override;

private:
    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    simdjson::ondemand::document_stream _doc_stream;
    simdjson::ondemand::document_stream::iterator _doc_stream_itr;
};

class JsonArrayParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) override;
    Status get_current(simdjson::ondemand::object* row) override;
    Status advance() override;

private:
    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    simdjson::ondemand::document _doc;
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;
};

class JsonDocumentStreamParserWitRoot : public JsonDocumentStreamParser {
public:
    JsonDocumentStreamParserWitRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status get_current(simdjson::ondemand::object* row) override;

private:
    std::vector<SimpleJsonPath> _root_paths;
};

class JsonArrayParserWitRoot : public JsonArrayParser {
public:
    JsonArrayParserWitRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status get_current(simdjson::ondemand::object* row) override;

private:
    std::vector<SimpleJsonPath> _root_paths;
};

class ExpandedJsonDocumentStreamParserWitRoot : public JsonDocumentStreamParser {
public:
    ExpandedJsonDocumentStreamParserWitRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) override;
    Status get_current(simdjson::ondemand::object* row) override;
    Status advance() override;

private:
    std::vector<SimpleJsonPath> _root_paths;
    simdjson::ondemand::object _curr_row;
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;
};

class ExpandedJsonArrayParserWitRoot : public JsonArrayParser {
public:
    ExpandedJsonArrayParserWitRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) override;
    Status get_current(simdjson::ondemand::object* row) override;
    Status advance() override;

private:
    std::vector<SimpleJsonPath> _root_paths;
    simdjson::ondemand::object _curr_row;
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;
};

} // namespace starrocks::vectorized