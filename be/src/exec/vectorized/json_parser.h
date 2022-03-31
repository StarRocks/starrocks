// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exprs/vectorized/json_functions.h"
#include "simdjson.h"

namespace starrocks::vectorized {

class JsonParser {
public:
    JsonParser() = default;
    virtual ~JsonParser() = default;
    // parse initiates the parser. The inner iterator would point to the first object to be returned.
    virtual Status parse(uint8_t* data, size_t len, size_t allocated) noexcept = 0;
    // get returns the object, which is reentrant.
    virtual Status get_current(simdjson::ondemand::object* row) noexcept = 0;
    // next forwards the inner iterator.
    virtual Status advance() noexcept = 0;
};

class JsonDocumentStreamParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    Status _advance() noexcept;

    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    simdjson::ondemand::document_stream _doc_stream;
    simdjson::ondemand::document_stream::iterator _doc_stream_itr;

    // _need_reset is set true when the _curr_obj may be iterated.
    bool _need_reset = false;
    simdjson::ondemand::object _curr_obj;
};

class JsonArrayParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    Status _advance() noexcept;

    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    simdjson::ondemand::document _doc;

    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;

    // _need_reset is set true when the _curr_obj may be iterated.
    bool _need_reset = false;
    simdjson::ondemand::object _curr_obj;
};

class JsonDocumentStreamParserWithRoot : public JsonDocumentStreamParser {
public:
    JsonDocumentStreamParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;

    // _need_reset is set true when the _curr_obj may be iterated.
    bool _need_reset = false;
    simdjson::ondemand::object _curr_obj;
};

class JsonArrayParserWithRoot : public JsonArrayParser {
public:
    JsonArrayParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;

    // _need_reset is set true when the _curr_obj may be iterated.
    bool _need_reset = false;
    simdjson::ondemand::object _curr_obj;
};

class ExpandedJsonDocumentStreamParserWithRoot : public JsonDocumentStreamParser {
public:
    ExpandedJsonDocumentStreamParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;
    // _curr_row denotes the current object that the json root points to.
    simdjson::ondemand::object _curr_row;

    // array under json root.
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;

    // _need_reset is set true when the _curr_obj may be iterated.
    bool _need_reset = false;
    simdjson::ondemand::object _curr_obj;
};

class ExpandedJsonArrayParserWithRoot : public JsonArrayParser {
public:
    ExpandedJsonArrayParserWithRoot(const std::vector<SimpleJsonPath>& root_paths) : _root_paths(root_paths) {}
    Status parse(uint8_t* data, size_t len, size_t allocated) noexcept override;
    Status get_current(simdjson::ondemand::object* row) noexcept override;
    Status advance() noexcept override;

private:
    std::vector<SimpleJsonPath> _root_paths;
    // _curr_row denotes the current object that the json root points to.
    simdjson::ondemand::object _curr_row;

    // array under json root.
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;

    // _need_reset is set true when the _curr_obj may be iterated.
    bool _need_reset = false;
    simdjson::ondemand::object _curr_obj;
};

} // namespace starrocks::vectorized
