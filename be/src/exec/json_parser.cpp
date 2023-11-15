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

#include "exec/json_parser.h"

#include <fmt/format.h>

#include "gutil/strings/substitute.h"

namespace starrocks {

const size_t MAX_RAW_JSON_LEN = 64;

Status JsonDocumentStreamParser::parse(char* data, size_t len, size_t allocated) noexcept {
    try {
        _data = data;
        _len = len;

        _doc_stream = _parser->iterate_many(data, len, len);

        _doc_stream_itr = _doc_stream.begin();

    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse json as document stream. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    return Status::OK();
}

Status JsonDocumentStreamParser::get_current(simdjson::ondemand::object* row) noexcept {
    if (UNLIKELY(_curr_ready)) {
        _curr.reset();
        *row = _curr;
        return Status::OK();
    }

    try {
        if (_doc_stream_itr != _doc_stream.end()) {
            simdjson::ondemand::document_reference doc = *_doc_stream_itr;

            _curr = doc.get_object();
            *row = _curr;
            _curr_ready = true;
            return Status::OK();
        }
        return Status::EndOfFile("all documents of the stream are iterated");
    } catch (simdjson::simdjson_error& e) {
        std::string err_msg;
        if (e.error() == simdjson::CAPACITY) {
            // It's necessary to tell the user when they try to load json array whose payload size is beyond the simdjson::ondemand::parser's buffer.
            err_msg =
                    "The input payload size is beyond parser limit. Please set strip_outer_array true if you want "
                    "to "
                    "load json array";
        } else {
            err_msg = strings::Substitute("Failed to iterate document stream as object. error: $0",
                                          simdjson::error_message(e.error()));
        }
        return Status::DataQualityError(err_msg);
    }
}

Status JsonDocumentStreamParser::advance() noexcept {
    _curr_ready = false;
    if (++_doc_stream_itr != _doc_stream.end()) {
        return Status::OK();
    }
    return Status::EndOfFile("all documents of the stream are iterated");
}

std::string JsonDocumentStreamParser::left_bytes_string(size_t sz) noexcept {
    if (_len == 0) {
        return {};
    }

    auto off = _doc_stream_itr.current_index();
    if (off >= _len) {
        return {};
    }
    return std::string(reinterpret_cast<char*>(_data) + off, _len - off);
}

Status JsonArrayParser::parse(char* data, size_t len, size_t allocated) noexcept {
    try {
        _doc = _parser->iterate(data, len, allocated);

        _data = data;
        _len = len;

        if (_doc.type() != simdjson::ondemand::json_type::array) {
            auto err_msg = fmt::format("the value should be array type with strip_outer_array=true, value: {}",
                                       JsonFunctions::to_json_string(_doc, MAX_RAW_JSON_LEN));
            return Status::DataQualityError(err_msg);
        }

        _array = _doc.get_array();
        _array_itr = _array.begin();

    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to parse json as array. error: $0", simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    return Status::OK();
}

Status JsonArrayParser::get_current(simdjson::ondemand::object* row) noexcept {
    if (UNLIKELY(_curr_ready)) {
        _curr.reset();
        *row = _curr;
        return Status::OK();
    }

    try {
        if (_array_itr == _array.end()) {
            return Status::EndOfFile("all values of the array are iterated");
        }

        simdjson::ondemand::value val = *_array_itr;

        _curr = val.get_object();
        *row = _curr;
        _curr_ready = true;

        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate json array as object. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status JsonArrayParser::advance() noexcept {
    _curr_ready = false;
    try {
        if (++_array_itr == _array.end()) {
            return Status::EndOfFile("all values of the array are iterated");
        }
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to iterate json as array. error: $0", simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

std::string JsonArrayParser::left_bytes_string(size_t sz) noexcept {
    if (_len == 0) {
        return {};
    }

    const char* loc;
    auto err = _doc.current_location().get(loc);
    if (err) {
        return {};
    }
    auto off = loc - reinterpret_cast<const char*>(_data);

    if (off < 0 || off >= _len) {
        return {};
    }

    return std::string(reinterpret_cast<const char*>(_data) + off, _len - off);
}

Status JsonDocumentStreamParserWithRoot::get_current(simdjson::ondemand::object* row) noexcept {
    if (UNLIKELY(_curr_ready)) {
        _curr.reset();
        *row = _curr;
        return Status::OK();
    }

    RETURN_IF_ERROR(this->JsonDocumentStreamParser::get_current(row));

    // json root filter.
    simdjson::ondemand::value val;
    RETURN_IF_ERROR(JsonFunctions::extract_from_object(*row, _root_paths, &val));

    try {
        if (val.type() != simdjson::ondemand::json_type::object) {
            auto err_msg =
                    fmt::format("the value should be object type in json document stream with json root, value: {}",
                                JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
            return Status::DataQualityError(err_msg);
        }

        _curr = val.get_object();
        *row = _curr;
        _curr_ready = true;

        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate document stream as object with json root. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status JsonDocumentStreamParserWithRoot::advance() noexcept {
    _curr_ready = false;
    return this->JsonDocumentStreamParser::advance();
}

Status JsonArrayParserWithRoot::get_current(simdjson::ondemand::object* row) noexcept {
    if (UNLIKELY(_curr_ready)) {
        _curr.reset();
        *row = _curr;
        return Status::OK();
    }

    RETURN_IF_ERROR(this->JsonArrayParser::get_current(row));
    simdjson::ondemand::value val;
    // json root filter.
    RETURN_IF_ERROR(JsonFunctions::extract_from_object(*row, _root_paths, &val));

    try {
        if (val.type() != simdjson::ondemand::json_type::object) {
            auto err_msg = fmt::format("the value should be object type in json array with json root, value: {}",
                                       JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
            return Status::DataQualityError(err_msg);
        }

        _curr = val.get_object();
        *row = _curr;
        _curr_ready = true;

        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate json array as object with json root. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status JsonArrayParserWithRoot::advance() noexcept {
    _curr_ready = false;
    return this->JsonArrayParser::advance();
}

Status ExpandedJsonDocumentStreamParserWithRoot::parse(char* data, size_t len, size_t allocated) noexcept {
    RETURN_IF_ERROR(this->JsonDocumentStreamParser::parse(data, len, allocated));
    RETURN_IF_ERROR(this->JsonDocumentStreamParser::get_current(&_curr_row));

    simdjson::ondemand::value val;
    RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, &val));

    try {
        if (val.type() != simdjson::ondemand::json_type::array) {
            auto err_msg = fmt::format(
                    "the value should be array type with strip_outer_array=true in json document stream with root, "
                    "value: {}",
                    JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
            return Status::DataQualityError(err_msg);
        }

        _array = val.get_array();

        _array_itr = _array.begin();

    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse json as expanded document stream with json root. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    return Status::OK();
}

Status ExpandedJsonDocumentStreamParserWithRoot::get_current(simdjson::ondemand::object* row) noexcept {
    if (UNLIKELY(_curr_ready)) {
        _curr.reset();
        *row = _curr;
        return Status::OK();
    }

    try {
        if (_array_itr == _array.end()) {
            return Status::EndOfFile("all documents of the stream are iterated");
        }

        simdjson::ondemand::value val = *_array_itr;

        if (val.type() != simdjson::ondemand::json_type::object) {
            auto err_msg = fmt::format(
                    "the value should be object type in expanded json document stream with json root, value: {}",
                    JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
            return Status::DataQualityError(err_msg);
        }

        _curr = val.get_object();
        *row = _curr;
        _curr_ready = true;

        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to iterate expanded document stream as object with json root. error: $0",
                                    simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status ExpandedJsonDocumentStreamParserWithRoot::advance() noexcept {
    _curr_ready = false;

    if (++_array_itr == _array.end()) {
        do {
            // forward document stream parser.
            RETURN_IF_ERROR(this->JsonDocumentStreamParser::advance());
            RETURN_IF_ERROR(this->JsonDocumentStreamParser::get_current(&_curr_row));

            simdjson::ondemand::value val;
            RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, &val));

            try {
                if (val.type() != simdjson::ondemand::json_type::array) {
                    auto err_msg = fmt::format(
                            "the value under json root should be array type with strip_outer_array=true in json "
                            "document stream with root, value: {}",
                            JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
                    return Status::DataQualityError(err_msg);
                }

                _array = val.get_array();
                _array_itr = _array.begin();
            } catch (simdjson::simdjson_error& e) {
                auto err_msg = strings::Substitute("Failed to iterate document stream sub-array. error: $0",
                                                   simdjson::error_message(e.error()));
                return Status::DataQualityError(err_msg);
            }

            // until EOF or new elem is available.
        } while (_array_itr == _array.end());
    }
    return Status::OK();
}

Status ExpandedJsonArrayParserWithRoot::parse(char* data, size_t len, size_t allocated) noexcept {
    RETURN_IF_ERROR(this->JsonArrayParser::parse(data, len, allocated));
    RETURN_IF_ERROR(this->JsonArrayParser::get_current(&_curr_row));

    simdjson::ondemand::value val;
    RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, &val));

    try {
        if (val.type() != simdjson::ondemand::json_type::array) {
            auto err_msg = fmt::format(
                    "the value under json root should be array type with strip_outer_array=true in json array, "
                    "value: "
                    "{}",
                    JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
            return Status::DataQualityError(err_msg);
        }

        _array = val.get_array();

        _array_itr = _array.begin();

    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse json as expanded json array with json root. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    return Status::OK();
}

Status ExpandedJsonArrayParserWithRoot::get_current(simdjson::ondemand::object* row) noexcept {
    if (UNLIKELY(_curr_ready)) {
        _curr.reset();
        *row = _curr;
        return Status::OK();
    }

    try {
        if (_array_itr == _array.end()) {
            return Status::EndOfFile("all documents of the stream are iterated");
        }

        simdjson::ondemand::value val = *_array_itr;

        if (val.type() != simdjson::ondemand::json_type::object) {
            auto err_msg =
                    fmt::format("the value should be object type in expanded json array with json root, value: {}",
                                JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
            return Status::DataQualityError(err_msg);
        }

        _curr = val.get_object();
        *row = _curr;
        _curr_ready = true;

        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate json array as object with json root. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status ExpandedJsonArrayParserWithRoot::advance() noexcept {
    _curr_ready = false;

    if (++_array_itr == _array.end()) {
        do {
            // forward document stream parser.
            RETURN_IF_ERROR(this->JsonArrayParser::advance());
            RETURN_IF_ERROR(this->JsonArrayParser::get_current(&_curr_row));

            simdjson::ondemand::value val;
            RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, &val));

            try {
                if (val.type() != simdjson::ondemand::json_type::array) {
                    auto err_msg = fmt::format(
                            "the value under json root should be array type with strip_outer_array=true in json "
                            "array, "
                            "value: {}",
                            JsonFunctions::to_json_string(val, MAX_RAW_JSON_LEN));
                    return Status::DataQualityError(err_msg);
                }

                _array = val.get_array();
                _array_itr = _array.begin();
            } catch (simdjson::simdjson_error& e) {
                auto err_msg = strings::Substitute("Failed to iterate json array sub-array. error: $0",
                                                   simdjson::error_message(e.error()));
                return Status::DataQualityError(err_msg);
            }

            // until EOF or new elem is available.
        } while (_array_itr == _array.end());
    }
    return Status::OK();
}

} // namespace starrocks
