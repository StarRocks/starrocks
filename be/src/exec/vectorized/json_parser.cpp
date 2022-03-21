// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/json_parser.h"

#include "gutil/strings/substitute.h"

namespace starrocks::vectorized {

Status JsonDocumentStreamParser::parse(uint8_t* data, size_t len, size_t allocated) {
    try {
        _doc_stream = _parser.iterate_many(data, len);

        _doc_stream_itr = _doc_stream.begin();

    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to parse json as object error: $0", simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    return Status::OK();
}

Status JsonDocumentStreamParser::get_current(simdjson::ondemand::object* row) {
    try {
        if (_doc_stream_itr != _doc_stream.end()) {
            simdjson::ondemand::document_reference doc = *_doc_stream_itr;

            if (doc.type() != simdjson::ondemand::json_type::object) {
                return Status::DataQualityError("JSON data is an object, strip_outer_array must be set false");
            }

            *row = doc.get_object();
            return Status::OK();
        }
        return Status::EndOfFile("all documents of the stream are iterated");
    } catch (simdjson::simdjson_error& e) {
        std::string err_msg;
        if (e.error() == simdjson::CAPACITY) {
            // It's necessary to tell the user when they try to load json array whose payload size is beyond the simdjson::ondemand::parser's buffer.
            err_msg =
                    "The input payload size is beyond parser limit. Please set strip_outer_array true if you want to "
                    "load json array";
        } else {
            err_msg = strings::Substitute("Failed to iterate json as object. error: $0",
                                          simdjson::error_message(e.error()));
        }
        return Status::DataQualityError(err_msg);
    }
}

Status JsonDocumentStreamParser::advance() {
    try {
        if (++_doc_stream_itr != _doc_stream.end()) {
            return Status::OK();
        }
        return Status::EndOfFile("all documents of the stream are iterated");
    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to iterate json as object. error: $0", simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status JsonArrayParser::parse(uint8_t* data, size_t len, size_t allocated) {
    try {
        _doc = _parser.iterate(data, len, allocated);

        if (_doc.type() != simdjson::ondemand::json_type::array) {
            return Status::DataQualityError("JSON data is an array, strip_outer_array must be set true");
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

Status JsonArrayParser::get_current(simdjson::ondemand::object* row) {
    try {
        if (_array_itr == _array.end()) {
            return Status::EndOfFile("all values of the array are iterated");
        }

        simdjson::ondemand::value val = *_array_itr;

        if (val.type() != simdjson::ondemand::json_type::object) {
            return Status::DataQualityError("nested array is not supported");
        }

        *row = val.get_object();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to iterate json as array. error: $0", simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status JsonArrayParser::advance() {
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

Status JsonDocumentStreamParserWithRoot::get_current(simdjson::ondemand::object* row) {
    try {
        RETURN_IF_ERROR(this->JsonDocumentStreamParser::get_current(row));
        // json root filter.
        simdjson::ondemand::value val;
        RETURN_IF_ERROR(JsonFunctions::extract_from_object(*row, _root_paths, val));

        *row = val.get_object();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate json as document stream with filter. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
} // namespace starrocks::vectorized

Status JsonArrayParserWithRoot::get_current(simdjson::ondemand::object* row) {
    RETURN_IF_ERROR(this->JsonArrayParser::get_current(row));
    simdjson::ondemand::value val;
    // json root filter.
    RETURN_IF_ERROR(JsonFunctions::extract_from_object(*row, _root_paths, val));

    try {
        *row = val.get_object();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate json as array with filter. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status ExpandedJsonDocumentStreamParserWithRoot::parse(uint8_t* data, size_t len, size_t allocated) {
    RETURN_IF_ERROR(this->JsonDocumentStreamParser::parse(data, len, allocated));
    RETURN_IF_ERROR(this->JsonDocumentStreamParser::get_current(&_curr_row));
    simdjson::ondemand::value val;
    RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, val));

    try {
        _array = val.get_array();

        _array_itr = _array.begin();

    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to parse json as object error: $0", simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    return Status::OK();
}

Status ExpandedJsonDocumentStreamParserWithRoot::get_current(simdjson::ondemand::object* row) {
    try {
        if (_array_itr == _array.end()) {
            return Status::EndOfFile("all documents of the stream are iterated");
        }

        simdjson::ondemand::value val = *_array_itr;

        if (val.type() != simdjson::ondemand::json_type::object) {
            return Status::DataQualityError("nested array is not supported");
        }

        *row = val.get_object();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate json as document stream with filter. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status ExpandedJsonDocumentStreamParserWithRoot::advance() {
    if (++_array_itr == _array.end()) {
        do {
            // forward document stream parser.
            RETURN_IF_ERROR(this->JsonDocumentStreamParser::advance());
            RETURN_IF_ERROR(this->JsonDocumentStreamParser::get_current(&_curr_row));

            simdjson::ondemand::value val;
            RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, val));

            try {
                _array = val.get_array();
                _array_itr = _array.begin();
            } catch (simdjson::simdjson_error& e) {
                auto err_msg = strings::Substitute("Failed to iterate json as array. error: $0",
                                                   simdjson::error_message(e.error()));
                return Status::DataQualityError(err_msg);
            }

            // until EOF or new elem is available.
        } while (_array_itr == _array.end());
    }
    return Status::OK();
}

Status ExpandedJsonArrayParserWithRoot::parse(uint8_t* data, size_t len, size_t allocated) {
    RETURN_IF_ERROR(this->JsonArrayParser::parse(data, len, allocated));
    RETURN_IF_ERROR(this->JsonArrayParser::get_current(&_curr_row));

    simdjson::ondemand::value val;
    RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, val));

    try {
        _array = val.get_array();

        _array_itr = _array.begin();

    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Failed to parse json as object error: $0", simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    return Status::OK();
}

Status ExpandedJsonArrayParserWithRoot::get_current(simdjson::ondemand::object* row) {
    try {
        if (_array_itr == _array.end()) {
            return Status::EndOfFile("all documents of the stream are iterated");
        }

        simdjson::ondemand::value val = *_array_itr;

        if (val.type() != simdjson::ondemand::json_type::object) {
            return Status::DataQualityError("nested array is not supported");
        }

        *row = val.get_object();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to iterate json as document stream with filter. error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status ExpandedJsonArrayParserWithRoot::advance() {
    if (++_array_itr == _array.end()) {
        do {
            // forward document stream parser.
            RETURN_IF_ERROR(this->JsonArrayParser::advance());
            RETURN_IF_ERROR(this->JsonArrayParser::get_current(&_curr_row));

            simdjson::ondemand::value val;
            RETURN_IF_ERROR(JsonFunctions::extract_from_object(_curr_row, _root_paths, val));

            try {
                _array = val.get_array();
                _array_itr = _array.begin();
            } catch (simdjson::simdjson_error& e) {
                auto err_msg = strings::Substitute("Failed to iterate json as array. error: $0",
                                                   simdjson::error_message(e.error()));
                return Status::DataQualityError(err_msg);
            }

            // until EOF or new elem is available.
        } while (_array_itr == _array.end());
    }
    return Status::OK();
}

} // namespace starrocks::vectorized