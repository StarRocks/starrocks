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

#include "geo/wkb.h"

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <utility>

#include "fmt/format.h"

namespace starrocks {
namespace {

constexpr size_t kMaxNestingDepth = 32;
constexpr uint32_t kMaxElements = 1'000'000;

Status invalid_wkt(const std::string& message) {
    return Status::InvalidArgument("Invalid WKT: " + message);
}

Status invalid_wkb(const std::string& message) {
    return Status::InvalidArgument("Invalid WKB: " + message);
}

bool valid_type(uint32_t type) {
    return type >= static_cast<uint32_t>(WkbGeometryType::POINT) &&
           type <= static_cast<uint32_t>(WkbGeometryType::GEOMETRYCOLLECTION);
}

class WktParser {
public:
    explicit WktParser(std::string_view input) : _input(input) {}

    Status parse(WkbGeometry* output) {
        RETURN_IF_ERROR(parse_geometry(0, output));
        skip_spaces();
        if (_position != _input.size()) {
            return invalid_wkt("unexpected trailing input at byte " + std::to_string(_position));
        }
        return Status::OK();
    }

private:
    Status parse_geometry(size_t depth, WkbGeometry* output) {
        if (depth > kMaxNestingDepth) {
            return invalid_wkt("geometry nesting is too deep");
        }

        std::string type_name;
        RETURN_IF_ERROR(parse_word(&type_name));
        if (type_name == "POINT") {
            output->type = WkbGeometryType::POINT;
        } else if (type_name == "LINESTRING") {
            output->type = WkbGeometryType::LINESTRING;
        } else if (type_name == "POLYGON") {
            output->type = WkbGeometryType::POLYGON;
        } else if (type_name == "MULTIPOINT") {
            output->type = WkbGeometryType::MULTIPOINT;
        } else if (type_name == "MULTILINESTRING") {
            output->type = WkbGeometryType::MULTILINESTRING;
        } else if (type_name == "MULTIPOLYGON") {
            output->type = WkbGeometryType::MULTIPOLYGON;
        } else if (type_name == "GEOMETRYCOLLECTION") {
            output->type = WkbGeometryType::GEOMETRYCOLLECTION;
        } else {
            return invalid_wkt("unsupported geometry type " + type_name);
        }

        skip_spaces();
        if (peek_alpha()) {
            std::string modifier;
            RETURN_IF_ERROR(parse_word(&modifier));
            if (modifier != "EMPTY") {
                return invalid_wkt("Z, M, SRID, and other type modifiers are not supported");
            }
            output->empty = true;
            return Status::OK();
        }

        switch (output->type) {
        case WkbGeometryType::POINT:
            return parse_point(output);
        case WkbGeometryType::LINESTRING:
            return parse_linestring(output);
        case WkbGeometryType::POLYGON:
            return parse_polygon(output);
        case WkbGeometryType::MULTIPOINT:
            return parse_multipoint(output);
        case WkbGeometryType::MULTILINESTRING:
            return parse_multilinestring(output);
        case WkbGeometryType::MULTIPOLYGON:
            return parse_multipolygon(output);
        case WkbGeometryType::GEOMETRYCOLLECTION:
            return parse_collection(depth, output);
        }
        return invalid_wkt("unknown geometry type");
    }

    Status parse_point(WkbGeometry* output) {
        RETURN_IF_ERROR(expect('('));
        WkbCoordinate coordinate;
        RETURN_IF_ERROR(parse_coordinate(&coordinate));
        output->coordinates.push_back(coordinate);
        return expect(')');
    }

    Status parse_linestring(WkbGeometry* output) {
        RETURN_IF_ERROR(expect('('));
        RETURN_IF_ERROR(parse_coordinate_list(&output->coordinates));
        RETURN_IF_ERROR(expect(')'));
        if (output->coordinates.size() < 2) {
            return invalid_wkt("LINESTRING must contain at least two points");
        }
        return Status::OK();
    }

    Status parse_polygon(WkbGeometry* output) {
        RETURN_IF_ERROR(expect('('));
        RETURN_IF_ERROR(parse_ring_list(&output->rings));
        return expect(')');
    }

    Status parse_multipoint(WkbGeometry* output) {
        RETURN_IF_ERROR(expect('('));
        do {
            WkbGeometry child;
            child.type = WkbGeometryType::POINT;
            skip_spaces();
            if (peek_alpha()) {
                std::string word;
                RETURN_IF_ERROR(parse_word(&word));
                if (word != "EMPTY") {
                    return invalid_wkt("expected EMPTY in MULTIPOINT");
                }
                child.empty = true;
            } else {
                bool wrapped = consume('(');
                WkbCoordinate coordinate;
                RETURN_IF_ERROR(parse_coordinate(&coordinate));
                child.coordinates.push_back(coordinate);
                if (wrapped) {
                    RETURN_IF_ERROR(expect(')'));
                }
            }
            output->children.emplace_back(std::move(child));
            if (output->children.size() > kMaxElements) {
                return invalid_wkt("too many MULTIPOINT elements");
            }
        } while (consume(','));
        return expect(')');
    }

    Status parse_multilinestring(WkbGeometry* output) {
        RETURN_IF_ERROR(expect('('));
        do {
            WkbGeometry child;
            child.type = WkbGeometryType::LINESTRING;
            skip_spaces();
            if (peek_alpha()) {
                std::string word;
                RETURN_IF_ERROR(parse_word(&word));
                if (word != "EMPTY") {
                    return invalid_wkt("expected EMPTY in MULTILINESTRING");
                }
                child.empty = true;
            } else {
                RETURN_IF_ERROR(expect('('));
                RETURN_IF_ERROR(parse_coordinate_list(&child.coordinates));
                RETURN_IF_ERROR(expect(')'));
                if (child.coordinates.size() < 2) {
                    return invalid_wkt("LINESTRING must contain at least two points");
                }
            }
            output->children.emplace_back(std::move(child));
            if (output->children.size() > kMaxElements) {
                return invalid_wkt("too many MULTILINESTRING elements");
            }
        } while (consume(','));
        return expect(')');
    }

    Status parse_multipolygon(WkbGeometry* output) {
        RETURN_IF_ERROR(expect('('));
        do {
            WkbGeometry child;
            child.type = WkbGeometryType::POLYGON;
            skip_spaces();
            if (peek_alpha()) {
                std::string word;
                RETURN_IF_ERROR(parse_word(&word));
                if (word != "EMPTY") {
                    return invalid_wkt("expected EMPTY in MULTIPOLYGON");
                }
                child.empty = true;
            } else {
                RETURN_IF_ERROR(expect('('));
                RETURN_IF_ERROR(parse_ring_list(&child.rings));
                RETURN_IF_ERROR(expect(')'));
            }
            output->children.emplace_back(std::move(child));
            if (output->children.size() > kMaxElements) {
                return invalid_wkt("too many MULTIPOLYGON elements");
            }
        } while (consume(','));
        return expect(')');
    }

    Status parse_collection(size_t depth, WkbGeometry* output) {
        RETURN_IF_ERROR(expect('('));
        do {
            WkbGeometry child;
            RETURN_IF_ERROR(parse_geometry(depth + 1, &child));
            output->children.emplace_back(std::move(child));
            if (output->children.size() > kMaxElements) {
                return invalid_wkt("too many GEOMETRYCOLLECTION elements");
            }
        } while (consume(','));
        return expect(')');
    }

    Status parse_ring_list(std::vector<std::vector<WkbCoordinate>>* rings) {
        do {
            RETURN_IF_ERROR(expect('('));
            std::vector<WkbCoordinate> ring;
            RETURN_IF_ERROR(parse_coordinate_list(&ring));
            RETURN_IF_ERROR(expect(')'));
            if (ring.size() < 4) {
                return invalid_wkt("polygon ring must contain at least four points");
            }
            if (!(ring.front() == ring.back())) {
                return invalid_wkt("polygon ring is not closed");
            }
            rings->emplace_back(std::move(ring));
            if (rings->size() > kMaxElements) {
                return invalid_wkt("too many polygon rings");
            }
        } while (consume(','));
        return Status::OK();
    }

    Status parse_coordinate_list(std::vector<WkbCoordinate>* coordinates) {
        do {
            WkbCoordinate coordinate;
            RETURN_IF_ERROR(parse_coordinate(&coordinate));
            coordinates->push_back(coordinate);
            if (coordinates->size() > kMaxElements) {
                return invalid_wkt("too many coordinates");
            }
        } while (consume(','));
        return Status::OK();
    }

    Status parse_coordinate(WkbCoordinate* coordinate) {
        RETURN_IF_ERROR(parse_number(&coordinate->x));
        if (_position == _input.size() || !is_space(_input[_position])) {
            return invalid_wkt("expected whitespace between coordinate ordinates at byte " +
                               std::to_string(_position));
        }
        RETURN_IF_ERROR(parse_number(&coordinate->y));
        skip_spaces();
        if (_position < _input.size() && _input[_position] != ',' && _input[_position] != ')') {
            return invalid_wkt("only two-dimensional coordinates are supported");
        }
        return Status::OK();
    }

    Status parse_number(double* output) {
        skip_spaces();
        if (_position == _input.size()) {
            return invalid_wkt("expected coordinate at end of input");
        }

        const size_t begin_position = _position;
        if (_input[_position] == '+' || _input[_position] == '-') {
            ++_position;
        }

        bool has_digits = false;
        while (_position < _input.size() && _input[_position] >= '0' && _input[_position] <= '9') {
            has_digits = true;
            ++_position;
        }
        if (_position < _input.size() && _input[_position] == '.') {
            ++_position;
            while (_position < _input.size() && _input[_position] >= '0' && _input[_position] <= '9') {
                has_digits = true;
                ++_position;
            }
        }
        if (!has_digits) {
            _position = begin_position;
            return invalid_wkt("invalid coordinate at byte " + std::to_string(_position));
        }

        if (_position < _input.size() && (_input[_position] == 'e' || _input[_position] == 'E')) {
            ++_position;
            if (_position < _input.size() && (_input[_position] == '+' || _input[_position] == '-')) {
                ++_position;
            }
            const size_t exponent_position = _position;
            while (_position < _input.size() && _input[_position] >= '0' && _input[_position] <= '9') {
                ++_position;
            }
            if (_position == exponent_position) {
                _position = begin_position;
                return invalid_wkt("invalid coordinate at byte " + std::to_string(_position));
            }
        }

        std::string token = _input.substr(begin_position, _position - begin_position);
        const char* begin = token.c_str();
        char* end = nullptr;
        errno = 0;
        double value = std::strtod(begin, &end);
        if (end == begin || *end != '\0' || errno == ERANGE || !std::isfinite(value)) {
            _position = begin_position;
            return invalid_wkt("invalid coordinate at byte " + std::to_string(_position));
        }
        *output = value;
        return Status::OK();
    }

    Status parse_word(std::string* output) {
        skip_spaces();
        const size_t begin = _position;
        while (_position < _input.size() && is_alpha(_input[_position])) {
            char value = _input[_position++];
            output->push_back(value >= 'a' && value <= 'z' ? value - ('a' - 'A') : value);
        }
        if (_position == begin) {
            return invalid_wkt("expected geometry type at byte " + std::to_string(_position));
        }
        return Status::OK();
    }

    Status expect(char expected) {
        skip_spaces();
        if (_position == _input.size() || _input[_position] != expected) {
            return invalid_wkt(std::string("expected '") + expected + "' at byte " + std::to_string(_position));
        }
        ++_position;
        return Status::OK();
    }

    bool consume(char expected) {
        skip_spaces();
        if (_position < _input.size() && _input[_position] == expected) {
            ++_position;
            return true;
        }
        return false;
    }

    void skip_spaces() {
        while (_position < _input.size()) {
            char value = _input[_position];
            if (!is_space(value)) {
                break;
            }
            ++_position;
        }
    }

    bool peek_alpha() {
        skip_spaces();
        return _position < _input.size() && is_alpha(_input[_position]);
    }

    static bool is_alpha(char value) {
        return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z');
    }

    static bool is_space(char value) {
        return value == ' ' || value == '\t' || value == '\r' || value == '\n';
    }

    std::string _input;
    size_t _position = 0;
};

class WkbReader {
public:
    explicit WkbReader(const Slice& input)
            : _data(reinterpret_cast<const uint8_t*>(input.data)), _size(input.size) {}

    Status parse(WkbGeometry* output) {
        RETURN_IF_ERROR(parse_geometry(0, output));
        if (_position != _size) {
            return invalid_wkb("unexpected trailing bytes");
        }
        return Status::OK();
    }

private:
    Status parse_geometry(size_t depth, WkbGeometry* output) {
        if (depth > kMaxNestingDepth) {
            return invalid_wkb("geometry nesting is too deep");
        }
        uint8_t byte_order = 0;
        RETURN_IF_ERROR(read_byte(&byte_order));
        if (byte_order > 1) {
            return invalid_wkb("byte order must be 0 or 1");
        }
        const bool little_endian = byte_order == 1;

        uint32_t raw_type = 0;
        RETURN_IF_ERROR(read_uint32(little_endian, &raw_type));
        if (!valid_type(raw_type)) {
            return invalid_wkb("unsupported geometry type or dimensional/SRID flags");
        }
        output->type = static_cast<WkbGeometryType>(raw_type);

        switch (output->type) {
        case WkbGeometryType::POINT:
            return parse_point(little_endian, output);
        case WkbGeometryType::LINESTRING:
            return parse_linestring(little_endian, output);
        case WkbGeometryType::POLYGON:
            return parse_polygon(little_endian, output);
        case WkbGeometryType::MULTIPOINT:
        case WkbGeometryType::MULTILINESTRING:
        case WkbGeometryType::MULTIPOLYGON:
        case WkbGeometryType::GEOMETRYCOLLECTION:
            return parse_children(depth, little_endian, output);
        }
        return invalid_wkb("unknown geometry type");
    }

    Status parse_point(bool little_endian, WkbGeometry* output) {
        WkbCoordinate coordinate;
        RETURN_IF_ERROR(read_double(little_endian, &coordinate.x));
        RETURN_IF_ERROR(read_double(little_endian, &coordinate.y));
        if (std::isnan(coordinate.x) && std::isnan(coordinate.y)) {
            output->empty = true;
            return Status::OK();
        }
        if (!std::isfinite(coordinate.x) || !std::isfinite(coordinate.y)) {
            return invalid_wkb("POINT coordinates must be finite or both NaN for EMPTY");
        }
        output->coordinates.push_back(coordinate);
        return Status::OK();
    }

    Status parse_linestring(bool little_endian, WkbGeometry* output) {
        uint32_t count = 0;
        RETURN_IF_ERROR(read_count(little_endian, &count));
        if (count == 0) {
            output->empty = true;
            return Status::OK();
        }
        if (count < 2) {
            return invalid_wkb("LINESTRING must contain at least two points");
        }
        return read_coordinates(little_endian, count, &output->coordinates);
    }

    Status parse_polygon(bool little_endian, WkbGeometry* output) {
        uint32_t ring_count = 0;
        RETURN_IF_ERROR(read_count(little_endian, &ring_count));
        if (ring_count == 0) {
            output->empty = true;
            return Status::OK();
        }
        output->rings.reserve(ring_count);
        for (uint32_t i = 0; i < ring_count; ++i) {
            uint32_t point_count = 0;
            RETURN_IF_ERROR(read_count(little_endian, &point_count));
            if (point_count < 4) {
                return invalid_wkb("polygon ring must contain at least four points");
            }
            std::vector<WkbCoordinate> ring;
            RETURN_IF_ERROR(read_coordinates(little_endian, point_count, &ring));
            if (!(ring.front() == ring.back())) {
                return invalid_wkb("polygon ring is not closed");
            }
            output->rings.emplace_back(std::move(ring));
        }
        return Status::OK();
    }

    Status parse_children(size_t depth, bool little_endian, WkbGeometry* output) {
        uint32_t count = 0;
        RETURN_IF_ERROR(read_count(little_endian, &count));
        if (count == 0) {
            output->empty = true;
            return Status::OK();
        }
        output->children.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            WkbGeometry child;
            RETURN_IF_ERROR(parse_geometry(depth + 1, &child));
            if (output->type == WkbGeometryType::MULTIPOINT && child.type != WkbGeometryType::POINT) {
                return invalid_wkb("MULTIPOINT child is not a POINT");
            }
            if (output->type == WkbGeometryType::MULTILINESTRING && child.type != WkbGeometryType::LINESTRING) {
                return invalid_wkb("MULTILINESTRING child is not a LINESTRING");
            }
            if (output->type == WkbGeometryType::MULTIPOLYGON && child.type != WkbGeometryType::POLYGON) {
                return invalid_wkb("MULTIPOLYGON child is not a POLYGON");
            }
            output->children.emplace_back(std::move(child));
        }
        return Status::OK();
    }

    Status read_coordinates(bool little_endian, uint32_t count, std::vector<WkbCoordinate>* output) {
        if (count > (_size - _position) / (sizeof(double) * 2)) {
            return invalid_wkb("coordinate count exceeds input size");
        }
        output->reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            WkbCoordinate coordinate;
            RETURN_IF_ERROR(read_double(little_endian, &coordinate.x));
            RETURN_IF_ERROR(read_double(little_endian, &coordinate.y));
            if (!std::isfinite(coordinate.x) || !std::isfinite(coordinate.y)) {
                return invalid_wkb("coordinates must be finite");
            }
            output->push_back(coordinate);
        }
        return Status::OK();
    }

    Status read_count(bool little_endian, uint32_t* output) {
        RETURN_IF_ERROR(read_uint32(little_endian, output));
        if (*output > kMaxElements) {
            return invalid_wkb("element count exceeds safety limit");
        }
        return Status::OK();
    }

    Status read_byte(uint8_t* output) {
        if (_position == _size) {
            return invalid_wkb("unexpected end of input");
        }
        *output = _data[_position++];
        return Status::OK();
    }

    Status read_uint32(bool little_endian, uint32_t* output) {
        if (_size - _position < sizeof(uint32_t)) {
            return invalid_wkb("unexpected end of input");
        }
        const uint8_t* value = _data + _position;
        _position += sizeof(uint32_t);
        if (little_endian) {
            *output = static_cast<uint32_t>(value[0]) | (static_cast<uint32_t>(value[1]) << 8) |
                      (static_cast<uint32_t>(value[2]) << 16) | (static_cast<uint32_t>(value[3]) << 24);
        } else {
            *output = (static_cast<uint32_t>(value[0]) << 24) | (static_cast<uint32_t>(value[1]) << 16) |
                      (static_cast<uint32_t>(value[2]) << 8) | static_cast<uint32_t>(value[3]);
        }
        return Status::OK();
    }

    Status read_double(bool little_endian, double* output) {
        if (_size - _position < sizeof(double)) {
            return invalid_wkb("unexpected end of input");
        }
        uint64_t bits = 0;
        if (little_endian) {
            for (size_t i = 0; i < sizeof(double); ++i) {
                bits |= static_cast<uint64_t>(_data[_position + i]) << (i * 8);
            }
        } else {
            for (size_t i = 0; i < sizeof(double); ++i) {
                bits = (bits << 8) | _data[_position + i];
            }
        }
        _position += sizeof(double);
        std::memcpy(output, &bits, sizeof(double));
        return Status::OK();
    }

    const uint8_t* _data;
    size_t _size;
    size_t _position = 0;
};

Status validate_geometry(const WkbGeometry& geometry, size_t depth) {
    if (depth > kMaxNestingDepth) {
        return Status::InvalidArgument("geometry nesting is too deep");
    }
    if (geometry.empty) {
        if (!geometry.coordinates.empty() || !geometry.rings.empty() || !geometry.children.empty()) {
            return Status::InvalidArgument("EMPTY geometry contains data");
        }
        return Status::OK();
    }

    auto validate_coordinates = [](const std::vector<WkbCoordinate>& coordinates) -> Status {
        if (coordinates.size() > kMaxElements) {
            return Status::InvalidArgument("geometry contains too many coordinates");
        }
        for (const auto& coordinate : coordinates) {
            if (!std::isfinite(coordinate.x) || !std::isfinite(coordinate.y)) {
                return Status::InvalidArgument("geometry coordinates must be finite");
            }
        }
        return Status::OK();
    };

    switch (geometry.type) {
    case WkbGeometryType::POINT:
        if (geometry.coordinates.size() != 1 || !geometry.rings.empty() || !geometry.children.empty()) {
            return Status::InvalidArgument("POINT must contain exactly one coordinate");
        }
        return validate_coordinates(geometry.coordinates);
    case WkbGeometryType::LINESTRING:
        if (geometry.coordinates.size() < 2 || !geometry.rings.empty() || !geometry.children.empty()) {
            return Status::InvalidArgument("LINESTRING must contain at least two coordinates");
        }
        return validate_coordinates(geometry.coordinates);
    case WkbGeometryType::POLYGON:
        if (!geometry.coordinates.empty() || geometry.rings.empty() || !geometry.children.empty()) {
            return Status::InvalidArgument("POLYGON must contain at least one ring");
        }
        if (geometry.rings.size() > kMaxElements) {
            return Status::InvalidArgument("geometry contains too many polygon rings");
        }
        for (const auto& ring : geometry.rings) {
            if (ring.size() < 4 || !(ring.front() == ring.back())) {
                return Status::InvalidArgument("polygon ring must be closed and contain at least four points");
            }
            RETURN_IF_ERROR(validate_coordinates(ring));
        }
        return Status::OK();
    case WkbGeometryType::MULTIPOINT:
    case WkbGeometryType::MULTILINESTRING:
    case WkbGeometryType::MULTIPOLYGON:
    case WkbGeometryType::GEOMETRYCOLLECTION:
        if (!geometry.coordinates.empty() || !geometry.rings.empty() || geometry.children.empty()) {
            return Status::InvalidArgument("non-empty collection must contain child geometries");
        }
        if (geometry.children.size() > kMaxElements) {
            return Status::InvalidArgument("geometry contains too many child geometries");
        }
        for (const auto& child : geometry.children) {
            if (geometry.type == WkbGeometryType::MULTIPOINT && child.type != WkbGeometryType::POINT) {
                return Status::InvalidArgument("MULTIPOINT child is not a POINT");
            }
            if (geometry.type == WkbGeometryType::MULTILINESTRING && child.type != WkbGeometryType::LINESTRING) {
                return Status::InvalidArgument("MULTILINESTRING child is not a LINESTRING");
            }
            if (geometry.type == WkbGeometryType::MULTIPOLYGON && child.type != WkbGeometryType::POLYGON) {
                return Status::InvalidArgument("MULTIPOLYGON child is not a POLYGON");
            }
            RETURN_IF_ERROR(validate_geometry(child, depth + 1));
        }
        return Status::OK();
    }
    return Status::InvalidArgument("unknown geometry type");
}

void append_uint32(uint32_t value, std::string* output) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        output->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

void append_double(double value, std::string* output) {
    uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(double));
    for (size_t i = 0; i < sizeof(double); ++i) {
        output->push_back(static_cast<char>((bits >> (i * 8)) & 0xff));
    }
}

Status write_wkb(const WkbGeometry& geometry, std::string* output) {
    output->push_back(1); // canonical little-endian encoding
    append_uint32(static_cast<uint32_t>(geometry.type), output);

    switch (geometry.type) {
    case WkbGeometryType::POINT:
        if (geometry.empty) {
            const double nan = std::numeric_limits<double>::quiet_NaN();
            append_double(nan, output);
            append_double(nan, output);
        } else {
            append_double(geometry.coordinates[0].x, output);
            append_double(geometry.coordinates[0].y, output);
        }
        return Status::OK();
    case WkbGeometryType::LINESTRING:
        append_uint32(static_cast<uint32_t>(geometry.coordinates.size()), output);
        for (const auto& coordinate : geometry.coordinates) {
            append_double(coordinate.x, output);
            append_double(coordinate.y, output);
        }
        return Status::OK();
    case WkbGeometryType::POLYGON:
        append_uint32(static_cast<uint32_t>(geometry.rings.size()), output);
        for (const auto& ring : geometry.rings) {
            append_uint32(static_cast<uint32_t>(ring.size()), output);
            for (const auto& coordinate : ring) {
                append_double(coordinate.x, output);
                append_double(coordinate.y, output);
            }
        }
        return Status::OK();
    case WkbGeometryType::MULTIPOINT:
    case WkbGeometryType::MULTILINESTRING:
    case WkbGeometryType::MULTIPOLYGON:
    case WkbGeometryType::GEOMETRYCOLLECTION:
        append_uint32(static_cast<uint32_t>(geometry.children.size()), output);
        for (const auto& child : geometry.children) {
            RETURN_IF_ERROR(write_wkb(child, output));
        }
        return Status::OK();
    }
    return Status::InvalidArgument("unknown geometry type");
}

std::string format_number(double value) {
    return fmt::format("{}", value);
}

void append_coordinate(const WkbCoordinate& coordinate, std::string* output) {
    output->append(format_number(coordinate.x));
    output->push_back(' ');
    output->append(format_number(coordinate.y));
}

void append_coordinate_list(const std::vector<WkbCoordinate>& coordinates, std::string* output) {
    for (size_t i = 0; i < coordinates.size(); ++i) {
        if (i != 0) {
            output->append(", ");
        }
        append_coordinate(coordinates[i], output);
    }
}

void append_ring_list(const std::vector<std::vector<WkbCoordinate>>& rings, std::string* output) {
    for (size_t i = 0; i < rings.size(); ++i) {
        if (i != 0) {
            output->append(", ");
        }
        output->push_back('(');
        append_coordinate_list(rings[i], output);
        output->push_back(')');
    }
}

Status write_wkt(const WkbGeometry& geometry, std::string* output) {
    output->append(WkbCodec::type_name(geometry.type));
    if (geometry.empty) {
        output->append(" EMPTY");
        return Status::OK();
    }
    output->append(" (");
    switch (geometry.type) {
    case WkbGeometryType::POINT:
        append_coordinate(geometry.coordinates[0], output);
        break;
    case WkbGeometryType::LINESTRING:
        append_coordinate_list(geometry.coordinates, output);
        break;
    case WkbGeometryType::POLYGON:
        append_ring_list(geometry.rings, output);
        break;
    case WkbGeometryType::MULTIPOINT:
        for (size_t i = 0; i < geometry.children.size(); ++i) {
            if (i != 0) {
                output->append(", ");
            }
            const auto& child = geometry.children[i];
            if (child.empty) {
                output->append("EMPTY");
            } else {
                output->push_back('(');
                append_coordinate(child.coordinates[0], output);
                output->push_back(')');
            }
        }
        break;
    case WkbGeometryType::MULTILINESTRING:
        for (size_t i = 0; i < geometry.children.size(); ++i) {
            if (i != 0) {
                output->append(", ");
            }
            const auto& child = geometry.children[i];
            if (child.empty) {
                output->append("EMPTY");
            } else {
                output->push_back('(');
                append_coordinate_list(child.coordinates, output);
                output->push_back(')');
            }
        }
        break;
    case WkbGeometryType::MULTIPOLYGON:
        for (size_t i = 0; i < geometry.children.size(); ++i) {
            if (i != 0) {
                output->append(", ");
            }
            const auto& child = geometry.children[i];
            if (child.empty) {
                output->append("EMPTY");
            } else {
                output->push_back('(');
                append_ring_list(child.rings, output);
                output->push_back(')');
            }
        }
        break;
    case WkbGeometryType::GEOMETRYCOLLECTION:
        for (size_t i = 0; i < geometry.children.size(); ++i) {
            if (i != 0) {
                output->append(", ");
            }
            RETURN_IF_ERROR(write_wkt(geometry.children[i], output));
        }
        break;
    }
    output->push_back(')');
    return Status::OK();
}

} // namespace

Status WkbCodec::parse_wkt(std::string_view input, WkbGeometry* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("WKT output must not be null");
    }
    *output = WkbGeometry();
    WktParser parser(input);
    RETURN_IF_ERROR(parser.parse(output));
    return validate_geometry(*output, 0);
}

Status WkbCodec::parse_wkb(const Slice& input, WkbGeometry* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("WKB output must not be null");
    }
    if (input.data == nullptr || input.size == 0) {
        return invalid_wkb("input is empty");
    }
    *output = WkbGeometry();
    WkbReader reader(input);
    RETURN_IF_ERROR(reader.parse(output));
    return validate_geometry(*output, 0);
}

Status WkbCodec::to_wkb(const WkbGeometry& geometry, std::string* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("WKB output must not be null");
    }
    RETURN_IF_ERROR(validate_geometry(geometry, 0));
    output->clear();
    return write_wkb(geometry, output);
}

Status WkbCodec::to_wkt(const WkbGeometry& geometry, std::string* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("WKT output must not be null");
    }
    RETURN_IF_ERROR(validate_geometry(geometry, 0));
    output->clear();
    return write_wkt(geometry, output);
}

const char* WkbCodec::type_name(WkbGeometryType type) {
    switch (type) {
    case WkbGeometryType::POINT:
        return "POINT";
    case WkbGeometryType::LINESTRING:
        return "LINESTRING";
    case WkbGeometryType::POLYGON:
        return "POLYGON";
    case WkbGeometryType::MULTIPOINT:
        return "MULTIPOINT";
    case WkbGeometryType::MULTILINESTRING:
        return "MULTILINESTRING";
    case WkbGeometryType::MULTIPOLYGON:
        return "MULTIPOLYGON";
    case WkbGeometryType::GEOMETRYCOLLECTION:
        return "GEOMETRYCOLLECTION";
    }
    return "UNKNOWN";
}

} // namespace starrocks
