// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>
#include <string_view>

namespace starrocks {

// Creates a new S3URI in the form of scheme://bucket/key?query#fragment
// The URI supports any valid URI schemes to be backwards compatible with s3a and s3n,
// and also allows users to use S3ObjectStore with other S3-compatible object storage services.
class S3URI {
public:
    explicit S3URI(std::string location) : _location(std::move(location)) {}

    bool parse();

    // REQUIRE: has been `parse()`d
    std::string_view schema() const { return _schema; }

    // REQUIRE: has been `parse()`d
    std::string_view bucket() const { return _bucket; }

    // REQUIRE: has been `parse()`d
    std::string_view object() const { return _object; }

    const std::string& location() const { return _location; }

private:
    constexpr static const char kSchemaDelim[] = "://";
    constexpr static const char kPathDelim[] = "/";
    constexpr static const char kQueryDelim[] = "?";
    constexpr static const char kFragmentDelim[] = "#";

    std::string _location;
    std::string_view _schema;
    std::string_view _bucket;
    std::string_view _object;
};

inline bool S3URI::parse() {
    auto pos0 = _location.find(kSchemaDelim);
    if (pos0 == std::string::npos || pos0 == 0) {
        return false;
    }
    _schema = std::string_view(_location.data(), pos0);

    // Move pos0 to the end of kSchemaDelim
    pos0 = pos0 + sizeof(kSchemaDelim) - 1;

    auto pos1 = _location.find(kPathDelim, pos0);
    if (pos1 == std::string::npos || pos0 == pos1) {
        return false;
    }
    _bucket = std::string_view(_location.data() + pos0, pos1 - pos0);

    pos1 = pos1 + sizeof(kPathDelim) - 1;
    _object = std::string_view(_location.data() + pos1, _location.size() - pos1);

    auto pos2 = _object.find(kQueryDelim);
    auto pos3 = _object.find(kFragmentDelim);
    if (pos2 != std::string::npos && pos3 != std::string::npos) {
        _object = std::string_view(_object.data(), std::min(pos2, pos3));
    } else if (pos2 != std::string::npos) {
        _object = std::string_view(_object.data(), pos2);
    } else if (pos3 != std::string::npos) {
        _object = std::string_view(_object.data(), pos3);
    }
    return true;
}

} // namespace starrocks
