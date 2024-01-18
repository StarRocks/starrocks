// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>
#include <string_view>

namespace starrocks {

class S3URI {
public:
    S3URI() = default;

    // Decompose |uri| and set into corresponding fields.
    // Returns true on success, false otherwise.
    bool parse(const char* uri, const size_t size);
    bool parse(const std::string& uri) { return parse(uri.c_str(), uri.size()); }

    const std::string& scheme() const { return _scheme; }

    std::string& bucket() { return _bucket; }

    const std::string& bucket() const { return _bucket; }

    std::string& key() { return _key; }

    const std::string& key() const { return _key; }

    std::string& region() { return _region; }

    const std::string& region() const { return _region; }

    std::string& endpoint() { return _endpoint; }

    const std::string& endpoint() const { return _endpoint; }

    void set_scheme(std::string value) { _scheme = std::move(value); }

    void set_bucket(std::string value) { _bucket = std::move(value); }

    void set_key(std::string value) { _key = std::move(value); }

    void set_region(std::string value) { _region = std::move(value); }

    void set_endpoint(std::string value) { _endpoint = std::move(value); }

private:
    std::string _scheme;
    std::string _bucket;
    std::string _key;
    std::string _region;
    std::string _endpoint;
};

} // namespace starrocks
