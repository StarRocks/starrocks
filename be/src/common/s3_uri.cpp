// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "common/s3_uri.h"

#include <brpc/uri.h>
#include <fmt/format.h>
#include <gutil/strings/util.h>

namespace starrocks {

bool S3URI::parse(const char* uri_str) {
    brpc::URI uri;
    if (uri.SetHttpURL(uri_str) != 0) {
        return false;
    }

    _scheme = uri.scheme();

    const std::string& host = uri.host();
    std::string_view path;

    if (!uri.path().empty() && uri.path()[0] == '/') {
        path = std::string_view(uri.path().data() + 1, uri.path().size() - 1);
    } else {
        path = uri.path();
    }

    if (host.find('.') == std::string::npos) {
        // URL like S3://bucket-name/key-name
        _bucket = host;
        _key = path;
    } else if (HasPrefixString(host, "s3.") && HasSuffixString(host, ".amazonaws.com")) {
        // S3's specific path-style URL: https://s3.<Region>.amazonaws.com/<Bucket>/<Object Key>
        auto region_start = host.data() + sizeof("s3.") - 1;
        auto region_end = host.data() + host.size() - sizeof(".amazonaws.com") + 1;
        if (region_start >= region_end) {
            return false;
        }
        _region.assign(region_start, region_end);
        _endpoint = fmt::format("s3.{}.amazonaws.com", _region);
        auto pos = path.find('/');
        if (pos != std::string::npos) {
            _bucket = path.substr(0, pos);
            _key = path.substr(pos + 1);
        } else {
            _bucket = path;
        }
    } else if (HasSuffixString(host, ".amazonaws.com")) {
        // S3's specific virtual-hosted–style URL: https://<Bucket>.s3.<Region>.amazonaws.com/<Object Key>
        auto pos = host.find(".s3.");
        if (pos == std::string::npos || pos == 0) {
            return false;
        }
        _bucket = host.substr(0, pos);
        _endpoint = host.substr(pos + 1); // s3.<Region>.amazonaws.com
        auto region_start = host.data() + pos + sizeof(".s3.") - 1;
        auto region_end = host.data() + host.size() - sizeof(".amazonaws.com") + 1;
        if (region_start >= region_end) {
            return false;
        }
        _region.assign(region_start, region_end);
        _key = path;
    } else {
        // URL like oss://<Bucket>.<Endpoint>/<Object>
        auto pos = host.find('.');
        if (pos == std::string::npos || pos == 0) {
            return false;
        }
        _bucket = host.substr(0, pos);
        _endpoint = host.substr(pos + 1);
        _key = path;
    }
    // The object key can be empty, but the bucket cannot be empty.
    return !_bucket.empty();
}

} // namespace starrocks
