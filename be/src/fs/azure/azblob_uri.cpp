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

#include "fs/azure/azblob_uri.h"

#include <absl/strings/str_split.h>
#include <fmt/format.h>

#include "fs/azure/utils.h"

namespace starrocks {

bool AzBlobURI::parse(std::string_view uri) {
    std::vector<std::string_view> tokens = absl::StrSplit(uri, absl::MaxSplits(kSchemeSeparator, 1));
    if (tokens.size() != 2) {
        return false;
    }

    _scheme = tokens[0];

    if (_scheme == kHttpScheme || _scheme == kHttpsScheme) {
        _is_path_style = is_path_style(tokens[1]);

        if (_is_path_style) {
            return parse_path_style(tokens[1]);
        } else {
            return parse_host_style(tokens[1]);
        }
    } else if (_scheme == kWasbScheme || _scheme == kWasbsScheme) {
        _is_path_style = false;

        return parse_hdfs_style(tokens[1]);
    } else {
        return false;
    }
}

// We define that host containing .blob. is host style, otherwise path style
bool AzBlobURI::is_path_style(std::string_view uri_with_no_scheme) const {
    return uri_with_no_scheme.rfind(".blob.", uri_with_no_scheme.find('/')) == uri_with_no_scheme.npos;
}

bool AzBlobURI::parse_host_style(std::string_view uri_with_no_scheme) {
    std::vector<std::string_view> slash_splits = absl::StrSplit(uri_with_no_scheme, absl::MaxSplits('/', 2));
    if (slash_splits.empty()) {
        return false;
    }

    std::vector<std::string_view> dot_splits = absl::StrSplit(slash_splits[0], absl::MaxSplits('.', 1));
    if (dot_splits.size() != 2) {
        return false;
    }

    _account = dot_splits[0];
    _endpoint_suffix = dot_splits[1];

    if (_account.empty() || _endpoint_suffix.empty()) {
        return false;
    }

    if (slash_splits.size() > 1) {
        _container = slash_splits[1];

        if (slash_splits.size() > 2) {
            _blob_name = slash_splits[2];
        } else {
            _blob_name.clear();
        }
    } else {
        _container.clear();
        _blob_name.clear();
    }

    return true;
}

bool AzBlobURI::parse_path_style(std::string_view uri_with_no_scheme) {
    std::vector<std::string_view> slash_splits = absl::StrSplit(uri_with_no_scheme, absl::MaxSplits('/', 3));
    if (slash_splits.size() < 2) {
        return false;
    }

    _endpoint_suffix = slash_splits[0];
    _account = slash_splits[1];

    if (slash_splits.size() > 2) {
        _container = slash_splits[2];

        if (slash_splits.size() > 3) {
            _blob_name = slash_splits[3];
        } else {
            _blob_name.clear();
        }
    } else {
        _container.clear();
        _blob_name.clear();
    }

    return true;
}

bool AzBlobURI::parse_hdfs_style(std::string_view uri_with_no_scheme) {
    std::vector<std::string_view> slash_splits = absl::StrSplit(uri_with_no_scheme, absl::MaxSplits('/', 1));
    if (slash_splits.empty()) {
        return false;
    }

    std::vector<std::string_view> dot_splits = absl::StrSplit(slash_splits[0], absl::MaxSplits('.', 1));
    if (dot_splits.size() != 2) {
        return false;
    }

    std::vector<std::string_view> at_splits = absl::StrSplit(dot_splits[0], absl::MaxSplits('@', 1));
    if (at_splits.size() != 2) {
        return false;
    }

    _container = at_splits[0];
    _account = at_splits[1];
    _endpoint_suffix = dot_splits[1];

    if (_container.empty() || _account.empty() || _endpoint_suffix.empty()) {
        return false;
    }

    if (slash_splits.size() > 1) {
        _blob_name = slash_splits[1];
    } else {
        _blob_name.clear();
    }

    return true;
}

std::string AzBlobURI::get_http_style_scheme() const {
    if (_scheme == kWasbsScheme) {
        return kHttpsScheme;
    } else if (_scheme == kWasbScheme) {
        return kHttpScheme;
    } else {
        return _scheme;
    }
}

std::string AzBlobURI::get_account_uri() const {
    if (_is_path_style) {
        return fmt::format("{}{}{}/{}", get_http_style_scheme(), kSchemeSeparator, _endpoint_suffix, _account);
    } else {
        return fmt::format("{}{}{}.{}", get_http_style_scheme(), kSchemeSeparator, _account, _endpoint_suffix);
    }
}

std::string AzBlobURI::get_container_uri() const {
    if (_is_path_style) {
        return fmt::format("{}{}{}/{}/{}", get_http_style_scheme(), kSchemeSeparator, _endpoint_suffix, _account,
                           _container);
    } else {
        return fmt::format("{}{}{}.{}/{}", get_http_style_scheme(), kSchemeSeparator, _account, _endpoint_suffix,
                           _container);
    }
}

std::string AzBlobURI::get_blob_uri() const {
    if (_is_path_style) {
        return fmt::format("{}{}{}/{}/{}/{}", get_http_style_scheme(), kSchemeSeparator, _endpoint_suffix, _account,
                           _container, _blob_name);
    } else {
        return fmt::format("{}{}{}.{}/{}/{}", get_http_style_scheme(), kSchemeSeparator, _account, _endpoint_suffix,
                           _container, _blob_name);
    }
}

} // namespace starrocks
