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

#pragma once

#include <string>

namespace starrocks {

class AzBlobURI {
public:
    AzBlobURI() = default;

    // Parses an azure blob storage uri into its components.
    // Supports the following uri styles:
    //  1. Host-style: http[s]://${account_name}.blob.core.windows.net/${container_name}/${blob_name}
    //  2. Path-style: http[s]://blob.core.windows.net/${account_name}/${container_name}/${blob_name}
    //  3. HDFS-style: wasb[s]://${container_name}@${account_name}.blob.core.windows.net/${blob_name}
    bool parse(std::string_view uri);

    bool is_path_style() const { return _is_path_style; }

    std::string& scheme() { return _scheme; }
    const std::string& scheme() const { return _scheme; }

    std::string& account() { return _account; }
    const std::string& account() const { return _account; }

    std::string& endpoint_suffix() { return _endpoint_suffix; }
    const std::string& endpoint_suffix() const { return _endpoint_suffix; }

    std::string& container() { return _container; }
    const std::string& container() const { return _container; }

    std::string& blob_name() { return _blob_name; }
    const std::string& blob_name() const { return _blob_name; }

    // Get http style uri
    std::string get_account_uri() const;
    std::string get_container_uri() const;
    std::string get_blob_uri() const;

private:
    bool is_path_style(std::string_view uri_with_no_scheme) const;

    std::string get_http_style_scheme() const;

    bool parse_host_style(std::string_view uri_with_no_scheme);
    bool parse_path_style(std::string_view uri_with_no_scheme);
    bool parse_hdfs_style(std::string_view uri_with_no_scheme);

private:
    bool _is_path_style = false;
    std::string _scheme;
    std::string _account;
    std::string _endpoint_suffix;
    std::string _container;
    std::string _blob_name;
};

} // namespace starrocks
