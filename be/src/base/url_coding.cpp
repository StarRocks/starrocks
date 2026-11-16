// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "base/url_coding.h"

#include <curl/curl.h>

#include <utility>

#include "base/utility/defer_op.h"
#include "simdutf.h"

namespace starrocks {

void base64_encode(const std::string& in, std::string* out) {
    if (&in == out) {
        std::string tmp;
        base64_encode(in, &tmp);
        *out = std::move(tmp);
        return;
    }
    out->resize(simdutf::base64_length_from_binary(in.size()));
    size_t len = simdutf::binary_to_base64(in.data(), in.size(), out->data());
    out->resize(len);
}

bool base64_decode(const std::string& in, std::string* out) {
    if (&in == out) {
        std::string tmp;
        if (!base64_decode(in, &tmp)) {
            out->clear();
            return false;
        }
        *out = std::move(tmp);
        return true;
    }
    out->resize(simdutf::maximal_binary_length_from_base64(in.data(), in.size()));
    simdutf::result r = simdutf::base64_to_binary(in.data(), in.size(), out->data());
    if (r.error != simdutf::error_code::SUCCESS) {
        out->clear();
        return false;
    }
    out->resize(r.count);
    return true;
}

// refers to https://stackoverflow.com/questions/154536/encode-decode-urls-in-c
StatusOr<std::string> url_encode(const std::string& decoded) {
    char* encoded_value = curl_easy_escape(nullptr, decoded.c_str(), static_cast<int>(decoded.length()));
    if (encoded_value == nullptr) {
        return Status::InternalError("curl_easy_escape returned NULL (libcurl out of memory)");
    }
    DeferOp guard([&] { curl_free(encoded_value); });
    return std::string(encoded_value);
}

StatusOr<std::string> url_decode(const std::string& in) {
    int decoded_length = 0;
    char* decoded_value = curl_easy_unescape(nullptr, in.c_str(), static_cast<int>(in.length()), &decoded_length);
    if (decoded_value == nullptr) {
        return Status::InvalidArgument("invalid encoding in URL");
    }
    DeferOp guard([&] { curl_free(decoded_value); });
    // Use the explicit length so embedded NULs in the decoded payload are preserved.
    return std::string(decoded_value, decoded_length);
}

} // namespace starrocks
