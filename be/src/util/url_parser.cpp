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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/url_parser.cpp

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

#include "util/url_parser.h"

#include "runtime/string_value.hpp"

namespace starrocks {

const StringValue UrlParser::_s_url_authority(const_cast<char*>("AUTHORITY"), 9);
const StringValue UrlParser::_s_url_file(const_cast<char*>("FILE"), 4);
const StringValue UrlParser::_s_url_host(const_cast<char*>("HOST"), 4);
const StringValue UrlParser::_s_url_path(const_cast<char*>("PATH"), 4);
const StringValue UrlParser::_s_url_protocol(const_cast<char*>("PROTOCOL"), 8);
const StringValue UrlParser::_s_url_query(const_cast<char*>("QUERY"), 5);
const StringValue UrlParser::_s_url_ref(const_cast<char*>("REF"), 3);
const StringValue UrlParser::_s_url_userinfo(const_cast<char*>("USERINFO"), 8);
const StringValue UrlParser::_s_protocol(const_cast<char*>("://"), 3);
const StringValue UrlParser::_s_at(const_cast<char*>("@"), 1);
const StringValue UrlParser::_s_slash(const_cast<char*>("/"), 1);
const StringValue UrlParser::_s_colon(const_cast<char*>(":"), 1);
const StringValue UrlParser::_s_question(const_cast<char*>("?"), 1);
const StringValue UrlParser::_s_hash(const_cast<char*>("#"), 1);
const StringSearch UrlParser::_s_protocol_search(&_s_protocol);
const StringSearch UrlParser::_s_at_search(&_s_at);
const StringSearch UrlParser::_s_slash_search(&_s_slash);
const StringSearch UrlParser::_s_colon_search(&_s_colon);
const StringSearch UrlParser::_s_question_search(&_s_question);
const StringSearch UrlParser::_s_hash_search(&_s_hash);

bool UrlParser::parse_url(const StringValue& url, UrlPart part, StringValue* result) {
    result->ptr = nullptr;
    result->len = 0;
    // Remove leading and trailing spaces.
    StringValue trimmed_url = url.trim();

    // All parts require checking for the _s_protocol.
    int32_t protocol_pos = _s_protocol_search.search(&trimmed_url);
    if (protocol_pos < 0) {
        return false;
    }

    // Positioned to first char after '://'.
    StringValue protocol_end = trimmed_url.substring(protocol_pos + _s_protocol.len);

    switch (part) {
    case AUTHORITY: {
        // Find first '/'.
        int32_t end_pos = _s_slash_search.search(&protocol_end);
        *result = protocol_end.substring(0, end_pos);
        break;
    }

    case FILE:
    case PATH: {
        // Find first '/'.
        int32_t start_pos = _s_slash_search.search(&protocol_end);

        if (start_pos < 0) {
            // Return empty string. This is what Hive does.
            return true;
        }

        StringValue path_start = protocol_end.substring(start_pos);
        int32_t end_pos;

        if (part == FILE) {
            // End _s_at '#'.
            end_pos = _s_hash_search.search(&path_start);
        } else {
            // End string _s_at next '?' or '#'.
            end_pos = _s_question_search.search(&path_start);

            if (end_pos < 0) {
                // No '?' was found, look for '#'.
                end_pos = _s_hash_search.search(&path_start);
            }
        }

        *result = path_start.substring(0, end_pos);
        break;
    }

    case HOST: {
        // Find '@'.
        int32_t start_pos = _s_at_search.search(&protocol_end);

        if (start_pos < 0) {
            // No '@' was found, i.e., no user:pass info was given, start after _s_protocol.
            start_pos = 0;
        } else {
            // Skip '@'.
            start_pos += _s_at.len;
        }

        StringValue host_start = protocol_end.substring(start_pos);
        // Find ':' to strip out port.
        int32_t end_pos = _s_colon_search.search(&host_start);

        if (end_pos < 0) {
            // No port was given. search for '/' to determine ending position.
            end_pos = _s_slash_search.search(&host_start);
        }

        *result = host_start.substring(0, end_pos);
        break;
    }

    case PROTOCOL: {
        *result = trimmed_url.substring(0, protocol_pos);
        break;
    }

    case QUERY: {
        // Find first '?'.
        int32_t start_pos = _s_question_search.search(&protocol_end);

        if (start_pos < 0) {
            // Indicate no query was found.
            return false;
        }

        StringValue query_start = protocol_end.substring(start_pos + _s_question.len);
        // End string _s_at next '#'.
        int32_t end_pos = _s_hash_search.search(&query_start);
        *result = query_start.substring(0, end_pos);
        break;
    }

    case REF: {
        // Find '#'.
        int32_t start_pos = _s_hash_search.search(&protocol_end);

        if (start_pos < 0) {
            // Indicate no user and pass were given.
            return false;
        }

        *result = protocol_end.substring(start_pos + _s_hash.len);
        break;
    }

    case USERINFO: {
        // Find '@'.
        int32_t end_pos = _s_at_search.search(&protocol_end);

        if (end_pos < 0) {
            // Indicate no user and pass were given.
            return false;
        }

        *result = protocol_end.substring(0, end_pos);
        break;
    }

    case INVALID:
        return false;
    }

    return true;
}

UrlParser::UrlPart UrlParser::get_url_part(const StringValue& part) {
    // Quick filter on requested URL part, based on first character.
    // Hive requires the requested URL part to be all upper case.
    if (part.len <= 0) {
        return INVALID;
    }

    switch (part.ptr[0]) {
    case 'A': {
        if (!part.eq(_s_url_authority)) {
            return INVALID;
        }

        return AUTHORITY;
    }

    case 'F': {
        if (!part.eq(_s_url_file)) {
            return INVALID;
        }

        return FILE;
    }

    case 'H': {
        if (!part.eq(_s_url_host)) {
            return INVALID;
        }

        return HOST;
    }

    case 'P': {
        if (part.eq(_s_url_path)) {
            return PATH;
        } else if (part.eq(_s_url_protocol)) {
            return PROTOCOL;
        } else {
            return INVALID;
        }
    }

    case 'Q': {
        if (!part.eq(_s_url_query)) {
            return INVALID;
        }

        return QUERY;
    }

    case 'R': {
        if (!part.eq(_s_url_ref)) {
            return INVALID;
        }

        return REF;
    }

    case 'U': {
        if (!part.eq(_s_url_userinfo)) {
            return INVALID;
        }

        return USERINFO;
    }

    default:
        return INVALID;
    }
}

} // namespace starrocks
