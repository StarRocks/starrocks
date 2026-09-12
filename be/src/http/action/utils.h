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

#include <event2/http.h>
#include <fmt/format.h>

#include <string>

#include "base/network/network_util.h"
#include "common/logging.h"
#include "common/system/master_info.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_request.h"

namespace starrocks {

// Redirect a request to the FE leader while this BE is shutting down, so that the FE can pick
// another coordinator BE. The caller must only redirect when the FE has observed the shutdown
// heartbeat (`is_frontend_aware_of_exit()`), i.e. the delay-path cutoff: otherwise (fallback
// deadline or heartbeat=false cutoff) the FE may not know this BE is exiting and could pick it
// again. No `exclude_bes` query param is attached for the same reason. Returns true if the 307
// redirect was sent; false if the FE master info could not be obtained (the caller is
// responsible for replying with an error). `source` names the redirect origin in logs.
inline bool redirect_to_fe_leader(HttpRequest* req, const std::string& label, const std::string& source) {
    MasterInfoPtr master_info;
    // get_master_info() may succeed with an unset FE address (no heartbeat received yet); a
    // redirect to an empty host or port 0 would be a malformed Location, so fall back instead.
    if (!get_master_info(&master_info) || master_info->network_address.hostname.empty() ||
        master_info->http_port <= 0) {
        LOG(WARNING) << "[redirect] " << source
                     << " get_master_info failed or FE address unset, reply SERVICE_UNAVAILABLE, label=" << label;
        return false;
    }
    std::string redirect_url = fmt::format(
            "http://{}{}", get_host_port(master_info->network_address.hostname, master_info->http_port), req->uri());
    LOG(INFO) << "[redirect] " << source << " sending 307 to FE leader, redirect_url=" << redirect_url
              << ", label=" << label;
    evhttp_add_header(evhttp_request_get_output_headers(req->get_evhttp_request()), "Location", redirect_url.c_str());
    HttpChannel::send_reply(req, HttpStatus::TEMPORARY_REDIRECT, "");
    return true;
}

} // namespace starrocks
