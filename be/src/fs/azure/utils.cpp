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

#include "fs/azure/utils.h"

#include <fmt/format.h>

namespace starrocks {

using namespace Azure::Core::Http;

Status azure_error_to_status(Azure::Core::Http::HttpStatusCode code, std::string_view message,
                             std::string_view object) {
    switch (code) {
        // No HTTP status code.
    case HttpStatusCode::None:
        return Status::Unknown(message);

        // === 1xx (information) Status Codes: ===
    case HttpStatusCode::Continue:           ///< HTTP 100 Continue.
    case HttpStatusCode::SwitchingProtocols: ///< HTTP 101 Switching Protocols.
    case HttpStatusCode::Processing:         ///< HTTP 102 Processing.
    case HttpStatusCode::EarlyHints:         ///< HTTP 103 Early Hints.

        // === 2xx (successful) Status Codes: ===
    case HttpStatusCode::Ok:                          ///< HTTP 200 OK.
    case HttpStatusCode::Created:                     ///< HTTP 201 Created.
    case HttpStatusCode::Accepted:                    ///< HTTP 202 Accepted.
    case HttpStatusCode::NonAuthoritativeInformation: ///< HTTP 203 Non-Authoritative Information.
    case HttpStatusCode::NoContent:                   ///< HTTP 204 No Content.
    case HttpStatusCode::ResetContent:                ///< HTTP 205 Rest Content.
    case HttpStatusCode::PartialContent:              ///< HTTP 206 Partial Content.
    case HttpStatusCode::MultiStatus:                 ///< HTTP 207 Multi-Status.
    case HttpStatusCode::AlreadyReported:             ///< HTTP 208 Already Reported.
    case HttpStatusCode::IMUsed:                      ///< HTTP 226 IM Used.
        return Status::OK();

        // === 3xx (redirection) Status Codes: ===
    case HttpStatusCode::MultipleChoices:   ///< HTTP 300 Multiple Choices.
    case HttpStatusCode::MovedPermanently:  ///< HTTP 301 Moved Permanently.
    case HttpStatusCode::Found:             ///< HTTP 302 Found.
    case HttpStatusCode::SeeOther:          ///< HTTP 303 See Other.
    case HttpStatusCode::NotModified:       ///< HTTP 304 Not Modified.
    case HttpStatusCode::UseProxy:          ///< HTTP 305 Use Proxy.
    case HttpStatusCode::TemporaryRedirect: ///< HTTP 307 Temporary Redirect.
    case HttpStatusCode::PermanentRedirect: ///< HTTP 308 Permanent Redirect.
        return Status::Aborted(message);

        // === 4xx (client error) Status Codes: ===
    case HttpStatusCode::BadRequest: ///< HTTP 400 Bad Request.
        return Status::InvalidArgument(message);
    case HttpStatusCode::Unauthorized: ///< HTTP 401 Unauthorized.
        return Status::NotAuthorized(message);
    case HttpStatusCode::PaymentRequired: ///< HTTP 402 Payment Required.
        return Status::InvalidArgument(message);
    case HttpStatusCode::Forbidden: ///< HTTP 403 Forbidden.
        return Status::NotAuthorized(message);
    case HttpStatusCode::NotFound: ///< HTTP 404 Not Found.
        return Status::NotFound(fmt::format("Object {} does not exist", object));
    case HttpStatusCode::MethodNotAllowed:            ///< HTTP 405 Method Not Allowed.
    case HttpStatusCode::NotAcceptable:               ///< HTTP 406 Not Acceptable.
    case HttpStatusCode::ProxyAuthenticationRequired: ///< HTTP 407 Proxy Authentication Required.
        return Status::InvalidArgument(message);
    case HttpStatusCode::RequestTimeout: ///< HTTP 408 Request Timeout.
        return Status::TimedOut(message);
    case HttpStatusCode::Conflict: ///< HTTP 409 Conflict.
        return Status::InternalError(message);
    case HttpStatusCode::Gone:                 ///< HTTP 410 Gone.
    case HttpStatusCode::LengthRequired:       ///< HTTP 411 Length Required.
    case HttpStatusCode::PreconditionFailed:   ///< HTTP 412 Precondition Failed.
    case HttpStatusCode::PayloadTooLarge:      ///< HTTP 413 Payload Too Large.
    case HttpStatusCode::UriTooLong:           ///< HTTP 414 URI Too Long.
    case HttpStatusCode::UnsupportedMediaType: ///< HTTP 415 Unsupported Media Type.
    case HttpStatusCode::RangeNotSatisfiable:  ///< HTTP 416 Range Not Satisfiable.
    case HttpStatusCode::ExpectationFailed:    ///< HTTP 417 Expectation Failed.
    case HttpStatusCode::MisdirectedRequest:   ///< HTTP 421 Misdirected Request.
    case HttpStatusCode::UnprocessableEntity:  ///< HTTP 422 Unprocessable Entity.
    case HttpStatusCode::Locked:               ///< HTTP 423 Locked.
    case HttpStatusCode::FailedDependency:     ///< HTTP 424 Failed Dependency.
    case HttpStatusCode::TooEarly:             ///< HTTP 425 Too Early.
    case HttpStatusCode::UpgradeRequired:      ///< HTTP 426 Upgrade Required.
    case HttpStatusCode::PreconditionRequired: ///< HTTP 428 Precondition Required.
        return Status::InvalidArgument(message);
    case HttpStatusCode::TooManyRequests: ///< HTTP 429 Too Many Requests.
        return Status::ResourceBusy(message);
    case HttpStatusCode::RequestHeaderFieldsTooLarge: ///< HTTP 431 Request Header Fields Too Large.
    case HttpStatusCode::UnavailableForLegalReasons:  ///< HTTP 451 Unavailable For Legal Reasons.
        return Status::InvalidArgument(message);

        // === 5xx (server error) Status Codes: ===
    case HttpStatusCode::InternalServerError: ///< HTTP 500 Internal Server Error.
        return Status::InternalError(message);
    case HttpStatusCode::NotImplemented: ///< HTTP 501 Not Implemented.
        return Status::NotSupported(message);
    case HttpStatusCode::BadGateway: ///< HTTP 502 Bad Gateway.
        return Status::InternalError(message);
    case HttpStatusCode::ServiceUnavailable: ///< HTTP 503 Unavailable.
        return Status::ServiceUnavailable(message);
    case HttpStatusCode::GatewayTimeout:                ///< HTTP 504 Gateway Timeout.
    case HttpStatusCode::HttpVersionNotSupported:       ///< HTTP 505 HTTP Version Not Supported.
    case HttpStatusCode::VariantAlsoNegotiates:         ///< HTTP 506 Variant Also Negotiates.
    case HttpStatusCode::InsufficientStorage:           ///< HTTP 507 Insufficient Storage.
    case HttpStatusCode::LoopDetected:                  ///< HTTP 508 Loop Detected.
    case HttpStatusCode::NotExtended:                   ///< HTTP 510 Not Extended.
    case HttpStatusCode::NetworkAuthenticationRequired: ///< HTTP 511 Network Authentication
        ///< Required.
        return Status::InternalError(message);

    default:
        return Status::InternalError(message);
    }
}

} // namespace starrocks
