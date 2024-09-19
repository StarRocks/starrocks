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

#include "arrow_flight_sql_service.h"

#include <arrow/array/builder_binary.h>
#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <netinet/in.h>
#include <openssl/aes.h>
#include <util/aes_util.h>
#include <util/arrow/utils.h>
#include <util/defer_op.h>
#include <util/slice.h>

#include "common/config.h"
#include "common/status.h"
#include "exec/arrow_flight_batch_reader.h"
#include "exprs/base64.h"
#include "service/backend_options.h"
#include "util/arrow/utils.h"
#include "util/uid_util.h"

namespace starrocks {

Status ArrowFlightSqlServer::start(int port) {
    arrow::flight::Location bind_location;
    RETURN_STATUS_IF_ERROR(arrow::flight::Location::ForGrpcTcp(BackendOptions::get_service_bind_address(), port)
                                   .Value(&bind_location));
    arrow::flight::FlightServerOptions flight_options(bind_location);
    RETURN_STATUS_IF_ERROR(Init(flight_options));

    return Status::OK();
}

arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> ArrowFlightSqlServer::GetFlightInfoSchemas(
        const arrow::flight::ServerCallContext& context, const arrow::flight::sql::GetDbSchemas& command,
        const arrow::flight::FlightDescriptor& descriptor) {
    return arrow::Status::NotImplemented("GetFlightInfoSchemas Result");
}

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> ArrowFlightSqlServer::DoGetStatement(
        const arrow::flight::ServerCallContext& context, const arrow::flight::sql::StatementQueryTicket& command) {
    ARROW_ASSIGN_OR_RAISE(auto pair, decode_ticket(command.statement_handle));

    const std::string query_id = pair.second;
    TUniqueId queryid;
    parse_id(query_id, &queryid);

    std::shared_ptr<ArrowFlightBatchReader> reader = std::make_shared<ArrowFlightBatchReader>(queryid);
    return std::make_unique<arrow::flight::RecordBatchStream>(reader);
}

arrow::Result<std::vector<unsigned char>> ArrowFlightSqlServer::pkcs7_unpadding(const std::vector<unsigned char>& data) {
    if (data.empty()) {
        return arrow::Status::Invalid("Data is empty");
    }

    int padding_length = data.back(); // Use back() for the last element
    if (padding_length <= 0 || padding_length > 32) {
        return arrow::Status::Invalid("Invalid PKCS7 padding");
    }

    for (int i = 0; i < padding_length; ++i) {
        if (data[data.size() - 1 - i] != padding_length) {
            return arrow::Status::Invalid("Invalid PKCS7 padding");
        }
    }

    return std::vector<unsigned char>(data.begin(), data.end() - padding_length);
}

arrow::Result<std::pair<std::string, std::string>> ArrowFlightSqlServer::decode_ticket(const std::string& ticket) {
    if (config::arrow_flight_sql_ase_key.size() != 43) {
        return arrow::Status::Invalid("BE configuration item arrow_flight_sql_ase_key is invalid.");
    }

    const std::string encoding_aes_key = config::arrow_flight_sql_ase_key + "=";
    std::string aes_key_decode;
    aes_key_decode.resize(encoding_aes_key.size() + 3);
    int64_t len = base64_decode2(encoding_aes_key.data(), encoding_aes_key.size(), aes_key_decode.data());
    aes_key_decode.resize(len);

    std::string encrypted_data;
    encrypted_data.resize(ticket.size() + 3);
    len = base64_decode2(ticket.data(), ticket.size(), encrypted_data.data());
    encrypted_data.resize(len);

    std::vector<unsigned char> decrypted_data(encrypted_data.size() + 3);
    unsigned char iv[AES_BLOCK_SIZE];
    memcpy(iv, aes_key_decode.data(), AES_BLOCK_SIZE);
    len = AesUtil::decrypt(AES_256_CBC, (unsigned char*)encrypted_data.data(), encrypted_data.size(),
                           (unsigned char*)aes_key_decode.data(), aes_key_decode.size(), iv, false,
                           decrypted_data.data());
    if (len < 0) {
        return arrow::Status::Invalid("Malformed ticket");
    }
    decrypted_data.resize(len);

    ARROW_ASSIGN_OR_RAISE(decrypted_data, pkcs7_unpadding(decrypted_data));
    if (decrypted_data.size() < 4) {
        return arrow::Status::Invalid("Malformed ticket");
    }

    std::string decrypted_string(decrypted_data.begin(), decrypted_data.end());
    if (decrypted_data.size() < 20) {
        return arrow::Status::Invalid("Malformed ticket");
    }

    uint32_t msg_len = 0;
    memcpy(&msg_len, decrypted_data.data() + 16, 4);
    msg_len = ntohl(msg_len);
    if (decrypted_string.size() < 16 + 4 + msg_len) {
        return arrow::Status::Invalid("Malformed ticket");
    }

    std::string message = decrypted_string.substr(16 + 4, msg_len);
    auto divider = message.find(':');
    if (divider == std::string::npos) {
        return arrow::Status::Invalid("Malformed ticket");
    }

    std::string query_id = message.substr(0, divider);
    std::string sql = message.substr(divider + 1);

    return std::make_pair(std::move(sql), std::move(query_id));
}

} // namespace starrocks
