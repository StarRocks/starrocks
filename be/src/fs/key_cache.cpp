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

#include "fs/key_cache.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/kms/KMSClient.h>
#include <aws/kms/model/DecryptRequest.h>
#include <aws/kms/model/DecryptResult.h>
#include <aws/sts/STSClient.h>
#include <curl/curl.h>

#include "base/format.h"
#include "base/metrics.h"
#include "base/url_coding.h"
#include "base/utility/defer_op.h"
#include "common/config.h"
#include "common/system/master_info.h"
#include "fs/encrypt_file.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"
#include "gutil/casts.h"
#include "runtime/client_cache.h"
#include "runtime/starrocks_metrics.h"
#include "util/global_metrics_registry.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

METRIC_DEFINE_INT_COUNTER(encryption_keys_created, MetricUnit::NOUNIT);
METRIC_DEFINE_INT_COUNTER(encryption_keys_unwrapped, MetricUnit::NOUNIT);
METRIC_DEFINE_INT_GAUGE(encryption_keys_in_cache, MetricUnit::NOUNIT);

EncryptionKey::EncryptionKey() = default;
EncryptionKey::EncryptionKey(EncryptionKeyPB pb) : _pb(std::move(pb)) {}
EncryptionKey::~EncryptionKey() = default;

static const std::string& get_identifier_from_pb(const EncryptionKeyPB& pb) {
    switch (pb.type()) {
    case NORMAL_KEY:
        return pb.has_plain_key() ? pb.plain_key() : pb.encrypted_key();
    case VAULT_PLAIN_KEY:
        return pb.key_desc();
    case KMS_KEY:
        return pb.key_desc();
    case KMS_DATA_KEY:
        return pb.key_desc();
    }
    CHECK(false) << fmt::format("get_identifier_from_pb not supported for type:{}", pb.type());
}

const std::string& EncryptionKey::identifier() const {
    return get_identifier_from_pb(_pb);
}

bool EncryptionKey::is_kek() const {
    return get_parent_id() == DEFAULT_MASTER_KYE_ID;
}

class NormalKey : public EncryptionKey {
public:
    NormalKey(EncryptionKeyPB pb) : EncryptionKey(std::move(pb)) {
        if (_pb.has_plain_key()) {
            // it's a plain master key
            _plain_key = _pb.plain_key();
        }
    }

    StatusOr<std::unique_ptr<EncryptionKey>> generate_key() const override {
        if (_plain_key.empty()) {
            return Status::InternalError("generate_key failed, empty plain_key");
        }
        if (algorithm() != AES_128) {
            return Status::NotSupported(fmt::format("algorithm not support:{}", algorithm()));
        }
        std::string pkey(16, '\0');
        ssl_random_bytes(pkey.data(), 16);
        ASSIGN_OR_RETURN(auto encrypted_key, wrap_key(algorithm(), _plain_key, pkey));
        EncryptionKeyPB pb;
        pb.set_parent_id(get_id());
        pb.set_type(NORMAL_KEY);
        pb.set_algorithm(algorithm());
        pb.set_encrypted_key(encrypted_key);
        auto ret = std::make_unique<NormalKey>(pb);
        ret->_plain_key = pkey;
        return std::move(ret);
    }

    // unwrap a child key wrapped by this key previously
    Status decrypt(EncryptionKey* child_key) const override {
        if (child_key->type() != NORMAL_KEY) {
            return Status::InternalError(fmt::format("decrypt failed, type not support:{}", child_key->type()));
        }
        auto key = down_cast<NormalKey*>(child_key);
        if (_plain_key.empty()) {
            return Status::InternalError("decrypt failed, empty plain_key");
        }
        if (key->_pb.encrypted_key().empty()) {
            return Status::InternalError("no encrypted_key to decrypt");
        }
        ASSIGN_OR_RETURN(key->_plain_key, unwrap_key(key->algorithm(), _plain_key, key->_pb.encrypted_key()));
        return Status::OK();
    }

    void set_plain_key(const std::string& plain_key) { _plain_key = plain_key; }

    StatusOr<std::string> get_plain_key() const override {
        if (_plain_key.empty()) {
            return Status::NotFound(fmt::format("plain key not available for key id:{}", get_id()));
        }
        return _plain_key;
    }

    bool is_decrypted() const override { return !_plain_key.empty(); }

    std::string to_string() const override {
        return fmt::format("id:{} parent:{} type:{} has_plain:{} has_encrypted:{}", get_id(), get_parent_id(), type(),
                           !_plain_key.empty(), _pb.has_encrypted_key());
    }

protected:
    std::string _plain_key;
};

StatusOr<std::string> get_token_from_vault_url(const std::string& url, std::string* url_without_query) {
    CURLU* h = curl_url(); // Initialize CURLU handle
    if (!h) {
        return Status::InternalError("init curlu failed");
    }

    CURLUcode uc = curl_url_set(h, CURLUPART_URL, url.c_str(), 0);
    if (uc) {
        curl_url_cleanup(h);
        return Status::InvalidArgument("curl_url_set failed");
    }

    char* query;
    *url_without_query = url;
    uc = curl_url_get(h, CURLUPART_QUERY, &query, CURLU_URLDECODE);
    std::string token;
    if (!uc) {
        std::string query_str(query);
        curl_free(query);
        curl_url_set(h, CURLUPART_QUERY, nullptr, 0);
        char* url_without_query_str = nullptr;
        curl_url_get(h, CURLUPART_URL, &url_without_query_str, 0);
        *url_without_query = url_without_query_str;
        curl_free(url_without_query_str);
        size_t token_pos = query_str.find("token=");
        if (token_pos != std::string::npos) {
            token_pos += 6;
            size_t end_pos = query_str.find('&', token_pos);
            if (end_pos != std::string::npos) {
                token = query_str.substr(token_pos, end_pos - token_pos);
            } else {
                token = query_str.substr(token_pos);
            }
        }
    }
    curl_url_cleanup(h);
    return token;
}

struct KeySpec {
    EncryptionAlgorithmPB algorithm;
    std::string key;
};

StatusOr<KeySpec> get_key_spec_from_vault_response(const std::string& vault_get_response) {
    // parse response as json
    rapidjson::Document doc;
    doc.Parse(vault_get_response.c_str());
    if (doc.HasParseError()) {
        return Status::InternalError("parse vault response failed");
    }
    if (!doc.HasMember("data")) {
        return Status::InternalError("no data field in vault response");
    }
    const auto& data = doc["data"];
    if (!data.HasMember("data")) {
        return Status::InternalError("no data.data field in vault response");
    }
    const auto& data_data = data["data"];
    // get plain_key field
    if (!data_data.HasMember("plain_key")) {
        return Status::InternalError("no data.data.plain_key field in vault response");
    }
    std::string spec_str = data_data["plain_key"].GetString();
    // parse <algo>:<key base64> format
    size_t pos = spec_str.find(':');
    if (pos == std::string::npos) {
        return Status::InternalError("invalid plain_key format");
    }
    std::string algo_str = spec_str.substr(0, pos);
    std::string algo_str_upper = algo_str;
    std::transform(algo_str_upper.begin(), algo_str_upper.end(), algo_str_upper.begin(), ::toupper);
    std::string key_str = spec_str.substr(pos + 1);
    KeySpec spec;
    if (!EncryptionAlgorithmPB_Parse(algo_str_upper, &spec.algorithm)) {
        return Status::InternalError(fmt::format("invalid algorithm:{}", algo_str));
    }
    if (!base64_decode(key_str, &spec.key)) {
        return Status::InternalError("base64 decode key failed");
    }
    return spec;
}

static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

static StatusOr<KeySpec> get_key_spec_from_vault(const std::string& url) {
    std::string url_without_query;
    std::string token;
    if (url.starts_with('/')) {
        // only has path, using VAULT_ADDR and VAULT_TOKEN to access vault service
        std::string vault_addr = config::vault_addr;
        if (vault_addr.empty()) {
            vault_addr = getenv("VAULT_ADDR");
        }
        if (vault_addr.empty()) {
            return Status::InternalError("no VAULT_ADDR found to access vault service");
        }
        url_without_query = vault_addr + url;
    } else {
        ASSIGN_OR_RETURN(token, get_token_from_vault_url(url, &url_without_query));
    }
    if (token.empty()) {
        token = config::vault_token;
    }
    if (token.empty()) {
        token = getenv("VAULT_TOKEN");
        if (token.empty()) {
            return Status::InternalError("no VAULT_TOKEN found in vault url, config or sysenv to access vault service");
        }
    }
    LOG(INFO) << "get key from vault: " << url_without_query;
    CURL* curl = curl_easy_init();
    DeferOp dd([&]() { curl_easy_cleanup(curl); });
    if (!curl) {
        return Status::InternalError("init curl failed");
    }
    curl_easy_setopt(curl, CURLOPT_URL, url_without_query.c_str());
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, fmt::format("X-Vault-Token:{}", token).c_str());
    DeferOp dh([&]() { curl_slist_free_all(headers); });
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::string resp_string;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp_string);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        std::string err = fmt::format("curl_easy_perform({}) failed {}", url_without_query, curl_easy_strerror(res));
        LOG(WARNING) << err;
        return Status::InternalError(err);
    }
    auto st = get_key_spec_from_vault_response(resp_string);
    if (st.ok()) {
        return st;
    } else {
        LOG(WARNING) << "get_key_spec_from_vault failed, url:" << url_without_query << "resp:" << resp_string
                     << " error:" << st.status().message();
        return st.status().clone_and_append(fmt::format(" url:{} resp:{}", url_without_query, resp_string));
    }
}

static std::mutex g_get_key_spec_from_vault_cache_lock;
static std::unique_ptr<std::pair<std::string, KeySpec>> g_get_key_spec_from_vault_cache;
static StatusOr<KeySpec> get_key_spec_from_vault_cached(const std::string& url) {
    std::lock_guard lg(g_get_key_spec_from_vault_cache_lock);
    if (g_get_key_spec_from_vault_cache && g_get_key_spec_from_vault_cache->first == url) {
        return g_get_key_spec_from_vault_cache->second;
    }
    auto spec = get_key_spec_from_vault(url);
    RETURN_IF_ERROR_WITH_WARN(spec, "get_key_spec_from_vault failed");
    g_get_key_spec_from_vault_cache = std::make_unique<std::pair<std::string, KeySpec>>(url, spec.value());
    return spec;
}

class VaultPlainKey : public NormalKey {
public:
    VaultPlainKey(EncryptionKeyPB pb) : NormalKey(std::move(pb)) {}

    EncryptionAlgorithmPB algorithm() const override { return _algorithm; }

    Status init_plain_key_from_vault() {
        if (_pb.key_desc().empty()) {
            return Status::InternalError("no key_desc in vault plain key pb");
        }
        ASSIGN_OR_RETURN(auto spec, get_key_spec_from_vault_cached(_pb.key_desc()));
        _plain_key = spec.key;
        _algorithm = spec.algorithm;
        return Status::OK();
    }

private:
    EncryptionAlgorithmPB _algorithm;
};

static StatusOr<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>> create_kms_credentials_provider() {
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credential_provider;
    if (!config::aws_kms_access_key.value().empty() && !config::aws_kms_secret_key.value().empty()) {
        credential_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(config::aws_kms_access_key,
                                                                                        config::aws_kms_secret_key);
        LOG(INFO) << "use aws_kms_access_key and aws_kms_secret_key to access KMS service";
    } else {
        credential_provider = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
        LOG(INFO) << "use DefaultAWSCredentialsProviderChain to access KMS service";
    }

    if (!config::aws_kms_iam_role_arn.value().empty()) {
        Aws::Client::ClientConfiguration config;
        if (!config::aws_kms_region.value().empty()) {
            config.region = config::aws_kms_region;
        }
        auto sts = std::make_shared<Aws::STS::STSClient>(credential_provider, config);
        credential_provider = std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                config::aws_kms_iam_role_arn, Aws::String(), config::aws_kms_external_id,
                Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts);
        LOG(INFO) << "use STSAssumeRoleCredentialsProvider to access KMS service";
    }
    return credential_provider;
}

static StatusOr<std::unique_ptr<Aws::KMS::KMSClient>> create_kms_client() {
    Aws::Client::ClientConfiguration config;
    if (!config::aws_kms_region.value().empty()) {
        config.region = config::aws_kms_region;
    }
    ASSIGN_OR_RETURN(auto credential_provider, create_kms_credentials_provider());
    return std::make_unique<Aws::KMS::KMSClient>(credential_provider, config);
}

static std::unique_ptr<Aws::KMS::KMSClient> g_kms_client;
static StatusOr<std::string> decrypt_kms_data_key(const std::string& keyId, const std::string& encryptedKeyBase64) {
    std::string encryptedKey;
    if (!base64_decode(encryptedKeyBase64, &encryptedKey)) {
        return Status::InternalError(fmt::format("base64 decode encryptedKey({}) failed", encryptedKeyBase64));
    }
    std::lock_guard lg(g_get_key_spec_from_vault_cache_lock);
    if (g_kms_client == nullptr) {
        ASSIGN_OR_RETURN(g_kms_client, create_kms_client());
    }
    Aws::KMS::Model::DecryptRequest request;
    request.SetKeyId(keyId);
    request.SetCiphertextBlob(Aws::Utils::ByteBuffer((unsigned char*)encryptedKey.data(), encryptedKey.size()));
    auto outcome = g_kms_client->Decrypt(request);
    if (!outcome.IsSuccess()) {
        auto msg = fmt::format("decrypt KmsDataKey failed, keyId:{} error:{}", keyId, outcome.GetError().GetMessage());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    auto plaintext = outcome.GetResult().GetPlaintext();
    return std::string((const char*)plaintext.GetUnderlyingData(), plaintext.GetLength());
}

class KmsDataKey : public NormalKey {
public:
    KmsDataKey(EncryptionKeyPB pb) : NormalKey(std::move(pb)) {}
};

class KmsKey : public EncryptionKey {
public:
    KmsKey(EncryptionKeyPB pb) : EncryptionKey(std::move(pb)) {}

    StatusOr<std::unique_ptr<EncryptionKey>> generate_key() const override {
        return Status::NotSupported("generate_key not implemented for KmsKey");
    }

    Status decrypt(EncryptionKey* child_key) const override {
        if (child_key->type() != KMS_DATA_KEY) {
            return Status::InternalError(fmt::format("decrypt failed, type not support:{}", child_key->type()));
        }
        auto key = down_cast<KmsDataKey*>(child_key);
        if (key->pb().key_desc().empty()) {
            return Status::InternalError("no encrypted_key to decrypt");
        }
        ASSIGN_OR_RETURN(auto plain_key, decrypt_kms_data_key(_pb.key_desc(), key->pb().key_desc()));
        key->set_plain_key(plain_key);
        return Status::OK();
    }

    StatusOr<std::string> get_plain_key() const override { return Status::NotSupported("KmsKey has no plain key"); }

    bool is_decrypted() const override { return true; }

    std::string to_string() const override {
        return fmt::format("id:{} parent:{} type:{}", get_id(), get_parent_id(), type());
    }
};

StatusOr<std::unique_ptr<EncryptionKey>> EncryptionKey::create_from_pb(EncryptionKeyPB pb) {
    switch (pb.type()) {
    case EncryptionKeyTypePB::NORMAL_KEY:
        return std::make_unique<NormalKey>(std::move(pb));
    case EncryptionKeyTypePB::VAULT_PLAIN_KEY: {
        auto key = std::make_unique<VaultPlainKey>(std::move(pb));
        RETURN_IF_ERROR_WITH_WARN(key->init_plain_key_from_vault(), "create VaultPlainKey failed");
        return std::move(key);
    }
    case EncryptionKeyTypePB::KMS_KEY:
        return std::make_unique<KmsKey>(std::move(pb));
    case EncryptionKeyTypePB::KMS_DATA_KEY:
        return std::make_unique<KmsDataKey>(std::move(pb));
    }
    return Status::NotSupported(fmt::format("key type not supported:{}", pb.type()));
}

KeyCache& KeyCache::instance() {
    static KeyCache g_key_cache;
    return g_key_cache;
}

KeyCache::KeyCache() {
    GlobalMetricsRegistry::instance()->metrics()->register_metric("encryption_keys_created", &encryption_keys_created);
    GlobalMetricsRegistry::instance()->metrics()->register_metric("encryption_keys_unwrapped",
                                                                  &encryption_keys_unwrapped);
    GlobalMetricsRegistry::instance()->metrics()->register_metric("encryption_keys_in_cache",
                                                                  &encryption_keys_in_cache);
}

Status KeyCache::_resolve_encryption_meta(const EncryptionMetaPB& metaPb, std::vector<const EncryptionKey*>& keys,
                                          std::vector<std::unique_ptr<EncryptionKey>>& owned_keys,
                                          bool cache_last_key) {
    int nkey = metaPb.key_hierarchy_size();
    int i = nkey - 1;
    for (; i >= 0; i--) {
        bool use_cache = (i != nkey - 1) || cache_last_key;
        if (use_cache) {
            auto& keyPb = metaPb.key_hierarchy(i);
            auto key = get_key(get_identifier_from_pb(keyPb));
            if (key) {
                keys[i] = key;
                break;
            }
        }
        ASSIGN_OR_RETURN(owned_keys[i], EncryptionKey::create_from_pb(metaPb.key_hierarchy(i)));
        keys[i] = owned_keys[i].get();
        if (use_cache && keys[i]->is_decrypted() && keys[i]->has_id()) {
            add_key(owned_keys[i]);
            break;
        }
    }
    RETURN_IF_UNLIKELY(i < 0, Status::InvalidArgument("encryption meta not decryptable"));
    for (; i < nkey - 1; i++) {
        RETURN_IF_UNLIKELY(!owned_keys[i + 1], Status::InvalidArgument(fmt::format("no key created at idx {}", i + 1)));
        RETURN_IF_ERROR_WITH_WARN(keys[i]->decrypt(owned_keys[i + 1].get()),
                                  fmt::format("decrypt key {} using key {}", i + 1, i));
        DCHECK(keys[i + 1]->is_decrypted()) << "key should be decrypted";
        if (keys[i + 1]->has_id()) {
            add_key(owned_keys[i + 1]);
        }
    }
    return Status::OK();
}

StatusOr<FileEncryptionPair> KeyCache::create_encryption_meta_pair(const std::string& encryption_meta_prefix) {
    EncryptionMetaPB meta_pb;
    if (!meta_pb.ParseFromArray(encryption_meta_prefix.data(), encryption_meta_prefix.size()))
        return Status::InternalError("deserialize EncryptionMetaPB failed");
    int nkey = meta_pb.key_hierarchy_size();
    RETURN_IF_UNLIKELY(nkey == 0, Status::Corruption("no key in encryption_meta"););
    std::vector<const EncryptionKey*> keys(nkey);
    std::vector<std::unique_ptr<EncryptionKey>> owned_keys(nkey);
    RETURN_IF_ERROR(_resolve_encryption_meta(meta_pb, keys, owned_keys, true));
    auto parent_key = keys[nkey - 1];
    FileEncryptionPair ret;
    ASSIGN_OR_RETURN(auto key, parent_key->generate_key());
    // append generated key to hierarchy
    *meta_pb.add_key_hierarchy() = key->pb();
    RETURN_IF_UNLIKELY(!meta_pb.SerializeToString(&ret.encryption_meta),
                       Status::InternalError("serialize EncryptionMetaPB failed"));
    ret.info.algorithm = key->algorithm();
    ASSIGN_OR_RETURN(ret.info.key, key->get_plain_key());
    encryption_keys_created.increment(1);
    return ret;
}

StatusOr<FileEncryptionPair> KeyCache::create_encryption_meta_pair_using_current_kek() {
    auto root = _default_root.load();
    RETURN_IF_UNLIKELY(!root, Status::InternalError("root key not available in cache"));
    auto kek = _current_kek.load();
    RETURN_IF_UNLIKELY(!kek, Status::InternalError("kek not available in cache"));
    EncryptionMetaPB meta_pb;
    *meta_pb.add_key_hierarchy() = root->pb();
    *meta_pb.add_key_hierarchy() = kek->pb();
    FileEncryptionPair ret;
    ASSIGN_OR_RETURN(auto key, kek->generate_key());
    // append generated key to hierarchy
    *meta_pb.add_key_hierarchy() = key->pb();
    RETURN_IF_UNLIKELY(!meta_pb.SerializeToString(&ret.encryption_meta),
                       Status::InternalError("serialize EncryptionMetaPB failed"));
    ret.info.algorithm = key->algorithm();
    ASSIGN_OR_RETURN(ret.info.key, key->get_plain_key());
    encryption_keys_created.increment(1);
    return ret;
}

StatusOr<FileEncryptionPair> KeyCache::create_plain_random_encryption_meta_pair() {
    EncryptionKeyPB pb;
    pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
    pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
    std::string pkey(16, '\0');
    ssl_random_bytes(pkey.data(), 16);
    pb.set_plain_key(pkey);
    EncryptionMetaPB meta_pb;
    *meta_pb.add_key_hierarchy() = pb;
    FileEncryptionPair ret;
    RETURN_IF_UNLIKELY(!meta_pb.SerializeToString(&ret.encryption_meta),
                       Status::InternalError("serialize EncryptionMetaPB failed"));
    ret.info.algorithm = pb.algorithm();
    ret.info.key = pkey;
    encryption_keys_created.increment(1);
    return ret;
}

StatusOr<FileEncryptionInfo> KeyCache::unwrap_encryption_meta(const std::string& encryption_meta) {
    EncryptionMetaPB meta_pb;
    RETURN_IF_UNLIKELY(!meta_pb.ParseFromArray(encryption_meta.data(), encryption_meta.size()),
                       Status::InternalError("deserialize EncryptionMetaPB failed"));
    int nkey = meta_pb.key_hierarchy_size();
    RETURN_IF_UNLIKELY(nkey == 0, Status::Corruption("no key in encryption_meta"););
    const auto& keyPB = meta_pb.key_hierarchy(0);
    if (nkey == 1) {
        // no hierarchy, then must have plain key in meta, currently happens only in test case
        RETURN_IF_UNLIKELY(keyPB.plain_key().empty(), Status::InvalidArgument("no plain key in meta"));
        return FileEncryptionInfo(keyPB.algorithm(), keyPB.plain_key());
    }
    std::vector<const EncryptionKey*> keys(nkey);
    std::vector<std::unique_ptr<EncryptionKey>> owned_keys(nkey);
    RETURN_IF_ERROR(_resolve_encryption_meta(meta_pb, keys, owned_keys, false));
    auto key = keys[keys.size() - 1];
    FileEncryptionInfo ret;
    ret.algorithm = key->algorithm();
    ASSIGN_OR_RETURN(ret.key, key->get_plain_key());
    encryption_keys_unwrapped.increment(1);
    return ret;
}

Status KeyCache::refresh_keys_from_fe() {
    if (_last_refresh_key_time + 1800 >= UnixSeconds()) {
        return Status::OK();
    }
    TNetworkAddress master_addr = get_master_address();
    TGetKeysRequest req;
    TGetKeysResponse resp;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&req, &resp](FrontendServiceConnection& client) { client->getKeys(resp, req); }));
    _last_refresh_key_time = UnixSeconds();
    RETURN_IF_ERROR_WITH_WARN(refresh_keys(resp.key_metas), "refresh keys from FE failed");
    return Status::OK();
}

Status KeyCache::refresh_keys(const std::vector<std::string>& key_metas) {
    for (auto& key_meta : key_metas) {
        RETURN_IF_ERROR(refresh_keys(key_meta));
    }
    return Status::OK();
}

Status KeyCache::refresh_keys(const std::string& key_meta) {
    size_t size_before = size();
    EncryptionMetaPB meta_pb;
    RETURN_IF_UNLIKELY(!meta_pb.ParseFromArray(key_meta.data(), key_meta.size()),
                       Status::InternalError("deserialize EncryptionMetaPB failed"));
    int nkey = meta_pb.key_hierarchy_size();
    RETURN_IF_UNLIKELY(nkey == 0, Status::Corruption("no key in encryption_meta"););
    std::vector<const EncryptionKey*> keys(nkey);
    std::vector<std::unique_ptr<EncryptionKey>> owned_keys(nkey);
    RETURN_IF_ERROR(_resolve_encryption_meta(meta_pb, keys, owned_keys, true));
    if (size_before != size()) {
        LOG(INFO) << "refresh keys, num keys before: " << size_before << " after:" << size();
    }
    return Status::OK();
}

size_t KeyCache::size() const {
    std::lock_guard lg(_lock);
    return _identifier_to_keys.size();
}

const EncryptionKey* KeyCache::get_key(const std::string& identifier) {
    std::lock_guard lg(_lock);
    auto k = _identifier_to_keys.find(identifier);
    if (k == _identifier_to_keys.end()) {
        return nullptr;
    } else {
        return k->second.get();
    }
}

void KeyCache::add_key(std::unique_ptr<EncryptionKey>& key) {
    std::lock_guard lg(_lock);
    auto itr = _identifier_to_keys.find(key->identifier());
    if (itr == _identifier_to_keys.end()) {
        // save to tmp var, so it's still available after move
        EncryptionKey* tmp_key = key.get();
        auto id = key->identifier();
        _identifier_to_keys.insert({std::move(id), std::move(key)});
        encryption_keys_in_cache.set_value(_identifier_to_keys.size());
        if (tmp_key->is_kek()) {
            auto current_kek = _current_kek.load();
            if (current_kek == nullptr || current_kek->get_id() < tmp_key->get_id()) {
                LOG(INFO) << "KEK rotated from " << (current_kek ? current_kek->get_id() : 0) << " to "
                          << tmp_key->get_id();
                _current_kek.store(tmp_key);
            }
        } else if (tmp_key->get_id() == EncryptionKey::DEFAULT_MASTER_KYE_ID) {
            LOG(INFO) << "setup root key";
            _default_root.store(tmp_key);
        }
    }
}

std::string KeyCache::to_string() const {
    std::stringstream ss;
    std::lock_guard lg(_lock);
    std::vector<EncryptionKey*> ordered;
    ordered.reserve(_identifier_to_keys.size());
    for (const auto& e : _identifier_to_keys) {
        ordered.push_back(e.second.get());
    }
    std::sort(ordered.begin(), ordered.end(),
              [](EncryptionKey* lhs, EncryptionKey* rhs) { return lhs->get_id() < rhs->get_id(); });
    ss << "[" << std::endl;
    for (const auto& e : ordered) {
        ss << "  " << e->to_string() << std::endl;
    }
    ss << "]" << std::endl;
    return ss.str();
}

} // namespace starrocks
