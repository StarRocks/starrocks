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
package com.starrocks.encryption;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.common.Config;
import com.starrocks.proto.EncryptionAlgorithmPB;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.proto.EncryptionKeyTypePB;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * Store plain key in vault, and get plain key from vault server when FE restarts,
 * only persist the key address in meta, not the actual plain key
 */
public class VaultKey extends EncryptionKey {
    private static final Logger LOG = LoggerFactory.getLogger(VaultKey.class);
    private EncryptionAlgorithmPB algorithm;
    private String addr;
    private byte[] plainKey; // can be null means no plain key yet, need to get from vault

    /**
     * request plain key spec from vault server
     * @param url e.g. http://127.0.0.1:8200/v1/secret/data/sr1_master_key?token=hvs.a0aoKt5L9UkCDrIRH81ydOTQ
     * @return plain key spec in `algo:plain-key-base64 format
     */
    public static String getPlainKeySpecFromVault(String url) throws Exception {
        String addr = null;
        String path = null;
        String token = null;
        if (url.startsWith("/")) {
            path = url;
            addr = Config.vault_addr;
            if (addr == null || addr.isEmpty()) {
                addr = System.getenv("VAULT_ADDR");
            }
            if (addr == null || addr.isEmpty()) {
                throw new IllegalArgumentException("addr not found for vault service: " + url);
            }
        } else {
            // 1. parse url to get addr, path and token
            URI uri = URI.create(url);
            addr = uri.getScheme() + "://" + uri.getAuthority();
            path = uri.getPath();
            List<NameValuePair> params = URLEncodedUtils.parse(uri, Charset.forName("UTF-8"));
            for (NameValuePair param : params) {
                if (param.getName().equals("token")) {
                    token = param.getValue();
                    break;
                }
            }
        }
        if (token == null || token.isEmpty()) {
            token = Config.vault_token;
        }
        if (token == null || token.isEmpty()) {
            token = System.getenv("VAULT_TOKEN");
        }
        if (token == null) {
            throw new IllegalArgumentException("token not found for vault service: " + url);
        }
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            String reqUrl = String.format("%s%s", addr, path);
            LOG.info("Get key from vault: {}", reqUrl);
            HttpGet request = new HttpGet(reqUrl);
            request.setHeader("X-Vault-Token", token);

            HttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                StringBuilder result = new StringBuilder();
                String line;
                while ((line = rd.readLine()) != null) {
                    result.append(line);
                }
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(result.toString());
                JsonNode data = root.get("data");
                if (data == null) {
                    throw new IllegalArgumentException("Failed to read secret. No data field in response");
                }
                JsonNode dataData = data.get("data");
                if (dataData == null) {
                    throw new IllegalArgumentException("Failed to read secret. No data.data field in response");
                }
                JsonNode plainKey = dataData.get("plain_key");
                if (plainKey == null) {
                    throw new IllegalArgumentException("Failed to read secret. No data.data.plain_key field in response");
                }
                return plainKey.asText();
            } else {
                throw new IllegalArgumentException("Failed to read secret from vault. HTTP error code: " + statusCode);
            }
        } finally {
            httpClient.close();
        }
    }

    /**
     * create a key by key spec string with predefined format
     *
     * @param spec key address as uri, e.g.
     *               dev:        http://127.0.0.1:8200/v1/secret/data/sr1_master_key?token=hvs.a0aoKt5L9UkCDrIRH81ydOTQ
     *               production: https://vault.server:8200/v1/secret/data/sr_master_key?token=hvs.a0aoKt5L9UkCDrIRH81ydOTQ
     *             if addr may change, some parts can be skipped in url and use env.VAULT_ADDR and env.VAULT_TOKEN
     *             or Config.vault_addr and Config.vault_token instead, e.g.
     *                /v1/secret/data/sr1_master_key
     *                Config.vault_addr or env.VAULT_ADDR: http://127.0.0.1:8200
     *                Config.vault_token or env.VAULT_TOKEN: hvs.a0aoKt5L9UkCDrIRH81ydOTQ
     * @return created key
     */
    public static VaultKey createFromSpec(String spec) {
        return new VaultKey(spec);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VaultKey)) {
            return false;
        }
        VaultKey rhs = (VaultKey) obj;
        if (algorithm != rhs.algorithm) {
            return false;
        }
        return Arrays.equals(plainKey, rhs.plainKey);
    }

    @Override
    public String toSpec() {
        return String.format("vault:%s", addr);
    }

    public VaultKey() {
    }

    public VaultKey(String addr) {
        initFromAddr(addr);
    }

    private void initFromAddr(String addr) {
        String keySpec;
        try {
            keySpec = getPlainKeySpecFromVault(addr);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to get plain key from vault", e);
        }
        String[] parts = keySpec.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid key spec format");
        }

        String algorithmName = parts[0];
        String base64Key = parts[1];

        EncryptionAlgorithmPB algorithm;
        if (algorithmName.equalsIgnoreCase("AES_128")) {
            algorithm = EncryptionAlgorithmPB.AES_128;
        } else {
            throw new IllegalArgumentException("Unsupported algorithm: " + algorithmName);
        }

        byte[] plainKey;
        try {
            plainKey = Base64.getDecoder().decode(base64Key);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Base64 key", e);
        }

        if (plainKey.length != 16) {
            throw new IllegalArgumentException("Invalid key length " + plainKey.length * 8);
        }
        this.addr = addr;
        this.algorithm = algorithm;
        this.plainKey = plainKey;
    }

    public EncryptionAlgorithmPB getAlgorithm() {
        return algorithm;
    }

    public byte[] wrapKey(EncryptionAlgorithmPB algorithm, byte[] plainKey) {
        return EncryptionUtil.wrapKey(this.plainKey, algorithm, plainKey);
    }

    public byte[] unwrapKey(EncryptionAlgorithmPB algorithm, byte[] encryptedKey) {
        return EncryptionUtil.unwrapKey(this.plainKey, algorithm, encryptedKey);
    }

    @Override
    public void toPB(EncryptionKeyPB pb, KeyMgr mgr) {
        super.toPB(pb, mgr);
        pb.type = EncryptionKeyTypePB.VAULT_PLAIN_KEY;
        pb.keyDesc = addr;
    }

    @Override
    public void fromPB(EncryptionKeyPB pb, KeyMgr mgr) {
        super.fromPB(pb, mgr);
        initFromAddr(pb.keyDesc);
    }

    @Override
    public EncryptionKey generateKey() {
        NormalKey ret = NormalKey.createRandom(this.algorithm);
        ret.setEncryptedKey(wrapKey(this.algorithm, ret.getPlainKey()));
        ret.setParent(this);
        return ret;
    }

    @Override
    public void decryptKey(EncryptionKey key) {
        assert plainKey != null;
        if (!(key instanceof NormalKey)) {
            throw new IllegalArgumentException("VaultKey cannot not decrypt " + key.getClass().getName());
        }
        NormalKey normalKey = (NormalKey) key;
        normalKey.setPlainKey(unwrapKey(algorithm, normalKey.getEncryptedKey()));
    }

    @Override
    public String toString() {
        return String.format("VaultKey(id:%d createTime:%d addr:%s)", id, createTime, addr);
    }
}
