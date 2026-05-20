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

package com.starrocks.http;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.asn1.x500.X500Name;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.asn1.x509.BasicConstraints;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.asn1.x509.Extension;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.asn1.x509.GeneralName;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.asn1.x509.GeneralNames;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.cert.X509CertificateHolder;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.operator.ContentSigner;
import org.apache.arrow.driver.jdbc.shaded.org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;
import javax.net.ssl.SSLEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class NettyBoringSslCiphersTest {

    private static final List<String> TLS13_ALL = List.of(
            "TLS_AES_128_GCM_SHA256",
            "TLS_AES_256_GCM_SHA384",
            "TLS_CHACHA20_POLY1305_SHA256"
    );

    static File cert;
    static File key;

    @BeforeAll
    static void gen() throws Exception {
        var pem = PemGenerator.generate();
        cert = pem.cert();
        key = pem.key();
    }

    @AfterAll
    static void cleanup() {
        if (key != null) {
            key.delete();
        }
        if (cert != null) {
            cert.delete();
        }
        if (cert != null && cert.getParentFile() != null) {
            cert.getParentFile().delete();
        }
    }

    private static boolean hasAnyTls12(List<String> suites) {
        return suites.stream().anyMatch(s -> s.contains("_WITH_"));
    }

    @Test
    void boringSslIgnoresPrunedTls13ListAndStillEnablesAllThree() throws Exception {
        assumeTrue(OpenSsl.isAvailable(),
                () -> "OpenSSL not available: " + OpenSsl.unavailabilityCause());

        SslContext ctx = SslContextBuilder.forServer(cert, key)
                .ciphers(List.of("TLS_AES_128_GCM_SHA256"))
                .build();

        SSLEngine eng = ctx.newEngine(ByteBufAllocator.DEFAULT);
        List<String> enabled = Arrays.asList(eng.getEnabledCipherSuites());

        assertTrue(enabled.contains("TLS_AES_128_GCM_SHA256"));
        assertTrue(enabled.contains("TLS_AES_256_GCM_SHA384"));
        assertTrue(enabled.contains("TLS_CHACHA20_POLY1305_SHA256"));
    }

    @Test
    void boringSslNoTls13RequestedTls13Disabled() throws Exception {
        assumeTrue(OpenSsl.isAvailable(), () -> "OpenSSL not available: " + OpenSsl.unavailabilityCause());

        SslContext ctx = SslContextBuilder.forServer(cert, key)
                .ciphers(List.of("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"))
                .build();

        SSLEngine eng = ctx.newEngine(ByteBufAllocator.DEFAULT);
        List<String> enabled = Arrays.asList(eng.getEnabledCipherSuites());

        assertTrue(enabled.contains("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"));
        assertFalse(enabled.containsAll(TLS13_ALL));
    }

    @Test
    void boringSslOnlyTls13SubsetRequestedAllTls13PlusDefaultTls12() throws Exception {
        assumeTrue(OpenSsl.isAvailable(), () -> "OpenSSL not available: " + OpenSsl.unavailabilityCause());

        SslContext ctx = SslContextBuilder.forServer(cert, key)
                .ciphers(List.of("TLS_AES_256_GCM_SHA384"))
                .build();

        List<String> enabled = Arrays.asList(ctx.newEngine(ByteBufAllocator.DEFAULT).getEnabledCipherSuites());
        assertTrue(enabled.containsAll(TLS13_ALL));
        assertTrue(hasAnyTls12(enabled));
    }

    @Test
    void boringSslOnlyTls13AllRequestedStillAddsDefaultTls12() throws Exception {
        assumeTrue(OpenSsl.isAvailable(), () -> "OpenSSL not available: " + OpenSsl.unavailabilityCause());
        SslContext ctx = SslContextBuilder.forServer(cert, key)
                .ciphers(TLS13_ALL)
                .build();

        List<String> enabled = Arrays.asList(ctx.newEngine(ByteBufAllocator.DEFAULT).getEnabledCipherSuites());
        assertTrue(enabled.containsAll(TLS13_ALL));
        assertTrue(hasAnyTls12(enabled));
    }

    @Test
    void boringSslSubsetTls13PlusExplicitTls12NoExtraTls12Defaults() throws Exception {
        assumeTrue(OpenSsl.isAvailable(), () -> "OpenSSL not available: " + OpenSsl.unavailabilityCause());
        List<String> requestedTls12 = List.of("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        SslContext ctx = SslContextBuilder.forServer(cert, key)
                .ciphers(Stream.concat(Stream.of("TLS_AES_128_GCM_SHA256"), requestedTls12.stream()).toList())
                .build();

        List<String> enabled = Arrays.asList(ctx.newEngine(ByteBufAllocator.DEFAULT).getEnabledCipherSuites());
        assertTrue(enabled.containsAll(TLS13_ALL));
        assertTrue(enabled.containsAll(requestedTls12));
        long tls12Count = enabled.stream().filter(s -> s.contains("_WITH_")).count();
        assertEquals(requestedTls12.size(), tls12Count);
    }

    @Test
    void boringSslEmptyListDefaultsForTls12() throws Exception {
        assumeTrue(OpenSsl.isAvailable(), () -> "OpenSSL not available: " + OpenSsl.unavailabilityCause());
        SslContext ctx = SslContextBuilder.forServer(cert, key)
                .ciphers(Collections.emptyList())
                .build();

        List<String> enabled = Arrays.asList(ctx.newEngine(ByteBufAllocator.DEFAULT).getEnabledCipherSuites());
        assertFalse(enabled.containsAll(TLS13_ALL));
        assertTrue(hasAnyTls12(enabled));
    }

    @Test
    void boringSslOnlyTls12MultipleExactlyRequestedNoTls13() throws Exception {
        assumeTrue(OpenSsl.isAvailable(), () -> "OpenSSL not available: " + OpenSsl.unavailabilityCause());
        List<String> req12 = List.of(
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        );

        SslContext ctx = SslContextBuilder.forServer(cert, key)
                .sslProvider(SslProvider.OPENSSL)
                .ciphers(req12)
                .build();

        List<String> enabled = Arrays.asList(ctx.newEngine(ByteBufAllocator.DEFAULT).getEnabledCipherSuites());
        assertIterableEquals(req12, enabled);
    }

    private static final class PemGenerator {
        public static PemFiles generate() throws Exception {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048, new SecureRandom());
            KeyPair kp = kpg.generateKeyPair();

            X500Name subject = new X500Name("CN=localhost");
            Instant now = Instant.now();
            Date notBefore = Date.from(now.minus(1, ChronoUnit.MINUTES));
            Date notAfter = Date.from(now.plus(2, ChronoUnit.DAYS));

            JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(
                    subject,
                    new BigInteger(64, new SecureRandom()),
                    notBefore, notAfter,
                    subject,
                    kp.getPublic());

            JcaX509ExtensionUtils ext = new JcaX509ExtensionUtils();
            b.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
            b.addExtension(Extension.subjectKeyIdentifier, false, ext.createSubjectKeyIdentifier(kp.getPublic()));
            GeneralNames san = new GeneralNames(new GeneralName(GeneralName.dNSName, "localhost"));
            b.addExtension(Extension.subjectAlternativeName, false, san);

            ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(kp.getPrivate());
            X509CertificateHolder holder = b.build(signer);
            X509Certificate cert = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new java.io.ByteArrayInputStream(holder.getEncoded()));

            File dir = java.nio.file.Files.createTempDirectory("ssl-pem-").toFile();
            File certFile = new File(dir, "cert.pem");
            File keyFile  = new File(dir, "key.pem");

            writePem("CERTIFICATE", cert.getEncoded(), certFile);
            writePem("PRIVATE KEY", kp.getPrivate().getEncoded(), keyFile);
            return new PemFiles(certFile, keyFile, kp, cert);
        }

        private static void writePem(String type, byte[] der, File file) throws Exception {
            String b64 = java.util.Base64.getMimeEncoder(64, "\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                    .encodeToString(der);
            String txt = "-----BEGIN " + type + "-----\n" + b64 + "\n-----END " + type + "-----\n";
            try (java.io.Writer w = new java.io.OutputStreamWriter(new java.io.FileOutputStream(file),
                    java.nio.charset.StandardCharsets.US_ASCII)) {
                w.write(txt);
            }
        }

        record PemFiles(File cert, File key, KeyPair kp, X509Certificate x509) {
        }
    }
}
