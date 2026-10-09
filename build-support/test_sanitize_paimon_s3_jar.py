# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pathlib
import subprocess
import tempfile
import unittest
import zipfile


SCRIPT = pathlib.Path(__file__).with_name("SanitizePaimonS3Jar.java")
SERVICE = "paimon-plugin-s3/META-INF/services/java.net.spi.InetAddressResolverProvider"


class SanitizePaimonS3JarTest(unittest.TestCase):
    def test_removes_only_incompatible_service_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            jar = pathlib.Path(tmp_dir) / "paimon-s3.jar"
            with zipfile.ZipFile(jar, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\nMulti-Release: true\n")
                archive.writestr(SERVICE, "org.xbill.DNS.spi.DnsjavaInetAddressResolverProvider\n")
                archive.writestr("paimon-plugin-s3/example.txt", "preserved")

            for _ in range(2):
                subprocess.run(["java", str(SCRIPT), str(jar)], check=True)

            with zipfile.ZipFile(jar) as archive:
                self.assertNotIn(SERVICE, archive.namelist())
                self.assertEqual("preserved", archive.read("paimon-plugin-s3/example.txt").decode())
                manifest = archive.read("META-INF/MANIFEST.MF").decode()
                self.assertIn("Multi-Release: true", manifest)

    def test_rejects_missing_jar(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_jar = pathlib.Path(tmp_dir) / "missing.jar"
            result = subprocess.run(
                ["java", str(SCRIPT), str(missing_jar)], capture_output=True, text=True
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("Paimon S3 JAR does not exist", result.stderr)


if __name__ == "__main__":
    unittest.main()
