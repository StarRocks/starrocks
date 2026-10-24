# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("reuse_prebuilt", Path(__file__).with_name("reuse-prebuilt.py"))
reuse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reuse)


class ReusePrebuiltTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ("env.sh", "thirdparty/vars.sh", "thirdparty/patches/fix.patch",
                     "docker/dockerfiles/toolchains/toolchains-ubuntu.Dockerfile"):
            self.write(name, "original")
        for arch in ("aarch64", "x86_64"):
            self.write("thirdparty/vars-" + arch + ".sh",
                       'PPROF_DOWNLOAD="https://example.com/old/pprof"\n'
                       'PPROF_MD5SUM="11111111111111111111111111111111"\n'
                       'STARCACHE_MD5SUM="unchanged"\n')
        self.prefix = self.root / "thirdparty/installed"
        for name in ("bin/protoc", "lib/libprotobuf.a", "lib64/libarrow.a"):
            self.write("thirdparty/installed/" + name, "artifact")
        self.baseline = {"images": {"ubuntu": "pinned-image"}, "toolchain_tag": "pinned-toolchain",
                         "inputs_sha256": reuse.input_fingerprint(self.root)}

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def reason(self, **overrides):
        args = dict(root=self.root, prefix=self.prefix, distro="ubuntu", source="prebuilt",
                    toolchain_tag="pinned-toolchain", baseline=self.baseline)
        args.update(overrides)
        return reuse.reuse_reason(**args)

    def test_unchanged_dependencies_can_be_reused_after_lance_edits(self):
        self.assertIsNone(self.reason())
        for name in ("thirdparty/lance/src/lib.rs", "thirdparty/lance/Cargo.lock",
                     "thirdparty/build-lance.sh", "thirdparty/README.md",
                     "thirdparty/src/unpacked/file", "thirdparty/installed/lib/generated"):
            self.write(name, "new")
        self.assertIsNone(self.reason())

    def test_common_recipe_patch_toolchain_addition_and_deletion_invalidate_reuse(self):
        for name in ("env.sh", "thirdparty/vars.sh", "thirdparty/patches/fix.patch",
                     "docker/dockerfiles/toolchains/toolchains-ubuntu.Dockerfile"):
            with self.subTest(name=name):
                self.write(name, "changed")
                self.assertIn("build inputs", self.reason())
                self.write(name, "original")
        self.write("thirdparty/new-build-input", "new")
        self.assertIn("build inputs", self.reason())
        (self.root / "thirdparty/new-build-input").unlink()
        (self.root / "thirdparty/patches/fix.patch").unlink()
        self.assertIn("build inputs", self.reason())

    def test_pprof_version_changes_reuse_libraries_but_other_versions_do_not(self):
        for arch in ("aarch64", "x86_64"):
            name = "thirdparty/vars-" + arch + ".sh"
            original = (self.root / name).read_text()
            self.write(name, original.replace("/old/", "/new/").replace("1" * 32, "2" * 32))
        self.assertIsNone(self.reason())
        self.write("thirdparty/vars-x86_64.sh",
                   (self.root / "thirdparty/vars-x86_64.sh").read_text().replace("unchanged", "new-library"))
        self.assertIn("build inputs", self.reason())

    def test_pprof_normalization_does_not_hide_shell_code_changes(self):
        name = "thirdparty/vars-x86_64.sh"
        original = (self.root / name).read_text()
        for value in ('$(new_command)', '`new_command`', '${NEW_VARIABLE}', 'new"; new_command; #'):
            with self.subTest(value=value):
                self.write(name, original.replace("https://example.com/old/pprof", value))
                self.assertIn("build inputs", self.reason())

    def test_custom_environment_full_build_and_toolchain_change_disable_reuse(self):
        self.assertIn("full source build", self.reason(source="full"))
        self.assertIn("toolchain", self.reason(toolchain_tag="new-toolchain"))
        self.assertIn("no verified", self.reason(distro="centos7"))
        self.write("custom_env.sh", "export CFLAGS=-O0")
        self.assertIn("custom_env.sh", self.reason())

    def test_missing_or_empty_seed_is_not_reused(self):
        artifact = self.prefix / "bin/protoc"
        artifact.write_text("")
        self.assertIn("missing or empty", self.reason())
        artifact.unlink()
        self.assertIn("missing or empty", self.reason())

    def test_executable_mode_changes_invalidate_reuse(self):
        path = self.root / "env.sh"
        path.chmod(path.stat().st_mode ^ 0o100)
        self.assertIn("build inputs", self.reason())

    def test_symlink_directory_changes_invalidate_reuse(self):
        path = self.root / "thirdparty/linked-patches"
        path.symlink_to("patches", target_is_directory=True)
        self.assertIn("build inputs", self.reason())

    def test_docker_reuses_seed_or_rebuilds_without_stale_artifacts(self):
        repo = Path(__file__).resolve().parents[1]
        dockerfile = (repo / "docker/dockerfiles/dev-env/dev-env.Dockerfile").read_text()
        command = next(line[4:] for line in dockerfile.replace("\\\n", "").splitlines()
                       if line.startswith("RUN if python3 "))
        subprocess.run(["/bin/sh", "-n", "-c", command], check=True)
        self.write("thirdparty/reuse-prebuilt.py", (repo / "thirdparty/reuse-prebuilt.py").read_text())
        self.write("thirdparty/build-thirdparty.sh", "#!/bin/sh\nset -eu\n"
                   "if [ \"$*\" = '-j2 pprof' ]; then\n"
                   "  touch installed/refresh-called\n"
                   "  [ \"${FAIL_REFRESH:-0}\" = 0 ] || exit 38\n"
                   "  mkdir -p installed/flamegraph\n"
                   "  echo refreshed > installed/flamegraph/pprof\n"
                   "  exit 0\n"
                   "fi\n"
                   "touch build-called\n[ \"${FAIL_BUILD:-0}\" = 0 ] || exit 37\n"
                   "test ! -e installed/stale\nmkdir -p installed/lib\n"
                   "echo rebuilt > installed/lib/result\n")
        (self.root / "thirdparty/build-thirdparty.sh").chmod(0o755)
        self.baseline.update(source_commit="verified", inputs_sha256=reuse.input_fingerprint(self.root))
        self.write("thirdparty/prebuilt-baseline.json", json.dumps(self.baseline))
        # Keep the seeded prefix distinct from the checkout's install directory.
        seed = self.root / "seed"
        self.prefix.rename(seed)
        self.prefix = seed
        env = dict(os.environ, BUILD_ROOT=str(self.root), STARROCKS_THIRDPARTY=str(self.root / "published"),
                   distro="ubuntu", toolchain_tag="pinned-toolchain", thirdparty_source="prebuilt",
                   predownload_thirdparty="false", GITHUB_TOKEN="")
        published = self.root / "published/installed"
        published.parent.mkdir()
        seed.rename(published)
        self.assertEqual(0, subprocess.run(["/bin/sh", "-c", command], env=env).returncode)
        self.assertFalse((self.root / "thirdparty/build-called").exists())
        self.assertEqual("artifact", (published / "lib64/libarrow.a").read_text())
        self.assertEqual("refreshed\n", (published / "flamegraph/pprof").read_text())
        self.assertTrue((published / "refresh-called").exists())

        result = subprocess.run(["/bin/sh", "-c", command], env=dict(env, FAIL_REFRESH="1"))
        self.assertEqual(38, result.returncode)
        self.assertFalse((self.root / "thirdparty/build-called").exists())

        # A changed common input must force a fresh build and remove the seed.
        self.write("env.sh", "changed")
        (published / "stale").touch()
        self.write("thirdparty/installed/stale", "stale local output")
        self.assertEqual(0, subprocess.run(["/bin/sh", "-c", command], env=env).returncode)
        self.assertFalse((published / "stale").exists())
        self.assertFalse((published / "lib64/libarrow.a").exists())
        self.assertEqual("rebuilt\n", (published / "lib/result").read_text())

        # Rebuild failures must fail the image instead of publishing the old seed.
        result = subprocess.run(["/bin/sh", "-c", command], env=dict(env, FAIL_BUILD="1"))
        self.assertEqual(37, result.returncode)
        self.assertFalse((published / "lib/result").exists())

    def test_docker_uses_the_recorded_immutable_images(self):
        root = Path(__file__).resolve().parents[1]
        baseline = json.loads((root / "thirdparty/prebuilt-baseline.json").read_text())
        dockerfile = (root / "docker/dockerfiles/dev-env/dev-env.Dockerfile").read_text()
        for distro, image in baseline["images"].items():
            self.assertIn("@sha256:", image)
            self.assertIn("FROM " + image + " as prebuilt-" + distro, dockerfile)

    def test_lance_image_reuses_protoc_for_seed_and_full_build(self):
        repo = Path(__file__).resolve().parents[1]
        dockerfile = (repo / "docker/dockerfiles/dev-env/dev-env.Dockerfile").read_text()
        command = next(line[4:] for line in dockerfile.replace("\\\n", "").splitlines()
                       if line.startswith("RUN mkdir -p $STARROCKS_THIRDPARTY/installed/lance"))
        for source, distro, fail in (("prebuilt", "ubuntu", False), ("prebuilt", "rocky9", False),
                                    ("full", "ubuntu", False), ("full", "rocky9", False),
                                    ("prebuilt", "ubuntu", True), ("full", "centos7", True)):
            with self.subTest(source=source, distro=distro, fail=fail), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                checkout = root / "checkout/thirdparty"
                prefix = root / "published/installed"
                checkout.mkdir(parents=True)
                (prefix / "bin").mkdir(parents=True)
                protoc = prefix / "bin/protoc"
                protoc.write_text("#!/bin/sh\necho reused-protoc\n")
                protoc.chmod(0o755)
                if source == "full":
                    (checkout / "installed/bin").mkdir(parents=True)
                    local_protoc = checkout / "installed/bin/protoc"
                    local_protoc.write_bytes(protoc.read_bytes())
                    local_protoc.chmod(0o755)
                build = checkout / "build-thirdparty.sh"
                build.write_text('#!/bin/sh\nset -eu\n'
                                 'test "$*" = "-j2 lance"\n'
                                 'test "$(installed/bin/protoc --version)" = reused-protoc\n'
                                 'touch build-called\n'
                                 '[ "$FAIL_BUILD" = 0 ] || exit 37\n'
                                 'mkdir -p installed/lance/lib\n'
                                 'echo new-library > installed/lance/lib/libstarrocks_lance.so\n')
                build.chmod(0o755)
                env = dict(os.environ, BUILD_ROOT=str(root / "checkout"), distro=distro,
                           STARROCKS_THIRDPARTY=str(prefix.parent), FAIL_BUILD=str(int(fail)))
                result = subprocess.run(["/bin/sh", "-c", command], env=env)
                expected = 37 if fail and distro != "centos7" else 0
                self.assertEqual(expected, result.returncode)
                self.assertEqual(distro != "centos7", (checkout / "build-called").exists())
                library = prefix / "lance/lib/libstarrocks_lance.so"
                if not fail:
                    self.assertEqual("new-library\n", library.read_text())
                else:
                    self.assertFalse(library.exists())


if __name__ == "__main__":
    unittest.main()
