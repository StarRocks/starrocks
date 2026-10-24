Enable frame pointer
===================

When introducing thirdparty, make sure compiler option `-fno-omit-frame-pointer` is enabled. By default it's disabled, which makes profiling hard.  https://gcc.gnu.org/onlinedocs/gcc-10.3.0/gcc/Optimize-Options.html. The ovehead of it can be offset by observablity.


BPF Performance Tools.pdf Chapter2

> On x86_64 today, most software is compiled with gcc’s defaults, breaking frame pointer stack traces. Last time I studied the performance gain from frame pointer omission in our production environment, it was usually less than one percent, and it was often so close to zero that it was difficult to measure. Many microservices at Netflix are running with the frame pointer reenabled, as the performance wins found by CPU profiling outweigh the tiny loss of performance. 

## Optional Lance dependency

Run `./thirdparty/build-lance.sh -j 2` before enabling the Lance connector in BE.
This separate step builds and tests the native library; the StarRocks build only
consumes its installed CMake package. Linux also supports
`./thirdparty/build-thirdparty.sh -j2 lance` after protobuf has been installed.
It is excluded from the default local thirdparty build; the Ubuntu/Rocky9
dev-env Dockerfile copies preinstalled Rust/Cargo from a pinned official Rust
image into its `lance-builder` stage. This stage reuses the validated StarRocks
dependency installation, including protoc, and builds only Lance. It copies the
installed Lance package into the dependency image. See [Lance build and installation](lance/README.md)
for prerequisites, custom prefixes, and platform compatibility requirements.

## Reusing dependencies in dev-env images

The dev-env Dockerfile defaults to `thirdparty_source=prebuilt`. Ubuntu and
Rocky9 start with the installed dependencies from immutable, multi-platform
StarRocks dev-env images recorded in `prebuilt-baseline.json`. The source commit
in those image labels is `d78fb065ca1`; its existing dependency inputs match the
base of this Lance change. Only the Lance producer compiles new code when those
inputs still match. The standalone pprof executable is downloaded and installed
separately using the checkout's URL and checksum. The final image refreshes
Starlet from the requested artifact version and includes the newly built Lance
package.

`reuse-prebuilt.py` checks the common thirdparty recipes, versions, patches,
toolchain sources, `env.sh`, toolchain tag and required installed artifacts before
accepting the seed. It excludes Lance's separately built sources and build helper,
documentation and generated build/install directories. Only pprof's literal URL
and checksum assignments are normalized, because `build-thirdparty.sh -j2 pprof`
refreshes that executable on every reuse; its update must succeed before the image
can be published. Changing a library version, patch, common recipe or custom
environment still disables reuse: the Docker build discards the seed and rebuilds
dependencies from source. It never layers new builds over stale libraries.

For a completely source-built image without pulling a dependency seed, use:

```sh
DOCKER_BUILDKIT=1 docker build --build-arg thirdparty_source=full \
  --build-arg distro=ubuntu \
  -f docker/dockerfiles/dev-env/dev-env.Dockerfile -t starrocks-dev-env:full .
```

CentOS7 has no verified seed and always builds its existing dependencies from
source. Lance remains disabled there. Local `build-thirdparty.sh` behavior is
unchanged; reuse applies only to the dev-env image producer.

When refreshing the baseline, inspect the published image's source commit and
verify its dependency recipes and toolchain against the checkout before updating
the image digests and input fingerprint together. Do not just refresh the hash to
accept changed dependencies against an older image. Print the checkout fingerprint
with `python3 thirdparty/reuse-prebuilt.py --root . --print-fingerprint` and run
`python3 -m unittest discover -s thirdparty -p 'test_reuse_prebuilt.py'`.
