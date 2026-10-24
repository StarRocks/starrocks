# This docker file build the StarRocks development environment with all the tooling, dependencies libraries and
# maven dependencies pre-installed.
# Please run this command from the git repo root directory to build:
#    # load STARLET_ARTIFACTS_TAG env
#    . thirdparty/starlet-artifacts-version.sh
#    DOCKER_BUILDKIT=1 docker build --rm=true --build-arg starlet_tag=$STARLET_ARTIFACTS_TAG -f docker/dockerfiles/dev-env/dev-env.Dockerfile -t starrocks/dev-env-ubuntu:<tag> .
#
#  `distro` argument can be specified to build for a different linux distribution other than default `ubuntu`.
#  For example, build centos7 dev-env with following commands
#    DOCKER_BUILDKIT=1 docker build --rm=true --build-arg distro=centos7 --build-arg starlet_tag=$STARLET_ARTIFACTS_TAG -f docker/dockerfiles/dev-env/dev-env.Dockerfile -t dev-env-centos7:latest .
#  Or build rocky9 dev-env with following commands
#    DOCKER_BUILDKIT=1 docker build --rm=true --build-arg distro=rocky9 --build-arg starlet_tag=$STARLET_ARTIFACTS_TAG -f docker/dockerfiles/dev-env/dev-env.Dockerfile -t dev-env-rocky9:latest .
#
#  Supported linux distribution are: centos7, ubuntu, rocky9

# whether prebuild starrocks maven project and cache the maven artifacts
# value: true | false
# default: false
ARG prebuild_maven=false
# whether pre download thirdparty all-in-one tarball before build
# value: true | false
# default: false
ARG predownload_thirdparty=false
ARG thirdparty_url=https://cdn-thirdparty.starrocks.com/starrocks-thirdparty-main-20260526.tar
ARG commit_id
# check thirdparty/starlet-artifacts-version.sh, to get the right tag
ARG starlet_tag=v4.2-rc6
# build for which linux distro: centos7|ubuntu|rocky9
ARG distro=ubuntu
# Token to access artifacts in private github repositories.
ARG GITHUB_TOKEN
# the root directory to build the project
ARG BUILD_ROOT=/build
# Use a verified, immutable dependency seed by default. Set to full for an
# entirely source-built image, without pulling any prebuilt dev-env image.
ARG thirdparty_source=prebuilt
ARG toolchain_tag=main-20260707

FROM starrocks/toolchains-${distro}:${toolchain_tag} as base
ENV STARROCKS_THIRDPARTY=/var/local/thirdparty

WORKDIR /

# These multi-platform images were published from d78fb065ca1. Reuse is
# conditional on the build-input fingerprint in thirdparty/prebuilt-baseline.json.
FROM starrocks/dev-env-ubuntu@sha256:5b918a26c21f46038f82c17db7d9879a9a8a7bbecb6aee920da86313c66df3ee as prebuilt-ubuntu
FROM starrocks/dev-env-rocky9@sha256:c05f0e7be2fcfeadbd70137ff03781675925add2931af68fa806f77d24a57645 as prebuilt-rocky9
# There is no verified seed for the legacy platform; retain its full build.
FROM base as prebuilt-centos7
RUN mkdir -p $STARROCKS_THIRDPARTY/installed
FROM prebuilt-${distro} as thirdparty-source-prebuilt

FROM base as thirdparty-source-full
RUN mkdir -p $STARROCKS_THIRDPARTY/installed
FROM thirdparty-source-${thirdparty_source} as thirdparty-seed

FROM base as builder_stage1
# stage1: build thirdparty
ARG prebuild_maven
ARG predownload_thirdparty
ARG thirdparty_url
ARG GITHUB_TOKEN
ARG BUILD_ROOT
ARG distro
ARG toolchain_tag
ARG thirdparty_source

COPY --from=thirdparty-seed /var/local/thirdparty/installed $STARROCKS_THIRDPARTY/installed
COPY . $BUILD_ROOT
RUN if python3 ${BUILD_ROOT}/thirdparty/reuse-prebuilt.py \
        --root ${BUILD_ROOT} --prefix $STARROCKS_THIRDPARTY/installed \
        --distro "$distro" --toolchain-tag "$toolchain_tag" --source "$thirdparty_source" ; then \
        echo "Reusing verified prebuilt thirdparty libraries; refreshing pprof separately." && \
        cd ${BUILD_ROOT}/thirdparty && \
        if [ ! -d installed ]; then \
            ln -s $STARROCKS_THIRDPARTY/installed installed ; \
        fi && \
        ./build-thirdparty.sh -j2 pprof ; \
    else \
        echo "Building thirdparty dependencies from source." && \
        rm -rf $STARROCKS_THIRDPARTY/installed ${BUILD_ROOT}/thirdparty/installed && \
        mkdir -p $STARROCKS_THIRDPARTY/installed && \
        if test "x$predownload_thirdparty" = "xtrue" ; then \
            wget --progress=dot:mega --tries=3 --read-timeout=60 --connect-timeout=15 --no-check-certificate ${thirdparty_url} -O thirdparty.tar && \
            mkdir -p ${BUILD_ROOT}/thirdparty/src && tar -xf thirdparty.tar -C ${BUILD_ROOT}/thirdparty/src ; \
        fi && \
        cd ${BUILD_ROOT}/thirdparty && \
        PARALLEL=`nproc` GH_TOKEN=${GITHUB_TOKEN} ./build-thirdparty.sh && \
        cp -r installed $STARROCKS_THIRDPARTY/ ; \
    fi && \
    rm -rf $STARROCKS_THIRDPARTY/installed/lance $STARROCKS_THIRDPARTY/installed/starlet
# create empty maven directories
RUN mkdir -p /root/.m2 /root/.mvn


# Reuse the official preinstalled Rust toolchain instead of downloading rustup
# during every image build. This digest pins Rust 1.96.1 for amd64 and arm64.
FROM rust:1.96.1-slim-bookworm@sha256:e18a79fc84dfcfc3ab5ba72290398a644c135c97eaa881447fddc354ee4701a3 as rust-toolchain

# Reuse the validated dependency installation, including protoc and its includes.
# Do not download or rebuild zlib/protobuf/gtest just to compile Lance.
FROM builder_stage1 as lance-builder
ARG BUILD_ROOT
ARG distro

# Copy only the official Rust distribution, not Debian's system libraries.
# Compile in the target StarRocks toolchain environment to retain its ABI.
# The final dev-env image receives only the installed Lance package.
ENV RUSTUP_HOME=/opt/rustup
ENV CARGO_HOME=/opt/cargo
ENV PATH=/opt/cargo/bin:$PATH
COPY --from=rust-toolchain /usr/local/rustup/ /opt/rustup/
COPY --from=rust-toolchain /usr/local/cargo/ /opt/cargo/
# The official toolchain requires glibc >= 2.28; CentOS7 keeps skipping Lance.
RUN if [ "$distro" != "centos7" ]; then rustc --version && cargo --version; fi

RUN mkdir -p $STARROCKS_THIRDPARTY/installed/lance && \
    if [ "$distro" != "centos7" ]; then \
        cd ${BUILD_ROOT}/thirdparty && \
        if [ ! -d installed ]; then \
            ln -s $STARROCKS_THIRDPARTY/installed installed ; \
        fi && \
        ./build-thirdparty.sh -j2 lance && \
        if [ ! installed/lance -ef $STARROCKS_THIRDPARTY/installed/lance ]; then \
            cp -r installed/lance $STARROCKS_THIRDPARTY/installed/ ; \
        fi ; \
    fi


FROM builder_stage1 as build_prebuild_mvn_true
ARG BUILD_ROOT
# set the maven settings and download the dependency jars
RUN if [ ! -d ${BUILD_ROOT}/thirdparty/installed ]; then \
        ln -s $STARROCKS_THIRDPARTY/installed ${BUILD_ROOT}/thirdparty/installed ; \
    fi
RUN cp -a ${BUILD_ROOT}/docker/dockerfiles/dev-env/mvn/* /root/.mvn/ ; \
        export MAVEN_OPTS='-Dmaven.artifact.threads=128' ; cd ${BUILD_ROOT} ; ./build.sh --fe || true ; \
        cd java-extensions ; mvn package -DskipTests || true

FROM builder_stage1 as build_prebuild_mvn_false
# do nothing


FROM build_prebuild_mvn_${prebuild_maven} as build_stage2
# build_stage2: prebuild maven dependencies, could be no-op if prebuild_maven=false


FROM starrocks/starlet-artifacts-ubuntu22:${starlet_tag} as starlet-ubuntu
FROM starrocks/starlet-artifacts-centos7:${starlet_tag} as starlet-centos7
# rocky9 (glibc 2.34) reuses the centos7 artifacts (glibc 2.17, forward-compatible);
# the ubuntu22 artifacts (glibc 2.35) are too new to run on rocky9.
FROM starrocks/starlet-artifacts-centos7:${starlet_tag} as starlet-rocky9
# determine which artifacts to use
FROM starlet-${distro} as starlet
ARG BUILD_ROOT
# remove unnecessary and big starlet dependencies
COPY --from=builder_stage1 ${BUILD_ROOT}/docker/dockerfiles/dev-env/starlet_exclude.txt .
RUN while read line; do \
        if [[ "$line" == \#* ]] ; then \
            continue ; \
        fi ; \
        rm -rvf /release/$line ; \
    done < starlet_exclude.txt


FROM base as dev-env
# Final stage: collect all artifacts
ARG commit_id
LABEL org.opencontainers.image.source="https://github.com/StarRocks/starrocks"
LABEL com.starrocks.commit=${commit_id:-"UNKNOWN"}
ENV STARLET_INSTALL_DIR=$STARROCKS_THIRDPARTY/installed/starlet
ENV PATH=$STARROCKS_GCC_HOME/bin:$PATH

# Copy third-party dependencies
COPY --from=build_stage2 $STARROCKS_THIRDPARTY $STARROCKS_THIRDPARTY
COPY --from=lance-builder $STARROCKS_THIRDPARTY/installed/lance $STARROCKS_THIRDPARTY/installed/lance
# Copy maven dependencies
COPY --from=build_stage2 /root/.m2 /root/.m2/
COPY --from=build_stage2 /root/.mvn /root/.mvn/
# Copy starlet dependencies
COPY --from=starlet /release $STARLET_INSTALL_DIR
