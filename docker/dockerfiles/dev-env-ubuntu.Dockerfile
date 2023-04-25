# This docker file build the Starrocks backend developement environment with all the tooling, dependencies libraries and
# maven dependencies pre-installed.
# Please run this command from the git repo root directory to build:
#    DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/dev-env-ubuntu.Dockerfile -t starrocks/dev-env-ubuntu:tag .

FROM ubuntu:22.04 as base

# Install common libraries and tools that are needed for dev environment
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
    automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 make cmake gcc g++ default-jdk git patch lld bzip2 \
    wget unzip curl vim tree net-tools openssh-client && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/lib/jvm/default-java
ENV STARROCKS_THIRDPARTY=/opt/starrocks/thirdparty
ENV STARROCKS_LINKER=lld

WORKDIR /root

FROM base as builder
COPY . ./starrocks

# build third-party dependencies
RUN mkdir -p $STARROCKS_THIRDPARTY/installed && cd starrocks/thirdparty &&\
     ./build-thirdparty.sh && cp -r installed $STARROCKS_THIRDPARTY/

# build fe to trigger downloading all the maven dependencies
RUN cd starrocks && MAVEN_OPTS='-Dmaven.artifact.threads=128' ./build.sh --fe

# build be to trigger downloading all the maven dependencies
RUN cd starrocks && ./build.sh --be --clean -j `nproc`

FROM base as dev-env

LABEL org.opencontainers.image.source="https://github.com/StarRocks/starrocks"

# Copy third-party dependencies
COPY --from=builder $STARROCKS_THIRDPARTY $STARROCKS_THIRDPARTY

# Copy maven dependencies
COPY --from=builder /root/.m2 /root/.m2
