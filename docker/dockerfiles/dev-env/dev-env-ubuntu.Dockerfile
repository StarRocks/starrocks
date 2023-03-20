# This docker file build the Starrocks backend developement environment with all the tooling, dependencies libraries and
# maven dependencies pre-installed.
# Please run this command from the git repo root directory to build:
#    # load STARLET_ARTIFACTS_TAG env
#    . thirdparty/starlet-artifacts-version.sh
#    DOCKER_BUILDKIT=1 docker build --rm=true --build-arg starlet_tag=$STARLET_ARTIFACTS_TAG -f docker/dockerfiles/dev-env/dev-env-ubuntu.Dockerfile -t starrocks/dev-env-ubuntu:tag .

# whether prebuild starrocks maven project and cache the maven artifacts
# value: true | false
# default: true
ARG prebuild_maven=true
# whether pre download thirdparty all-in-one tarball before build
# value: true | false
# default: false
ARG predownload_thirdparty=false
ARG thirdparty_url=https://cdn-thirdparty.starrocks.com/starrocks-thirdparty-main-20230317.tar
ARG commit_id
# check thirdparty/starlet-artifacts-version.sh, to get the right tag
ARG starlet_tag=v0.2.6


FROM ubuntu:22.04 as base

# Install common libraries and tools that are needed for dev environment
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
    automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 make cmake gcc g++ default-jdk git patch lld bzip2 \
    wget unzip curl vim tree net-tools openssh-client && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/lib/jvm/default-java
ENV STARROCKS_THIRDPARTY=/var/local/thirdparty
ENV STARROCKS_LINKER=lld

WORKDIR /root

FROM base as builder
ARG prebuild_maven
ARG predownload_thirdparty
ARG thirdparty_url

COPY . ./starrocks
RUN if test "x$predownload_thirdparty" = "xtrue" ; then \
        wget --progress=dot:mega --tries=3 --read-timeout=60 --connect-timeout=15 --no-check-certificate ${thirdparty_url} -O thirdparty.tar ; \
        mkdir -p starrocks/thirdparty/src && tar -xf thirdparty.tar -C starrocks/thirdparty/src ; \
    fi
RUN mkdir -p $STARROCKS_THIRDPARTY/installed && cd starrocks/thirdparty && \
     ./build-thirdparty.sh && cp -r installed $STARROCKS_THIRDPARTY/
RUN if test "x$prebuild_maven" = "xtrue" ; then \
        export MAVEN_OPTS='-Dmaven.artifact.threads=128' ; cd /root/starrocks ; ./build.sh --fe || true ; \
        cd java-extensions ; mvn package -DskipTests || true ;  \
    else \
        mkdir -p /root/.m2  ;   \
    fi

FROM starrocks/starlet-artifacts-ubuntu22:${starlet_tag} as starlet
# empty section


FROM base as dev-env

ARG commit_id
LABEL org.opencontainers.image.source="https://github.com/StarRocks/starrocks"
LABEL com.starrocks.commit=${commit_id:-"UNKNOWN"}

ENV STARLET_INSTALL_DIR=$STARROCKS_THIRDPARTY/installed/starlet

# Copy third-party dependencies
COPY --from=builder $STARROCKS_THIRDPARTY $STARROCKS_THIRDPARTY

# Copy maven dependencies
COPY --from=builder /root/.m2 /root/.m2

# Copy starlet dependencies
COPY --from=starlet /release $STARLET_INSTALL_DIR
