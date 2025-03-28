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
#
#  Supported linux distribution are: centos7, ubuntu

# whether prebuild starrocks maven project and cache the maven artifacts
# value: true | false
# default: false
ARG prebuild_maven=false
# whether pre download thirdparty all-in-one tarball before build
# value: true | false
# default: false
ARG predownload_thirdparty=false
ARG thirdparty_url=https://cdn-thirdparty.starrocks.com/starrocks-thirdparty-main-20240411.tar
ARG commit_id
# check thirdparty/starlet-artifacts-version.sh, to get the right tag
ARG starlet_tag=v3.4-rc5
# build for which linux distro: centos7|ubuntu
ARG distro=ubuntu
# Token to access artifacts in private github repositories.
ARG GITHUB_TOKEN
# the root directory to build the project
ARG BUILD_ROOT=/build

FROM starrocks/toolchains-${distro}:main-20241206 as base
ENV STARROCKS_THIRDPARTY=/var/local/thirdparty

WORKDIR /

FROM base as builder_stage1
# stage1: build thirdparty
ARG prebuild_maven
ARG predownload_thirdparty
ARG thirdparty_url
ARG GITHUB_TOKEN
ARG BUILD_ROOT

COPY . $BUILD_ROOT
RUN if test "x$predownload_thirdparty" = "xtrue" ; then \
        wget --progress=dot:mega --tries=3 --read-timeout=60 --connect-timeout=15 --no-check-certificate ${thirdparty_url} -O thirdparty.tar ; \
        mkdir -p ${BUILD_ROOT}/thirdparty/src && tar -xf thirdparty.tar -C ${BUILD_ROOT}/thirdparty/src ; \
    fi
RUN mkdir -p $STARROCKS_THIRDPARTY/installed && cd ${BUILD_ROOT}/thirdparty && \
     PARALLEL=`nproc` GH_TOKEN=${GITHUB_TOKEN} ./build-thirdparty.sh && cp -r installed $STARROCKS_THIRDPARTY/
# create empty maven directories
RUN mkdir -p /root/.m2 /root/.mvn


FROM builder_stage1 as build_prebuild_mvn_true
ARG BUILD_ROOT
# set the maven settings and download the dependency jars
RUN cp -a ${BUILD_ROOT}/docker/dockerfiles/dev-env/mvn/* /root/.mvn/ ; \
        export MAVEN_OPTS='-Dmaven.artifact.threads=128' ; cd ${BUILD_ROOT} ; ./build.sh --fe || true ; \
        cd java-extensions ; mvn package -DskipTests || true

FROM builder_stage1 as build_prebuild_mvn_false
# do nothing


FROM build_prebuild_mvn_${prebuild_maven} as build_stage2
# build_stage2: prebuild maven dependencies, could be no-op if prebuild_maven=false


FROM starrocks/starlet-artifacts-ubuntu22:${starlet_tag} as starlet-ubuntu
FROM starrocks/starlet-artifacts-centos7:${starlet_tag} as starlet-centos7
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
# Copy maven dependencies
COPY --from=build_stage2 /root/.m2 /root/.m2/
COPY --from=build_stage2 /root/.mvn /root/.mvn/
# Copy starlet dependencies
COPY --from=starlet /release $STARLET_INSTALL_DIR
