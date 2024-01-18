# This docker file build the StarRocks development environment with all the tooling, dependencies libraries and
# maven dependencies pre-installed.
# Please run this command from the git repo root directory to build:
#    DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/dev-env/dev-env.Dockerfile -t starrocks/dev-env-ubuntu:<tag> .
#
#  `distro` argument can be specified to build for a different linux distribution other than default `ubuntu`.
#  For example, build centos7 dev-env with following commands
#    DOCKER_BUILDKIT=1 docker build --rm=true --build-arg distro=centos7 -f docker/dockerfiles/dev-env/dev-env.Dockerfile -t starrocks/dev-env-centos7:<tag> .
#
#  Supported linux distribution are: centos7, ubuntu

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
# build for which linux distro: centos7|ubuntu
ARG distro=ubuntu

<<<<<<< HEAD
FROM starrocks/toolchains-${distro}:20230324 as base
=======
FROM starrocks/toolchains-${distro}:main-20230517 as base
>>>>>>> 2.5.18
ENV STARROCKS_THIRDPARTY=/var/local/thirdparty

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
     PARALLEL=`nproc` ./build-thirdparty.sh && cp -r installed $STARROCKS_THIRDPARTY/
RUN if test "x$prebuild_maven" = "xtrue" ; then \
        export MAVEN_OPTS='-Dmaven.artifact.threads=128' ; cd /root/starrocks ; ./build.sh --fe || true ; \
        cd java-extensions ; mvn package -DskipTests || true ;  \
    else \
        mkdir -p /root/.m2  ;   \
    fi


FROM base as dev-env
ARG commit_id
LABEL org.opencontainers.image.source="https://github.com/StarRocks/starrocks"
LABEL com.starrocks.commit=${commit_id:-"UNKNOWN"}
ENV PATH=$STARROCKS_GCC_HOME/bin:$PATH

# Copy third-party dependencies
COPY --from=builder $STARROCKS_THIRDPARTY $STARROCKS_THIRDPARTY
# Copy maven dependencies
COPY --from=builder /root/.m2 /root/.m2
