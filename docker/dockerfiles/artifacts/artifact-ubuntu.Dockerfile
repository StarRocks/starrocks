# This docker file build the Starrocks artifacts fe & be and package them into a busybox-based image
# Please run this command from the git repo root directory to build:
# DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/artifact-ubuntu.Dockerfile -t starrocks/artifact-ubuntu:tag .

ARG builder=ghcr.io/starrocks/starrocks/dev-env-ubuntu:main
ARG RELEASE_VERSION

FROM ${builder} as fe-builder
ARG RELEASE_VERSION
# clean and build Frontend and Spark Dpp application
COPY . /build/starrocks
WORKDIR /build/starrocks
RUN STARROCKS_VERSION=${RELEASE_VERSION} MAVEN_OPTS='-Dmaven.artifact.threads=128' ./build.sh --fe --clean


FROM ${builder} as be-builder
ARG RELEASE_VERSION
# build Backend in different mode (build_type could be Release, Debug, or Asan. Default value is Release.
ARG BUILD_TYPE=Release
COPY . /build/starrocks
WORKDIR /build/starrocks
RUN STARROCKS_VERSION=${RELEASE_VERSION} BUILD_TYPE=${BUILD_TYPE} ./build.sh --be --clean -j `nproc`


FROM busybox:latest

LABEL org.opencontainers.image.source="https://github.com/starrocks/starrocks"

COPY --from=fe-builder /build/starrocks/output /release/fe_artifacts
COPY --from=be-builder /build/starrocks/output /release/be_artifacts

WORKDIR /release
