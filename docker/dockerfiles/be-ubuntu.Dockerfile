# This docker file build the Starrocks be ubuntu image
# Please run this command from the git repo root directory to build:
# DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/be-ubuntu.Dockerfile -t starrocks/be-ubuntu:tag .

ARG ARTIFACTIMAGE=artifact:latest
FROM ${ARTIFACTIMAGE} as artifacts

FROM ubuntu:22.04

RUN apt-get update -y \
        && apt-get install -y --no-install-recommends binutils-dev default-jdk python2 \
           mysql-client curl vim tree net-tools \
        && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/lib/jvm/default-java

ARG STARROCKS_ROOT=/opt/starrocks

WORKDIR $STARROCKS_ROOT

# Copy all artifacts to the runtime container image
COPY --from=artifacts /release/be_artifacts/ $STARROCKS_ROOT/

# Copy be k8s scripts to the runtime container image
COPY docker/bin/be_* $STARROCKS_ROOT/

# Create directory for BE storage
RUN mkdir -p $STARROCKS_ROOT/be/storage
