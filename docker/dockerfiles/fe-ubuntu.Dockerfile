# This docker file build the Starrocks fe ubuntu image
# Please run this command from the git repo root directory to build:
# DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/fe-ubuntu.Dockerfile -t starrocks/fe-ubuntu:tag .

ARG ARTIFACTIMAGE=artifact:latest
FROM ${ARTIFACTIMAGE} as artifacts

FROM ubuntu:22.04

RUN apt-get update -y \
        && apt-get install -y --no-install-recommends default-jdk \
           curl vim tree net-tools \
        && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/lib/jvm/default-java

ARG STARROCKS_ROOT=/opt/starrocks

WORKDIR $STARROCKS_ROOT

# Copy all artifacts to the runtime container image
COPY --from=artifacts /release/fe_artifacts/ $STARROCKS_ROOT/

# Copy fe k8s scripts to the runtime container image
COPY docker/bin/fe_* $STARROCKS_ROOT/

# Create directory for FE metadata
RUN mkdir -p /opt/starrocks/fe/meta
