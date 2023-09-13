# This docker file build the Starrocks fe ubuntu image
# Please run this command from the git repo root directory to build:
#   - Use artifact image to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest -f docker/dockerfiles/fe/fe-ubuntu.Dockerfile -t fe-ubuntu:latest .
#   - Use locally build artifacts to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f docker/dockerfiles/fe/fe-ubuntu.Dockerfile -t fe-ubuntu:latest .
#
# The artifact source used for packing the runtime docker image
#   image: copy the artifacts from a artifact docker image.
#   local: copy the artifacts from a local repo. Mainly used for local development and test.
ARG ARTIFACT_SOURCE=image

ARG ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest
FROM ${ARTIFACTIMAGE} as artifacts-from-image

# create a docker build stage that copy locally build artifacts
FROM busybox:latest as artifacts-from-local
ARG LOCAL_REPO_PATH
COPY ${LOCAL_REPO_PATH}/output/fe /release/fe_artifacts/fe


FROM artifacts-from-${ARTIFACT_SOURCE} as artifacts


FROM ubuntu:22.04
ARG STARROCKS_ROOT=/opt/starrocks

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        default-jdk mysql-client curl vim tree net-tools less tzdata linux-tools-common linux-tools-generic && \
        ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
        dpkg-reconfigure -f noninteractive tzdata && \
        rm -rf /var/lib/apt/lists/*
RUN echo "export PATH=/usr/lib/linux-tools/5.15.0-60-generic:$PATH" >> /etc/bash.bashrc && \
        touch /.dockerenv
ENV JAVA_HOME=/lib/jvm/default-java

WORKDIR $STARROCKS_ROOT

# Run as starrocks user
ARG USER=starrocks
ARG GROUP=starrocks
RUN groupadd --gid 1000 $GROUP && useradd --no-create-home --uid 1000 --gid 1000 \
             --shell /usr/sbin/nologin $USER && \
    chown -R $USER:$GROUP $STARROCKS_ROOT
USER $USER

# Copy all artifacts to the runtime container image
COPY --from=artifacts --chown=starrocks:starrocks /release/fe_artifacts/ $STARROCKS_ROOT/

# Copy fe k8s scripts to the runtime container image
COPY --chown=starrocks:starrocks docker/dockerfiles/fe/*.sh $STARROCKS_ROOT/

# Create directory for FE metadata
RUN mkdir -p /opt/starrocks/fe/meta

# run as root by default
USER root
