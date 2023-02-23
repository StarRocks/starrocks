# This docker file build the Starrocks be ubuntu image
# Please run this command from the git repo root directory to build:
#
#   - Use artifact image to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=ghcr.io/starrocks/starrocks/artifact-ubuntu:latest -f docker/dockerfiles/be-ubuntu.Dockerfile -t be-ubuntu:latest .
#   - Use locally build artifacts to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f docker/dockerfiles/be-ubuntu.Dockerfile -t be-ubuntu:latest .

# The artifact source used for packing the runtime docker image
#   image: copy the artifacts from a artifact docker image.
#   local: copy the artifacts from a local repo. Mainly used for local development and test.
ARG ARTIFACT_SOURCE=image

ARG ARTIFACTIMAGE=artifact:latest
FROM ${ARTIFACTIMAGE} as artifacts-from-image

# create a docker build stage that copy locally build artifacts
FROM busybox:latest as artifacts-from-local
ARG LOCAL_REPO_PATH
COPY ${LOCAL_REPO_PATH}/output/be /release/be_artifacts/be


FROM artifacts-from-${ARTIFACT_SOURCE} as artifacts

FROM ubuntu:22.04

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends binutils-dev default-jdk python2 \
           mysql-client curl vim tree net-tools less

# Install timezone data. This is needed by Starrocks broker load.
RUN apt-get install -yq tzdata && \
    ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# Install perf tool for low-level performance debug
RUN apt-get install -yq linux-tools-common linux-tools-generic
RUN echo "export PATH=/usr/lib/linux-tools/5.15.0-60-generic:$PATH" >> /etc/bash.bashrc

RUN rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/lib/jvm/default-java

ARG STARROCKS_ROOT=/opt/starrocks

WORKDIR $STARROCKS_ROOT

# Copy all artifacts to the runtime container image
COPY --from=artifacts /release/be_artifacts/ $STARROCKS_ROOT/

# Copy be k8s scripts to the runtime container image
COPY docker/dockerfiles/be/*.sh $STARROCKS_ROOT/

# Create directory for BE storage
RUN mkdir -p $STARROCKS_ROOT/be/storage
