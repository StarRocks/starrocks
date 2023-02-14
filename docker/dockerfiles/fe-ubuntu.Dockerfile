# This docker file build the Starrocks fe ubuntu image
# Please run this command from the git repo root directory to build:
#   - Use artifact image to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=ghcr.io/dengliu/starrocks/artifact-ubuntu:latest -f docker/dockerfiles/fe-ubuntu.Dockerfile -t fe-ubuntu:latest .
#   - Use locally build artifacts to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f docker/dockerfiles/fe-ubuntu.Dockerfile -t fe-ubuntu:latest .


# The artifact source used for packing the runtime docker image
#   image: copy the artifacts from a artifact docker image.
#   local: copy the artifacts from a local repo. Mainly used for local development and test.
ARG ARTIFACT_SOURCE=image

ARG ARTIFACTIMAGE=artifact:latest
FROM ${ARTIFACTIMAGE} as artifacts-from-image

# create a docker build stage that copy locally build artifacts
FROM busybox:latest as artifacts-from-local
ARG LOCAL_REPO_PATH
COPY ${LOCAL_REPO_PATH}/output/fe /release/fe_artifacts/fe


FROM artifacts-from-${ARTIFACT_SOURCE} as artifacts


FROM ubuntu:22.04

RUN apt-get update -y \
        && apt-get install -y --no-install-recommends default-jdk \
           mysql-client curl vim tree net-tools less \
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
