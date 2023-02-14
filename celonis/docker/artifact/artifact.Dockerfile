# This docker file build the Starrocks artifacts fe/be/udfs and package them into a busybox basedimage
# Please run this command from the git repo root directory to build:
# DOCKER_BUILDKIT=1 docker build --rm=true -f celonis/docker/StarrocksBuilder/StarrocksBuilder.Dockerfile -t starrocks-artifacts:tag .

# dev-env image, replace it with ghcr.io/celonis/celostar/starrocks-centos-dev-env:latest to build centos artifacts
ARG builder=ghcr.io/celonis/celostar/starrocks-ubuntu-dev-env:latest

FROM ${builder} as fe-builder
# clean and build Frontend and Spark Dpp application
COPY . /build/starrocks
WORKDIR /build/starrocks
RUN MAVEN_OPTS='-Dmaven.artifact.threads=128' ./build.sh --fe --clean

FROM ${builder} as be-builder
# build Backend in different mode (build_type could be Release, Debug, or Asan. Default value is Release.
ARG BUILD_TYPE=Release
COPY . /build/starrocks
WORKDIR /build/starrocks
RUN BUILD_TYPE=${BUILD_TYPE} ./build.sh --be --clean -j `nproc`

FROM ${builder} as udf-builder
# clean and build duplicate-invoice-checker UDF
COPY . /build/starrocks
WORKDIR /build/starrocks/java-extensions/udffailures
RUN MAVEN_OPTS='-Dmaven.artifact.threads=128' mvn package

FROM busybox:latest
LABEL org.opencontainers.image.source = "https://github.com/celonis/celostar-starrocks"

COPY --from=fe-builder /build/starrocks/output /release/fe_artifacts
COPY --from=be-builder /build/starrocks/output /release/be_artifacts
COPY --from=udf-builder /build/starrocks/java-extensions/udffailures/target/udf-failure-reproduce-1.0-SNAPSHOT-jar-with-dependencies.jar /release/udf/udf-failure-reproduce-1.0-SNAPSHOT.jar

WORKDIR /release
