The building of Starrocks artifacts and packaging to runtime container images are performed in a hermetic, [multi-stage docker build](https://docs.docker.com/build/building/multi-stage/) environment. This setup enables the reuse of FE/BE artifacts for packaging into container images for different deployment scenarios. The building of artifacts will be executed in parallel leveraging the [BuildKit](https://docs.docker.com/build/buildkit/) for optimal speed.

![img.png](img.png)

### [1. StarRocks Ubuntu dev env image](dev-env/README.md)
### [2. StarRocks artifacts image](artifacts/README.md)
### [3. StarRocks fe image](fe/README.md)
### [4. StarRocks be image](be/README.md)
### [5. StarRocks all-in-one image](allin1/README.md)
### [6. StarRocks toolchains image](toolchains/README.md)
