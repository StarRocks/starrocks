# Setup development environment with Docker

This directory provides Docker-based build tools for StarRocks that use the official `starrocks/dev-env-ubuntu:latest` development environment image. This ensures consistent builds across different host systems by using a standardized Ubuntu environment with all required toolchains and dependencies pre-installed.

## 🚀 Quick Start

### Prerequisites
- Docker installed and running
- At least 8GB RAM available for Docker
- At least 20GB free disk space

### Simple Commands

```bash
# Open development shell
./docker-dev.sh shell

# Build Frontend only
./docker-dev.sh build-fe

# Build Backend only  
./docker-dev.sh build-be

# Build everything
./docker-dev.sh build-all

# Clean build everything
./docker-dev.sh clean-build

# Run Frontend tests
./docker-dev.sh test-fe
```

## 📋 Available Tools

### 1. `build-in-docker.sh` - Full-Featured Build Script

The main build script that automatically passes through all `build.sh` options:

```bash
# Basic usage
./build-in-docker.sh                          # Build all (FE + BE)
./build-in-docker.sh --fe                     # Build Frontend only
./build-in-docker.sh --be                     # Build Backend only
./build-in-docker.sh --fe --be --clean        # Clean and build both

# Advanced options
./build-in-docker.sh --be --with-gcov          # Build BE with code coverage
./build-in-docker.sh --fe --disable-java-check-style  # Skip checkstyle
./build-in-docker.sh --be -j 8                 # Build with 8 parallel jobs

# Development
./build-in-docker.sh --shell                  # Interactive shell
./build-in-docker.sh --test                   # Build and run tests

# Custom image
./build-in-docker.sh --image starrocks/dev-env-ubuntu:latest --fe

# Future build.sh options work automatically
./build-in-docker.sh --be --new-future-option
```

### 2. `docker-dev.sh` - Simple Wrapper

Quick commands for common tasks:

```bash
./docker-dev.sh shell           # Development shell
./docker-dev.sh build-fe        # Build Frontend
./docker-dev.sh build-be        # Build Backend
./docker-dev.sh build-all       # Build everything
./docker-dev.sh clean-build     # Clean and build all
./docker-dev.sh test-fe         # Run FE tests
./docker-dev.sh test-be         # Run BE tests
./docker-dev.sh test-all        # Run all tests

# Pass through any build.sh options
./docker-dev.sh build --be --with-gcov
./docker-dev.sh build --fe --new-option
```

### 3. `docker-compose.dev.yml` - Docker Compose

For persistent development environments:

```bash
# Start development shell
docker-compose -f docker-compose.dev.yml run --rm starrocks-dev

# Build Frontend
docker-compose -f docker-compose.dev.yml run --rm build-fe

# Build Backend
docker-compose -f docker-compose.dev.yml run --rm build-be

# Run tests
docker-compose -f docker-compose.dev.yml run --rm test-fe
docker-compose -f docker-compose.dev.yml run --rm test-be

# Clean up
docker-compose -f docker-compose.dev.yml down -v
```

## 🔧 Configuration

### Volume Mounts

The Docker scripts automatically mount:
- **Source code**: `$(pwd):/workspace` - Your local repository
- **Maven cache**: `~/.m2:/tmp/.m2` - Maven dependencies cache for faster builds

**Note**: The Maven cache is shared between your host system and the Docker container, so dependencies downloaded during builds are persisted and reused across build sessions.

### Environment Variables

```bash
# Use different Docker image
export STARROCKS_DEV_ENV_IMAGE=starrocks/dev-env-ubuntu:latest

# Additional Docker options
export DOCKER_BUILD_OPTS="--memory=16g --cpus=8"

# Set user ID for file permissions
export UID=$(id -u)
export GID=$(id -g)
```

### Custom Build Options

All original `build.sh` options are supported:

```bash
# Backend build types
BUILD_TYPE=Debug ./build-in-docker.sh --be      # Debug build
BUILD_TYPE=Release ./build-in-docker.sh --be    # Release build (default)
BUILD_TYPE=Asan ./build-in-docker.sh --be       # AddressSanitizer build

# Feature flags
./build-in-docker.sh --be --enable-shared-data  # Enable shared data
./build-in-docker.sh --be --with-gcov           # Code coverage
./build-in-docker.sh --be --with-bench          # Benchmarks
./build-in-docker.sh --be --without-avx2        # Disable AVX2
```

## 📁 Output and Artifacts

Build artifacts are created in the `output/` directory:

```
output/
├── fe/                 # Frontend artifacts
├── be/                 # Backend artifacts
└── java-extensions/    # Java extensions
```

The Docker container mounts your local repository and Maven cache, so all build outputs are available on your host system and dependencies are cached for faster subsequent builds.

**Multi-User Support**: Container names include username and user ID to prevent conflicts on shared development machines (e.g., `starrocks-build-username-1001-1234567890`).

**Automatic Extensibility**: All unrecognized options are automatically passed through to `build.sh`, so new build options work without updating the Docker scripts.

## 🐛 Troubleshooting

### Common Issues

1. **Permission Issues**
   ```bash
   # Fix file permissions
   sudo chown -R $(id -u):$(id -g) output/
   
   # Or run with correct user
   export UID=$(id -u) GID=$(id -g)
   ./build-in-docker.sh --fe
   ```

2. **Out of Memory**
   ```bash
   # Increase Docker memory limit or reduce parallel jobs
   ./build-in-docker.sh --be -j 2
   ```

3. **Docker Image Not Found**
   ```bash
   # Pull the image manually
   docker pull starrocks/dev-env-ubuntu:latest
   ```

4. **Build Failures**
   ```bash
   # Clean build
   ./build-in-docker.sh --clean --fe --be

   # Check logs in interactive shell
   ./build-in-docker.sh --shell
   ```

### Debug Mode

For debugging build issues:

```bash
# Open shell and run commands manually
./build-in-docker.sh --shell

# Inside container:
./build.sh --fe --clean
./run-fe-ut.sh
```

## 🔍 Verification

Test that everything works:

```bash
# 1. Test Docker setup
docker --version
docker info

# 2. Test image availability
docker pull starrocks/dev-env-ubuntu:latest

# 3. Test build scripts
./docker-dev.sh shell
# Inside container: exit

# 4. Test simple build
./docker-dev.sh build-fe
```

## 📊 Performance Tips

1. **Use parallel builds**: `./build-in-docker.sh --be -j $(nproc)`
2. **Maven cache**: Automatically mounted from `~/.m2` for faster dependency resolution
3. **Memory allocation**: Increase Docker memory limit for faster builds
4. **SSD storage**: Use SSD for Docker storage driver
5. **Persistent volumes**: Use Docker Compose for additional cache persistence

## 🤝 Integration with IDEs

### VS Code with Dev Containers

Create `.devcontainer/devcontainer.json`:

```json
{
    "name": "StarRocks Dev Environment",
    "image": "starrocks/dev-env-ubuntu:latest",
    "workspaceMount": "source=${localWorkspaceFolder},target=/workspace,type=bind",
    "workspaceFolder": "/workspace",
    "customizations": {
        "vscode": {
            "extensions": [
                "ms-vscode.cpptools",
                "redhat.java"
            ]
        }
    }
}
```

### IntelliJ IDEA

Use the Docker integration to run builds and tests within the container environment.

## 📚 Additional Resources
- [Original build.sh](../../../../build.sh) for reference

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Verify Docker setup and image availability
3. Try clean builds with `--clean` flag
4. Open an issue with build logs and system information
