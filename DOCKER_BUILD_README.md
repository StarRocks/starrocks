# Docker-Based Build for StarRocks

This directory provides Docker-based build tools for StarRocks that use the official `starrocks/dev-env-ubuntu:main-20250619` development environment image. This ensures consistent builds across different host systems by using a standardized Ubuntu environment with all required toolchains and dependencies pre-installed.

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

### 1. `docker-build.sh` - Full-Featured Build Script

The main build script with all options from the original `build.sh`:

```bash
# Basic usage
./docker-build.sh                          # Build all (FE + BE)
./docker-build.sh --fe                     # Build Frontend only
./docker-build.sh --be                     # Build Backend only
./docker-build.sh --fe --be --clean        # Clean and build both

# Advanced options
./docker-build.sh --be --with-gcov          # Build BE with code coverage
./docker-build.sh --fe --disable-java-check-style  # Skip checkstyle
./docker-build.sh --be -j 8                 # Build with 8 parallel jobs

# Development
./docker-build.sh --shell                  # Interactive shell
./docker-build.sh --test                   # Build and run tests

# Custom image
./docker-build.sh --image starrocks/dev-env-ubuntu:main-20250619 --fe
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

### Environment Variables

```bash
# Use different Docker image
export STARROCKS_DEV_ENV_IMAGE=starrocks/dev-env-ubuntu:main-20250619

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
BUILD_TYPE=Debug ./docker-build.sh --be      # Debug build
BUILD_TYPE=Release ./docker-build.sh --be    # Release build (default)
BUILD_TYPE=Asan ./docker-build.sh --be       # AddressSanitizer build

# Feature flags
./docker-build.sh --be --enable-shared-data  # Enable shared data
./docker-build.sh --be --with-gcov           # Code coverage
./docker-build.sh --be --with-bench          # Benchmarks
./docker-build.sh --be --without-avx2        # Disable AVX2
```

## 📁 Output and Artifacts

Build artifacts are created in the `output/` directory:

```
output/
├── fe/                 # Frontend artifacts
├── be/                 # Backend artifacts
└── java-extensions/    # Java extensions
```

The Docker container mounts your local repository, so all build outputs are available on your host system.

## 🐛 Troubleshooting

### Common Issues

1. **Permission Issues**
   ```bash
   # Fix file permissions
   sudo chown -R $(id -u):$(id -g) output/
   
   # Or run with correct user
   export UID=$(id -u) GID=$(id -g)
   ./docker-build.sh --fe
   ```

2. **Out of Memory**
   ```bash
   # Increase Docker memory limit or reduce parallel jobs
   ./docker-build.sh --be -j 2
   ```

3. **Docker Image Not Found**
   ```bash
   # Pull the image manually
   docker pull starrocks/dev-env-ubuntu:main-20250619
   ```

4. **Build Failures**
   ```bash
   # Clean build
   ./docker-build.sh --clean --fe --be
   
   # Check logs in interactive shell
   ./docker-build.sh --shell
   ```

### Debug Mode

For debugging build issues:

```bash
# Open shell and run commands manually
./docker-build.sh --shell

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
docker pull starrocks/dev-env-ubuntu:main-20250619

# 3. Test build scripts
./docker-dev.sh shell
# Inside container: exit

# 4. Test simple build
./docker-dev.sh build-fe
```

## 📊 Performance Tips

1. **Use parallel builds**: `./docker-build.sh --be -j $(nproc)`
2. **Persistent volumes**: Use Docker Compose for Maven cache persistence
3. **Memory allocation**: Increase Docker memory limit for faster builds
4. **SSD storage**: Use SSD for Docker storage driver

## 🤝 Integration with IDEs

### VS Code with Dev Containers

Create `.devcontainer/devcontainer.json`:

```json
{
    "name": "StarRocks Dev Environment",
    "image": "starrocks/dev-env-ubuntu:main-20250619",
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
- [Original build.sh](./build.sh) for reference

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Verify Docker setup and image availability
3. Try clean builds with `--clean` flag
4. Open an issue with build logs and system information
