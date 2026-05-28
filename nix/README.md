# StarRocks Nix shell

Use this when you want the same build tools on Linux and Apple Silicon macOS.

```bash
nix develop
starrocks-build-env --print
```

Build StarRocks from the repository root:

```bash
starrocks-build-env --fe
starrocks-build-env --be
```

This is not a separate build system. The wrapper only puts the Nix-provided
tools on `PATH`, exports the build environment variables StarRocks already
understands, and then runs `./build.sh`. Third-party dependencies still follow
the normal StarRocks scripts and cache/download flow.

CI runs `nix flake check` on:

- `x86_64-linux`
- `aarch64-linux`
- `aarch64-darwin`
