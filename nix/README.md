# StarRocks Nix

Native Nix support for Linux and Apple Silicon macOS. No Docker.

## Development shell

Use the shell when you want the build tools on your machine:

```bash
nix develop
starrocks-build-env --print
starrocks-build-env --fe
```

The wrapper only sets environment variables and then runs the normal StarRocks
`build.sh`.

Direnv users can run `direnv allow` once. The checked-in `.envrc` enters this
default shell. Use `nix develop .#be` explicitly for backend work.

Backend builds need the native third-party tree. Use the backend shell when you
want that tree provided by Nix:

```bash
nix develop .#be
starrocks-build-env --be
```

The backend shell sets `STARROCKS_THIRDPARTY` to the Nix-built
`.#starrocks-thirdparty` output. It does not copy dependencies into the checkout.

## Package build

Use the package when you want Nix to build StarRocks:

```bash
nix build .#starrocks --cores 0 --max-jobs 1
```

This builds the current checkout into the Nix store. The derivation uses:

- Nix-provided build tools and JDK 21
- fixed-output third-party source archives
- a pinned offline Maven dependency repository
- the normal StarRocks third-party and top-level build scripts

Third-party dependencies are split into cacheable Nix layers:

- `.#starrocks-thirdparty-foundation`
- `.#starrocks-thirdparty-rpc`
- `.#starrocks-thirdparty-formats`
- `.#starrocks-thirdparty-leaf`

`.#starrocks-thirdparty` points at the assembled final third-party tree, and
`.#starrocks` consumes that tree directly from the Nix store.

`--cores 0` lets one build use all local cores. `--max-jobs 1` avoids starting
several large StarRocks builds at once.

The package build disables the optional binary payloads that are not native
source inputs here: starcache, tenann, Jindo SDK, GCS connector, pprof, and the
bundled JDK.

After changing third-party archive variables or the package manifest, run
`./nix/generate-thirdparty-archives.py`.

## Single node and multi node

The flake builds one StarRocks output tree. For local single-node use, run FE and
BE from that output tree on one machine with the normal StarRocks configs. For a
multi-node setup, use the same output tree on each host and configure FE/BE/CN
roles the usual StarRocks way.

## CI

The Nix workflow checks:

- `x86_64-linux`
- `aarch64-linux`
- `aarch64-darwin`

Normal PR CI evaluates the derivations and checks the shell. A manual
`full-build` workflow input runs the expensive `nix build .#starrocks` package
build on the full matrix.

## Cachix

Cache publishing is separate from PR checks. Pushes to `main`, or a manual run
with `publish-cache`, build `.#starrocks` for the full matrix and push the
result to Cachix. The `publish-nix` GitHub environment must provide:

- `CACHIX_CACHE_NAME`
- `CACHIX_AUTH_TOKEN`
