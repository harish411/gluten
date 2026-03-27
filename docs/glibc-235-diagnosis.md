# GLIBC_2.35 UnsatisfiedLinkError — Root Cause Analysis

## Problem

```
UnsatisfiedLinkError: libvelox.so: /lib64/libc.so.6: version `GLIBC_2.35' not found
```

Build host: Amazon Linux 2023, glibc 2.34
Runtime: EKS with AL2023, glibc 2.34

## Why GLIBC_2.35 Is Required Despite Building on glibc 2.34

`ldd --version` reports the *runtime linker* version. Symbol version requirements are baked into the `.so` at **link time**, based on which versioned symbols were actually resolved from system and third-party libraries during the build.

---

## Root Cause Candidates

### 1. Dynamically-linked system dependency compiled against glibc 2.35

`cpp/velox/CMakeLists.txt` links against several system and vcpkg libraries: ICU, RE2, Folly, protobuf, AWS SDK, GCS SDK, Azure SDK, OpenSSL, libcurl, etc. If any of those were themselves compiled against glibc 2.35 (e.g. a system `libssl.so.3` or `libcurl.so` installed from an OS that shipped with glibc 2.35), their GLIBC_2.35-versioned symbol references propagate into `libvelox.so`'s version requirement table.

This is the **most likely cause** — especially if the build machine had packages installed from a non-AL2023 source or a newer distro.

### 2. GCC 12+ emitting `__memcmpeq@GLIBC_2.35`

GCC 12+ generates calls to `__memcmpeq` (a GLIBC_2.35 symbol) as an optimized `memcmp` variant. GCC 11 does not. Verify the exact compiler used:

```bash
gcc --version   # on the build machine
cc --version
```

If either shows GCC 12+, this is the cause. Workaround: add `-fno-builtin-memcmp` to `CXXFLAGS`.

### 3. Build was done without vcpkg (`ENABLE_GLUTEN_VCPKG=OFF`)

The static-linking flags that prevent symbol bleed-through are **only set when vcpkg is active**:

- `dev/vcpkg/toolchain.cmake:41-42`:
  ```cmake
  set(CMAKE_EXE_LINKER_FLAGS  "-static-libstdc++ -static-libgcc")
  set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
  ```
- `dev/vcpkg/triplets/x64-linux-avx.cmake:10`:
  ```cmake
  set(VCPKG_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
  ```

If the velox `.so` was built with `build_velox.sh` directly (without vcpkg), system dynamic libraries are used instead of controlled vcpkg-built statics, making GLIBC version contamination much more likely.

---

## How to Diagnose

### Step 1: Extract libvelox.so from the jar

```bash
unzip -j gluten-velox-bundle-spark3.5_2.12-1.7.0-SNAPSHOT.jar \
  'linux/amd64/libvelox.so' -d /tmp/gluten-inspect/
```

### Step 2: Check the maximum GLIBC version required

```bash
objdump -p /tmp/gluten-inspect/libvelox.so | grep GLIBC
# or
readelf -V /tmp/gluten-inspect/libvelox.so | grep -A2 GLIBC
```

### Step 3: Find the exact offending symbols

```bash
# Most direct: shows function names requiring GLIBC_2.35
objdump -T /tmp/gluten-inspect/libvelox.so | grep "GLIBC_2.35"

# Alternative
nm -D /tmp/gluten-inspect/libvelox.so | grep GLIBC_2.35
```

Common culprits:
- `__memcmpeq` → GCC 12+ optimization (fix: use GCC 11 or `-fno-builtin-memcmp`)
- `pthread_getattr_default_np` → glibc 2.35 pthread symbol
- Any symbol from `libssl`, `libcrypto`, `libcurl` → system lib compiled against newer glibc

### Step 4: Identify which dependency introduces it

```bash
# Check what libvelox.so depends on
ldd /tmp/gluten-inspect/libvelox.so

# For each dynamic dependency (e.g. libssl.so.3), check its own GLIBC requirements
objdump -p /usr/lib64/libssl.so.3 | grep GLIBC
```

---

## Fixes

| Cause | Fix |
|---|---|
| System lib compiled against glibc 2.35 | Statically link it (via vcpkg), or use a build container pinned to AL2023 with no external packages |
| GCC 12+ `__memcmpeq` | Pin GCC to 11.x; or add `-fno-builtin-memcmp` to `CXXFLAGS` |
| Build without vcpkg | Rebuild with `ENABLE_GLUTEN_VCPKG=ON` and the `x64-linux-avx` triplet |
| Any dynamic dep leaking glibc symbols | Use an isolated AL2023 Docker build container; install all deps inside it |

### Verification after fix

```bash
objdump -p libvelox.so | grep GLIBC
# Expected: max version GLIBC_2.34 or lower
```

---

## Key Files

| File | Relevance |
|---|---|
| `dev/vcpkg/toolchain.cmake:41-42` | `-static-libstdc++ -static-libgcc` flags (vcpkg builds only) |
| `dev/vcpkg/triplets/x64-linux-avx.cmake:10` | vcpkg triplet repeats static linker flags for all ports |
| `cpp/velox/CMakeLists.txt:234-238` | symbol version script applied only with `ENABLE_GLUTEN_VCPKG` |
| `cpp/velox/symbols.map` | controls exported symbols; does not filter GLIBC requirements |
| `cpp/core/symbols.map` | hides internal google/glog/gflags symbols; same caveat |
