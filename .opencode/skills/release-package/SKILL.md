---
name: release-package
description: Release a Middenly NuGet package to nuget.org. Use when the user says "release", "publish package", "bump version", or "deploy nuget". Handles build, test, pack, push, git tag, and push.
---

# Release NuGet Package

Automates the full release workflow for Middenly NuGet packages.

## Prerequisites

- NuGet API key must be provided by the user (ask if not available)
- All tests must pass before publishing
- Docker must be running for integration tests

## Workflow

### 1. Determine package and version

Ask the user which package to release and the new version:

- Package: `Middenly.Outbox` or `Middenly.Outbox.EntityFrameworkCore`
- Version: follow semver (e.g., `1.0.2`, `1.1.0`, `2.0.0`)

### 2. Update version in csproj

Edit the `<Version>` tag in the package's `.csproj` file.

### 3. Build

```bash
dotnet build --configuration Release
```

Must succeed with 0 errors, 0 warnings.

### 4. Run all tests

```bash
dotnet test --configuration Release --verbosity normal
```

All tests must pass. If any fail — stop and fix before releasing.

### 5. Pack

```bash
dotnet pack src/<PackageId>.csproj --configuration Release --output ./nupkg
```

Verify the `.nupkg` file was created in `nupkg/`.

### 6. Push to NuGet

```bash
dotnet nuget push "nupkg/<PackageId>.<Version>.nupkg" \
  --source "https://api.nuget.org/v3/index.json" \
  --api-key "<API_KEY>" \
  --skip-duplicate
```

The API key should be provided by the user. If not available, ask for it.

### 7. Git commit and tag

```bash
git add -A
git commit -m "release: <PackageId> v<Version>"
git tag "v<Version>"
git push origin main --tags
```

### 8. Confirm

Report the NuGet URL to the user:
```
https://www.nuget.org/packages/<PackageId>/<Version>
```

## Important Notes

- Never publish without all tests passing
- The symbol package (.snupkg) may fail if no PDB files exist — this is normal and can be ignored
- NuGet indexing takes 1-5 minutes after push
- If the package has prerelease dependencies (e.g., .NET preview), the version should also be prerelease (e.g., `1.0.0-preview.1`)
