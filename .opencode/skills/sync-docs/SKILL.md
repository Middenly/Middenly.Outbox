---
name: sync-docs
description: Sync package docs to the Middenly docs hub. Use when the user says "update docs site", "sync docs", "deploy docs", or "publish docs". Builds VitePress docs and copies to the hub repo.
---

# Sync Docs to Hub

Builds package documentation and syncs it to the Middenly docs hub (https://middenly.github.io/Middenly/).

## Workflow

### 1. Build package docs

```bash
cd docs
npm install
npm run build
```

### 2. Copy to hub repo

The hub repo is at `D:\VS Projects\MyGithub\Middenly`. Copy the built docs:

```powershell
# Copy built docs to hub's outbox directory
Remove-Item -Path "D:\VS Projects\MyGithub\Middenly\_src\outbox" -Recurse -Force -ErrorAction SilentlyContinue
Copy-Item -Path "docs\.vitepress\dist\outbox" -Destination "D:\VS Projects\MyGithub\Middenly\_src\outbox" -Recurse

# Rebuild hub docs
cd "D:\VS Projects\MyGithub\Middenly\_src"
npm run build

# Copy built output to docs/ for GitHub Pages
Remove-Item -Path "D:\VS Projects\MyGithub\Middenly\docs" -Recurse -Force
Copy-Item -Path ".vitepress\dist" -Destination "D:\VS Projects\MyGithub\Middenly\docs" -Recurse
New-Item -Path "D:\VS Projects\MyGithub\Middenly\docs\.nojekyll" -ItemType File -Force
```

### 3. Commit and push hub repo

```bash
cd "D:\VS Projects\MyGithub\Middenly"
git add -A
git commit -m "docs: update outbox documentation"
git push origin main
```

### 4. Confirm

Tell the user the docs will be live at https://middenly.github.io/Middenly/ in ~1 minute.

## Important Notes

- The hub repo uses legacy GitHub Pages (serves from `docs/` folder on `main`)
- The `.nojekyll` file is required to prevent Jekyll processing
- The `_src/` folder contains VitePress source, `docs/` contains built output
- Always rebuild the hub docs after copying to ensure paths are correct
