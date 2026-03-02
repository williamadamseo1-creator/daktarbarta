# daktarbarta static export (Cloudflare Pages)

This repo contains a static export of `daktarbarta.com` generated from:

- WordPress XML: `daktarbarta.WordPress.2026-03-02.xml`
- Live snapshots from `https://daktarbarta.com`

## What is included

- `dist/`: Cloudflare Pages-ready static site
- `tools/export_wp_xml_to_static.py`: repeatable export script
- Auto-generated SEO files on each export:
  - `dist/robots.txt`
  - `dist/sitemap.xml`
  - `dist/sitemap_index.xml`
  - `dist/_headers` (cache + basic security headers)

## Cloudflare Pages setup

Use these settings in Cloudflare Pages:

- Framework preset: `None`
- Build command: *(leave empty)*
- Build output directory: `dist`

## Rebuild locally

```powershell
python tools/export_wp_xml_to_static.py --xml daktarbarta.WordPress.2026-03-02.xml --out dist --clean --follow-links --max-pages 500
```

## Notes

- Export keeps WordPress-style URLs by generating both decoded and percent-encoded path variants.
- During export, 4 media URLs returned 404 from the live source and could not be downloaded:
  - `/wp-content/uploads/2025/06/318414a3-4a8c-4ec9-a69d-3c08ac68a30f.png`
  - `/wp-content/uploads/2025/06/Daktar-Barta-683x1024.png`
  - `/wp-content/uploads/2025/07/DR-ALI-scaled.webp`
  - `/wp-content/uploads/2025/07/Dr.-Lina-1024x598.webp`

## AI content automation (OpenAI ChatGPT)

New generator script:

- `tools/ai_content_publisher.py`

It reads `kws.csv`, generates long-form posts with ChatGPT, publishes to `dist`, rebuilds homepage/pagination/category archives, and refreshes SEO files.

### Commands

```powershell
# publish N posts
python tools/ai_content_publisher.py run --count 20 --auto-commit --auto-push

# faster bulk generation with relaxed validation (manual push workflow)
python tools/ai_content_publisher.py run --count 200 --openai-model gpt-4.1-mini --openai-fallback-model gpt-4o-mini --max-retries-per-keyword 1 --relaxed-validation

# daily profile (same as run + auto commit/push)
python tools/ai_content_publisher.py run-daily

# generate + validate only, no file writes
python tools/ai_content_publisher.py dry-run --count 2

# rebuild manifest/archives/categories/sitemaps from existing dist only
python tools/ai_content_publisher.py rebuild-index
```

One-click generate all remaining keywords sequentially (no auto push):

```powershell
powershell -ExecutionPolicy Bypass -File .\run_generate_all.ps1
```

Optional flags:

- `--max-retries-per-keyword 3`
- `--timezone Asia/Dhaka`
- `--openai-model gpt-4.1-mini`
- `--openai-fallback-model gpt-4o-mini`
- `--openai-timeout 180`

Required environment variable:

- `OPENAI_API_KEY`

Windows PowerShell example:

```powershell
$env:OPENAI_API_KEY="sk-..."
powershell -ExecutionPolicy Bypass -File .\run_generate_all.ps1
```

Runtime data:

- `data/content_pipeline_state.json`
- `data/posts_manifest.json`
- `logs/content_pipeline.log`

### Windows Task Scheduler (daily 00:20)

```powershell
powershell -ExecutionPolicy Bypass -File tools/setup_daily_task.ps1
```

Custom example:

```powershell
powershell -ExecutionPolicy Bypass -File tools/setup_daily_task.ps1 -RunTime "00:20" -PythonExe "python"
```
