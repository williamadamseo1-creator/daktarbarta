# daktarbarta static export (Cloudflare Pages)

This repo contains a static export of `daktarbarta.com` generated from:

- WordPress XML: `daktarbarta.WordPress.2026-03-02.xml`
- Live snapshots from `https://daktarbarta.com`

## What is included

- `dist/`: Cloudflare Pages-ready static site
- `tools/export_wp_xml_to_static.py`: repeatable export script

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
