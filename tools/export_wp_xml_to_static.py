#!/usr/bin/env python3
"""
Export a WordPress site to static files for Cloudflare Pages.

Workflow:
1) Read publish post/page URLs from a WordPress XML export.
2) Crawl those URLs from the live site, plus discovered internal links.
3) Download same-domain assets (css/js/img/fonts/etc).
4) Save everything in a static file tree under an output directory.
"""

from __future__ import annotations

import argparse
import posixpath
import re
import shutil
import sys
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urldefrag, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


NAMESPACES = {
    "content": "http://purl.org/rss/1.0/modules/content/",
    "excerpt": "http://wordpress.org/export/1.2/excerpt/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "wp": "http://wordpress.org/export/1.2/",
}

USER_AGENT = (
    "Mozilla/5.0 (compatible; StaticExportBot/1.0; +https://daktarbarta.com)"
)

ASSET_TAG_ATTRS = [
    ("img", "src"),
    ("img", "data-src"),
    ("img", "srcset"),
    ("source", "src"),
    ("source", "srcset"),
    ("link", "href"),
    ("script", "src"),
    ("video", "src"),
    ("video", "poster"),
    ("audio", "src"),
    ("iframe", "src"),
]

PAGE_SKIP_PREFIXES = (
    "/wp-admin",
    "/wp-login",
    "/wp-json",
    "/xmlrpc.php",
    "/feed",
    "/comments/feed",
    "/cdn-cgi",
    "/product",
    "/cart",
    "/checkout",
    "/my-account",
)

ASSET_EXTENSIONS = {
    ".css",
    ".js",
    ".json",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
    ".eot",
    ".mp4",
    ".mp3",
    ".webm",
    ".avif",
    ".xml",
    ".txt",
    ".pdf",
    ".zip",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export WP XML site to static files.")
    parser.add_argument("--xml", required=True, help="Path to WordPress XML export file")
    parser.add_argument(
        "--out",
        default="dist",
        help="Output directory (default: dist)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1200,
        help="Maximum number of HTML pages to crawl (default: 1200)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete output directory before exporting",
    )
    parser.add_argument(
        "--follow-links",
        action="store_true",
        help="Also crawl discovered internal page links (default: disabled)",
    )
    return parser.parse_args()


def read_wp_urls(xml_path: Path) -> tuple[str, list[str]]:
    root = ET.parse(xml_path).getroot()
    channel = root.find("channel")
    if channel is None:
        raise ValueError("Invalid WordPress XML: <channel> not found")

    base_url = (channel.findtext("link") or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("Could not detect site base URL from XML <channel><link>")

    urls: list[str] = [base_url + "/"]
    for item in channel.findall("item"):
        status = (item.findtext("wp:status", default="", namespaces=NAMESPACES) or "").strip()
        post_type = (
            item.findtext("wp:post_type", default="", namespaces=NAMESPACES) or ""
        ).strip()
        link = (item.findtext("link") or "").strip()

        if status != "publish":
            continue
        if post_type not in {"post", "page"}:
            continue
        if not link:
            continue
        urls.append(link)

    deduped = []
    seen = set()
    for u in urls:
        if u not in seen:
            deduped.append(u)
            seen.add(u)
    return base_url, deduped


def normalize_url(raw_url: str, base_url: str, allowed_hosts: set[str]) -> str | None:
    if not raw_url:
        return None
    raw_url = raw_url.strip()
    if not raw_url or raw_url.startswith(("mailto:", "tel:", "javascript:", "data:")):
        return None

    abs_url = urljoin(base_url + "/", raw_url)
    abs_url, _ = urldefrag(abs_url)
    parsed = urlparse(abs_url)

    if parsed.scheme not in {"http", "https"}:
        return None
    host = parsed.netloc.lower()
    if host not in allowed_hosts:
        return None

    path = parsed.path or "/"
    path = re.sub(r"/{2,}", "/", path)
    if not path.startswith("/"):
        path = "/" + path
    if len(path) > 350:
        return None
    if set(path) == {"/"} and len(path) > 1:
        return None
    # Normalize obvious traversal artifacts and duplicate separators.
    normalized_path = posixpath.normpath(path)
    if path.endswith("/") and not normalized_path.endswith("/"):
        normalized_path += "/"
    if not normalized_path.startswith("/"):
        normalized_path = "/" + normalized_path
    if normalized_path == "/.":
        normalized_path = "/"

    # Ignore query parameters during static export to avoid duplicates.
    return f"{parsed.scheme}://{host}{normalized_path}"


def should_crawl_as_page(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path or "/"
    lower_path = path.lower()
    if any(lower_path.startswith(prefix) for prefix in PAGE_SKIP_PREFIXES):
        return False
    suffix = Path(lower_path).suffix
    if suffix in ASSET_EXTENSIONS:
        return False
    return True


def should_download_asset(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if any(path.startswith(prefix) for prefix in PAGE_SKIP_PREFIXES):
        return False
    suffix = Path(path).suffix
    if suffix in ASSET_EXTENSIONS:
        return True
    if path.startswith("/wp-content/") or path.startswith("/wp-includes/"):
        return True
    return False


def _path_to_local_rel(path_value: str, is_html: bool) -> Path:
    safe_path = path_value or "/"
    if not safe_path.startswith("/"):
        safe_path = "/" + safe_path

    if is_html:
        if safe_path.endswith("/"):
            local_rel = Path(safe_path.lstrip("/")) / "index.html"
        else:
            suffix = Path(safe_path).suffix
            if suffix:
                local_rel = Path(safe_path.lstrip("/"))
            else:
                local_rel = Path(safe_path.lstrip("/")) / "index.html"
    else:
        if safe_path.endswith("/"):
            local_rel = Path(safe_path.lstrip("/")) / "index.html"
        else:
            local_rel = Path(safe_path.lstrip("/"))
    return local_rel


def url_to_output_paths(url: str, out_dir: Path, is_html: bool) -> list[Path]:
    parsed = urlparse(url)
    raw_path = parsed.path or "/"
    decoded_path = unquote(raw_path)
    candidates = [decoded_path, raw_path]
    output_paths: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        local_rel = _path_to_local_rel(candidate, is_html)
        full_path = out_dir / local_rel
        if full_path not in seen:
            output_paths.append(full_path)
            seen.add(full_path)
    return output_paths


def extract_page_links(soup: BeautifulSoup) -> Iterable[str]:
    for tag in soup.find_all("a", href=True):
        yield tag["href"]


def _iter_srcset_urls(srcset: str) -> Iterable[str]:
    for chunk in srcset.split(","):
        part = chunk.strip()
        if not part:
            continue
        yield part.split()[0]


def extract_asset_links(soup: BeautifulSoup) -> Iterable[str]:
    url_in_style = re.compile(r"url\((['\"]?)([^'\")]+)\1\)")

    for tag_name, attr in ASSET_TAG_ATTRS:
        for tag in soup.find_all(tag_name):
            value = tag.get(attr)
            if not value:
                continue
            if attr == "srcset":
                for u in _iter_srcset_urls(value):
                    yield u
            else:
                yield value

    for tag in soup.find_all(style=True):
        style_value = tag.get("style") or ""
        for _, asset_url in url_in_style.findall(style_value):
            yield asset_url


def write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def fetch(session: requests.Session, url: str) -> requests.Response | None:
    try:
        resp = session.get(url, timeout=60, allow_redirects=True)
        return resp
    except requests.RequestException:
        return None


def main() -> int:
    args = parse_args()
    xml_path = Path(args.xml).resolve()
    out_dir = Path(args.out).resolve()

    if not xml_path.exists():
        print(f"[error] XML not found: {xml_path}")
        return 1

    if args.clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_url, seed_urls = read_wp_urls(xml_path)
    base_parsed = urlparse(base_url)
    canonical_host = base_parsed.netloc.lower()
    allowed_hosts = {canonical_host}
    if canonical_host.startswith("www."):
        allowed_hosts.add(canonical_host[4:])
    else:
        allowed_hosts.add("www." + canonical_host)

    print(f"[info] Base URL: {base_url}")
    print(f"[info] Seed URLs from XML: {len(seed_urls)}")
    print(f"[info] Output directory: {out_dir}")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    queue: deque[str] = deque()
    for seed in seed_urls:
        normalized = normalize_url(seed, base_url, allowed_hosts)
        if normalized and should_crawl_as_page(normalized):
            queue.append(normalized)

    visited_pages: set[str] = set()
    downloaded_assets: set[str] = set()
    failed_pages: list[str] = []
    failed_assets: set[str] = set()

    while queue and len(visited_pages) < args.max_pages:
        page_url = queue.popleft()
        if page_url in visited_pages:
            continue
        visited_pages.add(page_url)

        resp = fetch(session, page_url)
        if resp is None or resp.status_code >= 400:
            failed_pages.append(page_url)
            print(f"[warn] Page fetch failed: {page_url}")
            continue

        final_url = normalize_url(resp.url, base_url, allowed_hosts) or page_url
        content_type = (resp.headers.get("content-type") or "").lower()
        is_html = "text/html" in content_type or "</html" in resp.text.lower()

        if not is_html:
            # If this page URL actually points to a file, store as asset.
            for asset_path in url_to_output_paths(final_url, out_dir, is_html=False):
                write_file(asset_path, resp.content)
            downloaded_assets.add(final_url)
            print(f"[asset] {final_url}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        for output_path in url_to_output_paths(final_url, out_dir, is_html=True):
            write_file(output_path, resp.content)
        print(f"[page] {final_url}")

        if args.follow_links:
            for link in extract_page_links(soup):
                normalized = normalize_url(link, final_url, allowed_hosts)
                if not normalized:
                    continue
                if should_crawl_as_page(normalized) and normalized not in visited_pages:
                    queue.append(normalized)

        for asset_link in extract_asset_links(soup):
            normalized_asset = normalize_url(asset_link, final_url, allowed_hosts)
            if not normalized_asset:
                continue
            if not should_download_asset(normalized_asset):
                continue
            if normalized_asset in downloaded_assets:
                continue

            asset_resp = fetch(session, normalized_asset)
            if asset_resp is None or asset_resp.status_code >= 400:
                failed_assets.add(normalized_asset)
                continue
            asset_content_type = (asset_resp.headers.get("content-type") or "").lower()
            if "text/html" in asset_content_type:
                continue

            final_asset_url = (
                normalize_url(asset_resp.url, base_url, allowed_hosts) or normalized_asset
            )
            for asset_path in url_to_output_paths(final_asset_url, out_dir, is_html=False):
                write_file(asset_path, asset_resp.content)
            downloaded_assets.add(final_asset_url)

    # Basic fallback 404 page for static hosting.
    fallback_404 = out_dir / "404.html"
    if not fallback_404.exists():
        fallback_404.write_text(
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<title>404</title></head><body><h1>404 - Not Found</h1></body></html>",
            encoding="utf-8",
        )

    print()
    print("=== Export Summary ===")
    print(f"Pages saved: {len(visited_pages)}")
    print(f"Assets saved: {len(downloaded_assets)}")
    print(f"Failed pages: {len(failed_pages)}")
    print(f"Failed assets: {len(failed_assets)}")
    if failed_pages:
        print("Failed pages sample:")
        for url in failed_pages[:20]:
            print(f"  - {url}")
    if failed_assets:
        print("Failed assets sample:")
        for url in sorted(failed_assets)[:20]:
            print(f"  - {url}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
