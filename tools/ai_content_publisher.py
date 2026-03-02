#!/usr/bin/env python3
"""
Generate and publish static long-form articles from kws.csv using OpenAI ChatGPT.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
from copy import deepcopy
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse

import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

try:
    from tools.export_wp_xml_to_static import generate_seo_files
except ModuleNotFoundError:
    from export_wp_xml_to_static import generate_seo_files


REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"
DATA_DIR = REPO_ROOT / "data"
LOG_DIR = REPO_ROOT / "logs"
STATE_FILE = DATA_DIR / "content_pipeline_state.json"
MANIFEST_FILE = DATA_DIR / "posts_manifest.json"
LOG_FILE = LOG_DIR / "content_pipeline.log"
KWS_FILE = REPO_ROOT / "kws.csv"

SITE_BASE_URL = "https://daktarbarta.com"
SITE_NAME = "Daktar Barta"
SITE_TAGLINE = "দেশ সেরা ডাক্তারের উন্নত চিকিৎসা নিন"
DEFAULT_TIMEZONE = "Asia/Dhaka"
MAX_ARCHIVE_ITEMS_PER_PAGE = 10

BENGALI_CHAR_RE = re.compile(r"[\u0980-\u09FF]")
TOKEN_RE = re.compile(r"[A-Za-z0-9\u0980-\u09FF]+")
WORD_RE = re.compile(r"[A-Za-z0-9\u0980-\u09FF]+")

CATEGORY_RULES = [
    {"name": "নাক", "slug": "নাক", "tokens": ["নাক", "nasal", "sinus", "polyp", "rhinitis", "snoring"]},
    {"name": "কান", "slug": "কান", "tokens": ["কান", "ear", "tinnitus", "otitis"]},
    {"name": "গলনালী", "slug": "গলনালী", "tokens": ["গলা", "throat", "tonsil", "larynx"]},
    {"name": "চর্ম রোগ", "slug": "চর্ম-রোগ", "tokens": ["চর্ম", "skin", "eczema", "fungal", "psoriasis"]},
    {"name": "টিউমার", "slug": "টিউমার", "tokens": ["tumor", "tumour", "cancer", "mass", "cyst"]},
    {"name": "মলদ্বার", "slug": "মলদ্বার", "tokens": ["মলদ্বার", "anal", "piles", "hemorrhoid", "fissure", "fistula"]},
]
FALLBACK_CATEGORY = {"name": "মানসিক রোগ", "slug": "মানসিক-রোগ"}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and publish content with OpenAI ChatGPT")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--count", type=int, default=20)
    common.add_argument("--max-retries-per-keyword", type=int, default=3)
    common.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    common.add_argument("--openai-model", default="gpt-4.1-mini")
    common.add_argument("--openai-fallback-model", default="gpt-4o-mini")
    common.add_argument("--openai-timeout", type=int, default=180)
    common.add_argument(
        "--relaxed-validation",
        action="store_true",
        help="Publish content even if strict validation checks fail",
    )

    run = sub.add_parser("run", parents=[common])
    run.add_argument("--auto-commit", action="store_true")
    run.add_argument("--auto-push", action="store_true")

    sub.add_parser("run-daily", parents=[common])
    sub.add_parser("dry-run", parents=[common])

    rebuild = sub.add_parser("rebuild-index")
    rebuild.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    return parser.parse_args()


def setup_logging() -> logging.Logger:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ai_content_publisher")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def now_iso(tz_name: str) -> str:
    return datetime.now(ZoneInfo(tz_name)).isoformat(timespec="seconds")


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return deepcopy(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return deepcopy(default)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state() -> dict[str, Any]:
    state = read_json(STATE_FILE, {"cursor": 0, "next_post_id": None, "last_run": None})
    state.setdefault("cursor", 0)
    state.setdefault("next_post_id", None)
    state.setdefault("last_run", None)
    return state


def save_state(state: dict[str, Any]) -> None:
    write_json(STATE_FILE, state)


def load_manifest() -> list[dict[str, Any]]:
    payload = read_json(MANIFEST_FILE, [])
    return payload if isinstance(payload, list) else []


def save_manifest(records: list[dict[str, Any]]) -> None:
    write_json(MANIFEST_FILE, records)


def normalize_rel(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.lower()]
    if isinstance(value, list):
        return [str(item).lower() for item in value]
    return [str(value).lower()]


def find_canonical_url(soup: BeautifulSoup) -> str | None:
    for link in soup.find_all("link"):
        rel_values = normalize_rel(link.get("rel"))
        if "canonical" in rel_values and link.get("href"):
            return str(link.get("href")).strip()
    return None


def upsert_meta(soup: BeautifulSoup, attr_name: str, attr_value: str, content: str) -> None:
    node = soup.find("meta", attrs={attr_name: attr_value})
    if node is None:
        node = soup.new_tag("meta")
        node[attr_name] = attr_value
        (soup.head or soup).append(node)
    node["content"] = content


def upsert_link_rel(soup: BeautifulSoup, rel_value: str, href: str) -> None:
    node = None
    for link in soup.find_all("link"):
        if rel_value.lower() in normalize_rel(link.get("rel")):
            node = link
            break
    if node is None:
        node = soup.new_tag("link")
        node["rel"] = rel_value
        (soup.head or soup).append(node)
    node["href"] = href


def remove_link_rel(soup: BeautifulSoup, rel_value: str) -> None:
    for link in soup.find_all("link"):
        if rel_value.lower() in normalize_rel(link.get("rel")):
            link.decompose()


def path_from_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed.path or "/"


def is_post_canonical_path(path: str) -> bool:
    skip_prefixes = ("/page/", "/category/", "/tag/", "/author/", "/wp-", "/feed", "/comments")
    return path != "/" and not any(path.startswith(prefix) for prefix in skip_prefixes)


def parse_post_id(soup: BeautifulSoup) -> int | None:
    article = soup.select_one("article.entry.single-entry")
    if not article:
        return None
    if article.get("id"):
        m = re.search(r"post-(\d+)", str(article.get("id")))
        if m:
            return int(m.group(1))
    for cls in article.get("class") or []:
        m = re.match(r"post-(\d+)$", str(cls))
        if m:
            return int(m.group(1))
    return None


def extract_excerpt(soup: BeautifulSoup) -> str:
    p = soup.select_one("div.entry-content.single-content p")
    if p:
        return re.sub(r"\s+", " ", p.get_text(" ", strip=True))[:280]
    d = soup.select_one("meta[name='description']")
    if d and d.get("content"):
        return str(d.get("content")).strip()[:280]
    return ""


def parse_date_iso(raw: str | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    candidate = raw.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(candidate).isoformat(timespec="seconds")
    except ValueError:
        m = re.search(r"\d{4}-\d{2}-\d{2}", candidate)
        if m:
            return datetime.fromisoformat(m.group(0)).isoformat(timespec="seconds")
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def infer_language(text: str) -> str:
    return "bn" if BENGALI_CHAR_RE.search(text or "") else "en"


def tokenize(text: str) -> set[str]:
    return {t.lower() for t in TOKEN_RE.findall(text or "")}


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    out = {
        "slug": str(record.get("slug") or "").strip(),
        "canonical_url": str(record.get("canonical_url") or "").strip(),
        "title": str(record.get("title") or "").strip(),
        "excerpt": str(record.get("excerpt") or "").strip(),
        "language": str(record.get("language") or "bn").strip() or "bn",
        "focus_keyword": str(record.get("focus_keyword") or "").strip(),
        "additional_keywords": list(record.get("additional_keywords") or []),
        "category": str(record.get("category") or FALLBACK_CATEGORY["name"]).strip(),
        "published_at": parse_date_iso(str(record.get("published_at") or "")),
        "source": str(record.get("source") or "existing").strip() or "existing",
        "post_id": int(record.get("post_id") or 0),
    }
    out["additional_keywords"] = [str(x).strip() for x in out["additional_keywords"] if str(x).strip()][:5]
    return out


def sort_records_desc(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(rec: dict[str, Any]) -> tuple[datetime, int]:
        try:
            dt = datetime.fromisoformat(str(rec.get("published_at")))
        except ValueError:
            dt = datetime.fromtimestamp(0, tz=timezone.utc)
        return (dt, int(rec.get("post_id") or 0))

    return sorted(records, key=sort_key, reverse=True)


def map_category(keyword: str) -> dict[str, str]:
    low = keyword.lower()
    for rule in CATEGORY_RULES:
        if any(token in low for token in rule["tokens"]):
            return {"name": rule["name"], "slug": rule["slug"]}
    return deepcopy(FALLBACK_CATEGORY)


def parse_keywords_csv(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing keyword file: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "kws" not in reader.fieldnames:
            raise ValueError("kws.csv must contain `kws` column")
        keywords = []
        for row in reader:
            kw = (row.get("kws") or "").strip()
            if kw:
                keywords.append(kw)
        return keywords


def find_templates() -> dict[str, Path]:
    post_template: Path | None = None
    for html in sorted(DIST_DIR.rglob("index.html")):
        soup = BeautifulSoup(html.read_text(encoding="utf-8", errors="ignore"), "html.parser")
        if soup.select_one("article.entry.single-entry") and soup.select_one("h1.entry-title"):
            post_template = html
            break
    if post_template is None:
        raise RuntimeError("No single post template found in dist")

    home = DIST_DIR / "index.html"
    if not home.exists():
        raise RuntimeError("dist/index.html not found")

    page = DIST_DIR / "page" / "2" / "index.html"
    if not page.exists():
        page = home

    cat = DIST_DIR / "category" / quote("à¦•à¦¾à¦¨", safe="-") / "index.html"
    if not cat.exists():
        candidates = sorted((DIST_DIR / "category").glob("*/index.html"))
        cat = candidates[0] if candidates else home

    return {"post": post_template, "home": home, "page": page, "category": cat}


def index_existing_posts(logger: logging.Logger) -> tuple[list[dict[str, Any]], int]:
    records_by_canonical: dict[str, dict[str, Any]] = {}
    max_post_id = 0

    for html in sorted(DIST_DIR.rglob("index.html")):
        soup = BeautifulSoup(html.read_text(encoding="utf-8", errors="ignore"), "html.parser")
        canonical = find_canonical_url(soup)
        post_id = parse_post_id(soup)
        if post_id:
            max_post_id = max(max_post_id, post_id)
        if not canonical:
            continue
        canon_path = path_from_url(canonical)
        if not is_post_canonical_path(canon_path):
            continue
        if not soup.select_one("article.entry.single-entry"):
            continue

        h1 = soup.select_one("h1.entry-title")
        title = h1.get_text(" ", strip=True) if h1 else ""
        if not title and soup.title:
            title = soup.title.get_text(" ", strip=True).replace(" - Daktar Barta", "").strip()
        excerpt = extract_excerpt(soup)
        pub_meta = soup.find("meta", attrs={"property": "article:published_time"})
        cat_meta = soup.find("meta", attrs={"property": "article:section"})
        category = str(cat_meta.get("content")).strip() if cat_meta and cat_meta.get("content") else FALLBACK_CATEGORY["name"]

        rec = normalize_record(
            {
                "slug": unquote(canon_path.strip("/")),
                "canonical_url": canonical.strip(),
                "title": title,
                "excerpt": excerpt,
                "language": infer_language(title + " " + excerpt),
                "focus_keyword": "",
                "additional_keywords": [],
                "category": category,
                "published_at": parse_date_iso(pub_meta.get("content") if pub_meta else None),
                "source": "existing",
                "post_id": post_id or 0,
            }
        )
        records_by_canonical[rec["canonical_url"]] = rec

    logger.info("Indexed %d existing post pages", len(records_by_canonical))
    return sort_records_desc(list(records_by_canonical.values())), max_post_id


def merge_manifest(indexed: list[dict[str, Any]], old_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    old_map = {}
    for rec in old_manifest:
        n = normalize_record(rec)
        if n["canonical_url"]:
            old_map[n["canonical_url"]] = n

    merged = {rec["canonical_url"]: rec for rec in indexed if rec.get("canonical_url")}
    for canon, rec in old_map.items():
        if canon in merged:
            merged[canon].update(
                {
                    "focus_keyword": rec.get("focus_keyword", ""),
                    "additional_keywords": rec.get("additional_keywords", []),
                    "source": rec.get("source") or merged[canon].get("source"),
                    "category": rec.get("category") or merged[canon].get("category"),
                }
            )
        else:
            merged[canon] = rec
    return sort_records_desc(list(merged.values()))


def derive_additional_keywords(focus_keyword: str, language: str) -> list[str]:
    if language == "bn":
        suffixes = ["à¦•à¦¾à¦°à¦£", "à¦²à¦•à§à¦·à¦£", "à¦šà¦¿à¦•à¦¿à§Žà¦¸à¦¾", "à¦ªà§à¦°à¦¤à¦¿à¦•à¦¾à¦°", "à¦˜à¦°à§‹à¦¯à¦¼à¦¾ à¦‰à¦ªà¦¾à¦¯à¦¼"]
    else:
        suffixes = ["causes", "symptoms", "treatment", "home remedies", "prevention"]
    return [f"{focus_keyword} {s}".strip() for s in suffixes]


def sanitize_additional_keywords(raw: Any, focus_keyword: str, language: str) -> list[str]:
    items = []
    if isinstance(raw, list):
        for item in raw:
            txt = str(item).strip()
            if txt and txt.casefold() != focus_keyword.casefold():
                items.append(txt)
    dedup = []
    seen = set()
    for item in items:
        key = item.casefold()
        if key in seen:
            continue
        dedup.append(item)
        seen.add(key)
        if len(dedup) == 5:
            break
    if len(dedup) < 5:
        for fb in derive_additional_keywords(focus_keyword, language):
            key = fb.casefold()
            if key in seen:
                continue
            dedup.append(fb)
            seen.add(key)
            if len(dedup) == 5:
                break
    return dedup[:5]


def slugify(text: str) -> str:
    low = text.lower().strip()
    cleaned = re.sub(r"[^\w\u0980-\u09FF\s-]", " ", low, flags=re.UNICODE)
    slug = re.sub(r"[\s_-]+", "-", cleaned).strip("-")
    slug = re.sub(r"-{2,}", "-", slug)
    return (slug[:90].strip("-") or "post")


def unique_slug(base_slug: str, used_slugs: set[str]) -> str:
    if base_slug not in used_slugs:
        used_slugs.add(base_slug)
        return base_slug
    i = 2
    while True:
        cand = f"{base_slug}-{i}"
        if cand not in used_slugs:
            used_slugs.add(cand)
            return cand
        i += 1


def url_path_to_output_paths(path_value: str) -> list[Path]:
    candidates = [unquote(path_value), path_value]
    out = []
    seen = set()
    for cand in candidates:
        safe = cand or "/"
        if not safe.startswith("/"):
            safe = "/" + safe
        if safe.endswith("/"):
            rel = Path(safe.lstrip("/")) / "index.html"
        else:
            rel = Path(safe.lstrip("/")) if Path(safe).suffix else Path(safe.lstrip("/")) / "index.html"
        full = DIST_DIR / rel
        if full in seen:
            continue
        out.append(full)
        seen.add(full)
    return out


def choose_models(primary_model: str, fallback_model: str | None) -> list[str]:
    models = [primary_model.strip()]
    if fallback_model and fallback_model.strip() and fallback_model.strip() not in models:
        models.append(fallback_model.strip())
    return models


def select_internal_links(
    records: list[dict[str, Any]],
    focus_keyword: str,
    additional_keywords: list[str],
    exclude_canonical: str | None = None,
    limit: int = 5,
) -> list[dict[str, str]]:
    target_tokens = tokenize(" ".join([focus_keyword] + additional_keywords))
    scored = []
    for rec in records:
        canonical = str(rec.get("canonical_url") or "").strip()
        title = str(rec.get("title") or "").strip()
        if not canonical or not title:
            continue
        if exclude_canonical and canonical == exclude_canonical:
            continue
        if not is_post_canonical_path(path_from_url(canonical)):
            continue
        overlap = len(target_tokens.intersection(tokenize(title)))
        bonus = 2 if focus_keyword.casefold() in title.casefold() else 0
        scored.append((overlap + bonus, rec.get("published_at", ""), {"title": title, "canonical_url": canonical}))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)

    picked = []
    seen = set()
    for _score, _date, item in scored:
        if item["canonical_url"] in seen:
            continue
        picked.append(item)
        seen.add(item["canonical_url"])
        if len(picked) == limit:
            break
    return picked


def build_prompt(focus_keyword: str, language: str, category_name: str, hints: list[dict[str, str]]) -> str:
    hint_text = "\n".join(f"- {h['title']} ({h['canonical_url']})" for h in hints[:8]) or "- none"
    schema = """{
  "title": "string",
  "meta_description": "string",
  "excerpt": "string",
  "additional_keywords": ["kw1","kw2","kw3","kw4","kw5"],
  "sections": [
    {"heading":"string","paragraphs":["string","string","string"]},
    {"heading":"string","paragraphs":["string","string","string"]}
  ],
  "faq":[{"q":"string","a":"string"},{"q":"string","a":"string"}]
}"""
    if language == "bn":
        return f"""
à¦¤à§à¦®à¦¿ à¦à¦•à¦œà¦¨ à¦¬à¦¾à¦‚à¦²à¦¾ à¦¸à§à¦¬à¦¾à¦¸à§à¦¥à§à¦¯ à¦•à¦¨à¦Ÿà§‡à¦¨à§à¦Ÿ à¦°à¦¾à¦‡à¦Ÿà¦¾à¦°à¥¤
Focus keyword: "{focus_keyword}"
Category: "{category_name}"
à¦¨à¦¿à§Ÿà¦®:
1) Output à¦¹à¦¬à§‡ strict JSON only.
2) à¦®à§‹à¦Ÿ content 1700+ à¦¶à¦¬à§à¦¦à¥¤
3) Focus keyword à¦•à¦®à¦ªà¦•à§à¦·à§‡ 5 à¦¬à¦¾à¦°à¥¤
4) Headings (H2/H3) à¦ keyword à¦…à¦¬à¦¶à§à¦¯à¦‡ à¦¬à§à¦¯à¦¬à¦¹à¦¾à¦° à¦•à¦°à¦¬à§‡à¥¤
5) Exactly 5 additional keywords à¦¦à¦¿à¦¬à§‡à¥¤
6) Informational safety tone.
Internal link hints:
{hint_text}
Schema:
{schema}
"""
    return f"""
You are an English health content writer.
Focus keyword: "{focus_keyword}"
Category: "{category_name}"
Rules:
1) Return strict JSON only.
2) Total content must be 1700+ words.
3) Use focus keyword naturally at least 5 times.
4) Include heading-friendly sections.
5) Exactly 5 additional keywords.
6) Informational and safe medical tone.
Internal link hints:
{hint_text}
Schema:
{schema}
"""


def openai_generate_json(api_key: str, model: str, prompt: str, timeout_seconds: int) -> str:
    res = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "temperature": 0.4,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a medical content writer assistant. Return strict JSON only.",
                },
                {"role": "user", "content": prompt},
            ],
        },
        timeout=timeout_seconds,
    )
    res.raise_for_status()
    payload = res.json()
    choices = payload.get("choices") or []
    if not choices:
        raise ValueError("OpenAI response missing choices")
    content = ((choices[0] or {}).get("message") or {}).get("content")
    if not content:
        raise ValueError("OpenAI response missing message content")
    return str(content)


def extract_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    candidates = [stripped]
    fm = re.search(r"```(?:json)?\s*(\{.*\})\s*```", stripped, flags=re.DOTALL)
    if fm:
        candidates.append(fm.group(1).strip())
    first = stripped.find("{")
    last = stripped.rfind("}")
    if first != -1 and last != -1 and last > first:
        candidates.append(stripped[first : last + 1].strip())
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    raise ValueError("Model output is not valid JSON")


def strip_and_normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def ensure_keyword_in_title(title: str, focus_keyword: str) -> str:
    if focus_keyword.casefold() in title.casefold():
        return title.strip()
    return f"{focus_keyword}: {title.strip()}".strip()


def ensure_keyword_in_heading(heading: str, focus_keyword: str) -> str:
    if focus_keyword.casefold() in heading.casefold():
        return heading
    return f"{focus_keyword} - {heading}".strip(" -")


def word_count(text: str) -> int:
    return len(WORD_RE.findall(text))


def keyword_count(text: str, keyword: str) -> int:
    return text.casefold().count(keyword.casefold()) if keyword else 0


def language_ratio(text: str, language: str) -> float:
    letters = [c for c in text if c.isalpha() or BENGALI_CHAR_RE.match(c)]
    if not letters:
        return 0.0
    if language == "bn":
        target = sum(1 for c in letters if BENGALI_CHAR_RE.match(c))
    else:
        target = sum(1 for c in letters if "a" <= c.lower() <= "z")
    return target / len(letters)


def build_article_html(
    payload: dict[str, Any],
    focus_keyword: str,
    language: str,
    additional_keywords: list[str],
    links: list[dict[str, str]],
) -> str:
    sections = payload.get("sections")
    if not isinstance(sections, list) or not sections:
        raise ValueError("Missing sections in payload")

    intro = strip_and_normalize(str(payload.get("excerpt") or ""))
    if not intro:
        intro = (
            f"{focus_keyword} à¦¸à¦®à§à¦ªà¦°à§à¦•à§‡ à¦¬à¦¿à¦¸à§à¦¤à¦¾à¦°à¦¿à¦¤ à¦•à¦¾à¦°à¦£, à¦²à¦•à§à¦·à¦£, à¦šà¦¿à¦•à¦¿à§Žà¦¸à¦¾ à¦“ à¦ªà§à¦°à¦¤à¦¿à¦°à§‹à¦§à§‡à¦° à¦—à¦¾à¦‡à¦¡à¥¤"
            if language == "bn"
            else f"Detailed guide to {focus_keyword}: causes, symptoms, treatment, and prevention."
        )

    parts = [f"<p>{escape(intro)}</p>"]
    heading_hits = 0
    link_idx = 0

    for i, section in enumerate(sections):
        if not isinstance(section, dict):
            continue
        heading = strip_and_normalize(str(section.get("heading") or ""))
        if not heading:
            heading = f"{focus_keyword} topic {i + 1}" if language == "en" else f"{focus_keyword} à¦¬à¦¿à¦·à§Ÿ {i + 1}"
        if heading_hits < 2:
            heading = ensure_keyword_in_heading(heading, focus_keyword)
            heading_hits += 1
        parts.append(f"<h2>{escape(heading)}</h2>")

        paras = section.get("paragraphs")
        if not isinstance(paras, list):
            paras = []
        for para in paras:
            ptxt = strip_and_normalize(str(para))
            if ptxt:
                parts.append(f"<p>{escape(ptxt)}</p>")

        if link_idx < len(links):
            block = links[link_idx : min(link_idx + 2, len(links))]
            link_idx += len(block)
            anchors = " | ".join(
                f'<a href="{escape(path_from_url(item["canonical_url"]))}">{escape(item["title"])}</a>'
                for item in block
            )
            if anchors:
                lead = "à¦†à¦°à¦“ à¦ªà§œà§à¦¨" if language == "bn" else "Read more"
                parts.append(f"<p>{lead}: {anchors}</p>")

    label = "à¦¸à¦®à§à¦ªà¦°à§à¦•à¦¿à¦¤ à¦…à¦¤à¦¿à¦°à¦¿à¦•à§à¦¤ à¦•à§€à¦“à¦¯à¦¼à¦¾à¦°à§à¦¡" if language == "bn" else "Related Additional Keywords"
    parts.append(f"<h2>{escape(focus_keyword)} {label}</h2>")
    parts.append("<ul>")
    for kw in additional_keywords:
        parts.append(f"<li>{escape(kw)}</li>")
    parts.append("</ul>")

    parts.append("<h2>Related Reading</h2>")
    parts.append("<ul>")
    for link in links:
        parts.append(
            f'<li><a href="{escape(path_from_url(link["canonical_url"]))}">{escape(link["title"])}</a></li>'
        )
    parts.append("</ul>")

    faq = payload.get("faq")
    if isinstance(faq, list) and faq:
        parts.append("<h2>à¦¸à¦¾à¦§à¦¾à¦°à¦£ à¦œà¦¿à¦œà§à¦žà¦¾à¦¸à¦¾</h2>" if language == "bn" else "<h2>Frequently Asked Questions</h2>")
        for item in faq:
            if not isinstance(item, dict):
                continue
            q = strip_and_normalize(str(item.get("q") or ""))
            a = strip_and_normalize(str(item.get("a") or ""))
            if q:
                parts.append(f"<h3>{escape(q)}</h3>")
            if a:
                parts.append(f"<p>{escape(a)}</p>")

    if language == "bn":
        parts.append("<h2>à¦®à§‡à¦¡à¦¿à¦•à§‡à¦² à¦¡à¦¿à¦¸à¦•à§à¦²à§‡à¦‡à¦®à¦¾à¦°</h2>")
        parts.append(
            "<p>à¦à¦‡ à¦•à¦¨à¦Ÿà§‡à¦¨à§à¦Ÿ à¦¶à§à¦§à§à¦®à¦¾à¦¤à§à¦° à¦¤à¦¥à§à¦¯à§‡à¦° à¦œà¦¨à§à¦¯à¥¤ à¦à¦Ÿà¦¿ à¦¬à§à¦¯à¦•à§à¦¤à¦¿à¦—à¦¤ à¦šà¦¿à¦•à¦¿à§Žà¦¸à¦¾ à¦ªà¦°à¦¾à¦®à¦°à§à¦¶, à¦°à§‹à¦— à¦¨à¦¿à¦°à§à¦£à¦¯à¦¼ à¦¬à¦¾ à¦ªà§à¦°à§‡à¦¸à¦•à§à¦°à¦¿à¦ªà¦¶à¦¨à§‡à¦° à¦¬à¦¿à¦•à¦²à§à¦ª à¦¨à¦¯à¦¼à¥¤ à¦œà¦Ÿà¦¿à¦² à¦¬à¦¾ à¦œà¦°à§à¦°à¦¿ à¦¸à¦®à¦¸à§à¦¯à¦¾ à¦¹à¦²à§‡ à¦¨à¦¿à¦¬à¦¨à§à¦§à¦¿à¦¤ à¦šà¦¿à¦•à¦¿à§Žà¦¸à¦•à§‡à¦° à¦¸à¦¾à¦¥à§‡ à¦¯à§‹à¦—à¦¾à¦¯à§‹à¦— à¦•à¦°à§à¦¨à¥¤</p>"
        )
    else:
        parts.append("<h2>Medical Disclaimer</h2>")
        parts.append(
            "<p>This content is informational only and not a substitute for professional medical advice, diagnosis, or treatment. Consult a licensed clinician for personalized care.</p>"
        )
    return "\n".join(parts)


def validate_generated_content(
    title: str,
    article_html: str,
    focus_keyword: str,
    additional_keywords: list[str],
    language: str,
) -> tuple[bool, list[str]]:
    issues = []
    soup = BeautifulSoup(article_html, "html.parser")
    text = strip_and_normalize(soup.get_text(" ", strip=True))

    total_words = word_count(text)
    if total_words < 1500:
        issues.append(f"Word count too low: {total_words} (<1500)")

    focus_hits = keyword_count(title + " " + text, focus_keyword)
    if focus_hits < 5:
        issues.append(f"Focus keyword count too low: {focus_hits} (<5)")

    if focus_keyword.casefold() not in title.casefold():
        issues.append("H1/title missing focus keyword")

    heading_hits = 0
    for h in soup.select("h2,h3"):
        if focus_keyword.casefold() in h.get_text(" ", strip=True).casefold():
            heading_hits += 1
    if heading_hits < 2:
        issues.append(f"Heading keyword usage too low: {heading_hits} (<2)")

    if len(additional_keywords) != 5:
        issues.append(f"Additional keyword count invalid: {len(additional_keywords)}")

    link_count = len(
        [
            a
            for a in soup.select("a[href]")
            if str(a.get("href", "")).startswith("/") or str(a.get("href", "")).startswith(SITE_BASE_URL)
        ]
    )
    if link_count < 5:
        issues.append(f"Internal links too low: {link_count} (<5)")

    low = text.casefold()
    if "disclaimer" not in low and "à¦¡à¦¿à¦¸à¦•à§à¦²à§‡à¦‡à¦®à¦¾à¦°" not in low:
        issues.append("Disclaimer section missing")

    ratio = language_ratio(text, language)
    if language == "bn" and ratio < 0.20:
        issues.append(f"Bengali ratio too low: {ratio:.2f}")
    if language == "en" and ratio < 0.55:
        issues.append(f"English ratio too low: {ratio:.2f}")

    return (len(issues) == 0, issues)


def update_json_ld(
    soup: BeautifulSoup,
    canonical_url: str,
    title: str,
    meta_description: str,
    published_iso: str,
    modified_iso: str,
    category_name: str,
) -> None:
    script = soup.find("script", attrs={"class": "rank-math-schema"})
    if script is None:
        return
    raw = script.string or script.get_text()
    if not raw:
        return
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return
    graph = payload.get("@graph")
    if not isinstance(graph, list):
        return
    web_id = f"{canonical_url}#webpage"
    rich_id = f"{canonical_url}#richSnippet"

    for item in graph:
        if not isinstance(item, dict):
            continue
        typ = item.get("@type")
        if typ in {"WebPage", "CollectionPage"}:
            item["@type"] = "WebPage"
            item["@id"] = web_id
            item["url"] = canonical_url
            item["name"] = f"{title} - {SITE_NAME}"
            item["datePublished"] = published_iso
            item["dateModified"] = modified_iso
        if typ == "BlogPosting":
            item["@id"] = rich_id
            item["headline"] = f"{title} - {SITE_NAME}"
            item["name"] = f"{title} - {SITE_NAME}"
            item["description"] = meta_description
            item["datePublished"] = published_iso
            item["dateModified"] = modified_iso
            item["articleSection"] = category_name
            item["mainEntityOfPage"] = {"@id": web_id}
            item["isPartOf"] = {"@id": web_id}
    script.string = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def build_post_page_html(
    post_template_html: str,
    *,
    post_id: int,
    title: str,
    meta_description: str,
    canonical_url: str,
    article_html: str,
    category_name: str,
    published_iso: str,
    modified_iso: str,
    focus_keyword: str,
    language: str,
) -> str:
    soup = BeautifulSoup(post_template_html, "html.parser")
    if soup.title:
        soup.title.string = f"{title} - {SITE_NAME}"

    upsert_meta(soup, "name", "description", meta_description)
    upsert_meta(soup, "name", "robots", "index, follow")
    upsert_meta(soup, "property", "og:type", "article")
    upsert_meta(soup, "property", "og:title", f"{title} - {SITE_NAME}")
    upsert_meta(soup, "property", "og:description", meta_description)
    upsert_meta(soup, "property", "og:url", canonical_url)
    upsert_meta(soup, "property", "article:section", category_name)
    upsert_meta(soup, "property", "article:published_time", published_iso)
    upsert_meta(soup, "property", "article:modified_time", modified_iso)
    upsert_meta(soup, "name", "twitter:title", f"{title} - {SITE_NAME}")
    upsert_meta(soup, "name", "twitter:description", meta_description)
    upsert_meta(soup, "name", "x-ai-generated", "true")
    upsert_meta(soup, "name", "x-focus-keyword", focus_keyword)
    upsert_meta(soup, "name", "x-content-language", language)
    upsert_link_rel(soup, "canonical", canonical_url)

    update_json_ld(soup, canonical_url, title, meta_description, published_iso, modified_iso, category_name)

    article = soup.select_one("article.entry.single-entry")
    if article:
        article["id"] = f"post-{post_id}"
        classes = [str(c) for c in (article.get("class") or [])]
        classes = [c for c in classes if not re.match(r"^post-\d+$", c)]
        classes.insert(0, f"post-{post_id}")
        if "type-post" not in classes:
            classes.append("type-post")
        article["class"] = classes

    h1 = soup.select_one("h1.entry-title")
    if h1:
        h1.clear()
        h1.append(title)

    content_div = soup.select_one("div.entry-content.single-content")
    if content_div is None:
        raise RuntimeError("Template missing div.entry-content.single-content")
    content_div.clear()
    fragment = BeautifulSoup(article_html, "html.parser")
    for child in list(fragment.contents):
        content_div.append(child)
    return str(soup)


def render_archive_item_html(record: dict[str, Any]) -> str:
    canonical = str(record["canonical_url"])
    href = path_from_url(canonical)
    title = strip_and_normalize(record["title"])
    excerpt = strip_and_normalize(record["excerpt"])
    if len(excerpt) > 280:
        excerpt = excerpt[:277].rstrip() + "..."
    post_id = int(record.get("post_id") or 0)
    return (
        '<li class="entry-list-item">\n'
        f'  <article class="entry content-bg loop-entry post-{post_id} post type-post status-publish format-standard hentry">\n'
        '    <div class="entry-content-wrap">\n'
        '      <header class="entry-header">\n'
        f'        <h2 class="entry-title"><a href="{escape(href)}" rel="bookmark">{escape(title)}</a></h2>'
        "</header>\n"
        f'      <div class="entry-summary"><p>{escape(excerpt)}</p></div>\n'
        "      <footer class=\"entry-footer\"></footer>\n"
        "    </div>\n"
        "  </article>\n"
        "</li>"
    )


def build_pagination_nav_html(current: int, total_pages: int) -> str:
    if total_pages <= 1:
        return ""
    lines = ['<nav class="navigation pagination" aria-label="Posts">', '  <h2 class="screen-reader-text">Posts navigation</h2>', '  <div class="nav-links">']
    if current > 1:
        prev_href = "/" if current - 1 == 1 else f"/page/{current - 1}/"
        lines.append(f'    <a class="prev page-numbers" href="{prev_href}">Previous</a>')
    for i in range(1, total_pages + 1):
        href = "/" if i == 1 else f"/page/{i}/"
        if i == current:
            lines.append(f'    <span aria-current="page" class="page-numbers current">{i}</span>')
        else:
            lines.append(f'    <a class="page-numbers" href="{href}">{i}</a>')
    if current < total_pages:
        lines.append(f'    <a class="next page-numbers" href="/page/{current + 1}/">Next</a>')
    lines.extend(["  </div>", "</nav>"])
    return "\n".join(lines)


def replace_archive_block(soup: BeautifulSoup, records: list[dict[str, Any]]) -> None:
    container = soup.select_one("#archive-container")
    if container is None:
        main = soup.select_one("#main.site-main") or soup.select_one("#main")
        if main is None:
            raise RuntimeError("Archive template missing #main")
        container = soup.new_tag("ul", attrs={"id": "archive-container", "class": "content-wrap kadence-posts-list post-archive"})
        main.append(container)
    container.clear()
    for record in records:
        frag = BeautifulSoup(render_archive_item_html(record), "html.parser")
        item = frag.select_one("li.entry-list-item")
        if item:
            container.append(item)


def upsert_pagination_nav(soup: BeautifulSoup, current: int, total_pages: int) -> None:
    old = soup.select_one("nav.navigation.pagination")
    html = build_pagination_nav_html(current, total_pages)
    if not html:
        if old:
            old.decompose()
        return
    new_nav = BeautifulSoup(html, "html.parser").select_one("nav.navigation.pagination")
    if not new_nav:
        return
    if old:
        old.replace_with(new_nav)
    else:
        anchor = soup.select_one("#archive-container")
        if anchor:
            anchor.insert_after(new_nav)


def update_archive_head(soup: BeautifulSoup, page_no: int, total_pages: int) -> None:
    canonical = SITE_BASE_URL + ("/" if page_no == 1 else f"/page/{page_no}/")
    upsert_link_rel(soup, "canonical", canonical)
    remove_link_rel(soup, "prev")
    remove_link_rel(soup, "next")
    if page_no > 1:
        prev = SITE_BASE_URL + ("/" if page_no - 1 == 1 else f"/page/{page_no - 1}/")
        upsert_link_rel(soup, "prev", prev)
    if page_no < total_pages:
        upsert_link_rel(soup, "next", SITE_BASE_URL + f"/page/{page_no + 1}/")
    upsert_meta(soup, "property", "og:url", canonical)
    if page_no == 1:
        title = f"{SITE_NAME} - {SITE_TAGLINE}"
    else:
        title = f"{SITE_NAME} - Page {page_no} of {total_pages} - {SITE_TAGLINE}"
    if soup.title:
        soup.title.string = title
    upsert_meta(soup, "property", "og:title", title)
    upsert_meta(soup, "name", "twitter:title", title)


def rebuild_home_and_pagination(records: list[dict[str, Any]], home_template_html: str, page_template_html: str) -> None:
    ordered = sort_records_desc(records)
    total_pages = max(1, math.ceil(len(ordered) / MAX_ARCHIVE_ITEMS_PER_PAGE))
    for page_no in range(1, total_pages + 1):
        start = (page_no - 1) * MAX_ARCHIVE_ITEMS_PER_PAGE
        page_records = ordered[start : start + MAX_ARCHIVE_ITEMS_PER_PAGE]
        soup = BeautifulSoup(home_template_html if page_no == 1 else page_template_html, "html.parser")
        replace_archive_block(soup, page_records)
        upsert_pagination_nav(soup, page_no, total_pages)
        update_archive_head(soup, page_no, total_pages)
        target = DIST_DIR / "index.html" if page_no == 1 else DIST_DIR / "page" / str(page_no) / "index.html"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(soup), encoding="utf-8")

    page_root = DIST_DIR / "page"
    if page_root.exists():
        for d in page_root.iterdir():
            if d.is_dir() and d.name.isdigit() and (int(d.name) < 2 or int(d.name) > total_pages):
                shutil.rmtree(d, ignore_errors=True)


def build_category_page_html(template_html: str, category_name: str, category_slug: str, records: list[dict[str, Any]]) -> str:
    soup = BeautifulSoup(template_html, "html.parser")
    canonical = f"{SITE_BASE_URL}/category/{quote(category_slug, safe='-')}/"
    if soup.title:
        soup.title.string = f"{category_name} - {SITE_NAME}"
    upsert_meta(soup, "name", "robots", "index, follow")
    upsert_meta(soup, "property", "og:type", "website")
    upsert_meta(soup, "property", "og:title", f"{category_name} - {SITE_NAME}")
    upsert_meta(soup, "property", "og:url", canonical)
    upsert_meta(soup, "name", "twitter:title", f"{category_name} - {SITE_NAME}")
    upsert_link_rel(soup, "canonical", canonical)
    remove_link_rel(soup, "prev")
    remove_link_rel(soup, "next")

    main = soup.select_one("#main.site-main") or soup.select_one("#main")
    if main is None:
        raise RuntimeError("Category template missing #main")
    main.clear()
    header = soup.new_tag("header", attrs={"class": "entry-header post-archive-title"})
    h1 = soup.new_tag("h1", attrs={"class": "page-title archive-title"})
    h1.string = category_name
    header.append(h1)
    main.append(header)

    if records:
        lst = soup.new_tag("ul", attrs={"id": "archive-container", "class": "content-wrap kadence-posts-list post-archive"})
        for rec in records[:MAX_ARCHIVE_ITEMS_PER_PAGE]:
            frag = BeautifulSoup(render_archive_item_html(rec), "html.parser")
            item = frag.select_one("li.entry-list-item")
            if item:
                lst.append(item)
        main.append(lst)
    else:
        p = soup.new_tag("p")
        p.string = "No posts available in this category yet."
        main.append(p)
    return str(soup)


def write_category_pages(records: list[dict[str, Any]], category_template_html: str) -> None:
    grouped = {}
    all_categories = [*CATEGORY_RULES, FALLBACK_CATEGORY]
    for cat in all_categories:
        grouped[cat["name"]] = []
    for rec in sort_records_desc(records):
        cname = str(rec.get("category") or FALLBACK_CATEGORY["name"])
        grouped.setdefault(cname, [])
        grouped[cname].append(rec)

    for cat in all_categories:
        html = build_category_page_html(category_template_html, cat["name"], cat["slug"], grouped.get(cat["name"], []))
        for out in url_path_to_output_paths(f"/category/{quote(cat['slug'], safe='-')}/"):
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(html, encoding="utf-8")


def generate_keyword_content(
    keyword: str,
    language: str,
    category_name: str,
    api_key: str,
    model: str,
    timeout_seconds: int,
    record_pool: list[dict[str, Any]],
    logger: logging.Logger,
) -> tuple[str, str, str, list[str], str]:
    hints = select_internal_links(record_pool, keyword, [], limit=8)
    prompt = build_prompt(keyword, language, category_name, hints)
    raw = openai_generate_json(api_key=api_key, model=model, prompt=prompt, timeout_seconds=timeout_seconds)
    payload = extract_json_object(raw)

    title = ensure_keyword_in_title(strip_and_normalize(str(payload.get("title") or "")), keyword)
    if not title:
        title = f"{keyword}: à¦•à¦¾à¦°à¦£, à¦²à¦•à§à¦·à¦£, à¦šà¦¿à¦•à¦¿à§Žà¦¸à¦¾ à¦“ à¦ªà§à¦°à¦¤à¦¿à¦•à¦¾à¦°" if language == "bn" else f"{keyword}: Causes, Symptoms, Treatment and Prevention"

    additional = sanitize_additional_keywords(payload.get("additional_keywords"), keyword, language)
    links = select_internal_links(record_pool, keyword, additional, limit=5)
    if len(links) < 5:
        logger.info("Internal link candidates available: %d for `%s`", len(links), keyword)

    meta_description = strip_and_normalize(str(payload.get("meta_description") or ""))
    if keyword.casefold() not in meta_description.casefold():
        meta_description = (
            f"{keyword} à¦¸à¦®à§à¦ªà¦°à§à¦•à§‡ à¦•à¦¾à¦°à¦£, à¦²à¦•à§à¦·à¦£, à¦šà¦¿à¦•à¦¿à§Žà¦¸à¦¾, à¦ªà§à¦°à¦¤à¦¿à¦°à§‹à¦§ à¦“ à¦¨à¦¿à¦°à¦¾à¦ªà¦¦ à¦ªà¦°à¦¾à¦®à¦°à§à¦¶à§‡à¦° à¦ªà§‚à¦°à§à¦£à¦¾à¦™à§à¦— à¦—à¦¾à¦‡à¦¡à¥¤"
            if language == "bn"
            else f"{keyword}: complete guide covering causes, symptoms, treatment, prevention, and safe care advice."
        )
    meta_description = meta_description[:158]

    excerpt = strip_and_normalize(str(payload.get("excerpt") or "")) or meta_description
    article_html = build_article_html(payload, keyword, language, additional, links)
    return title, meta_description, excerpt, additional, article_html


def compute_next_post_id(records: list[dict[str, Any]], indexed_max: int) -> int:
    best = indexed_max
    for rec in records:
        try:
            best = max(best, int(rec.get("post_id") or 0))
        except (TypeError, ValueError):
            pass
    return best + 1


def write_post_outputs(path: str, html: str) -> None:
    for out in url_path_to_output_paths(path):
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(html, encoding="utf-8")


def build_record_pool(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    uniq = {}
    for rec in records:
        canon = str(rec.get("canonical_url") or "").strip()
        if canon:
            uniq[canon] = rec
    return sort_records_desc(list(uniq.values()))


def run_git_command(args: list[str], logger: logging.Logger) -> str:
    proc = subprocess.run(args, cwd=REPO_ROOT, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(args)}\n{(proc.stderr or '').strip()}")
    out = (proc.stdout or "").strip()
    if out:
        logger.info(out)
    return out


def auto_commit_and_push(generated_count: int, do_push: bool, logger: logging.Logger) -> None:
    run_git_command(["git", "add", "--", "dist", "data/content_pipeline_state.json", "data/posts_manifest.json"], logger)
    status = run_git_command(["git", "status", "--porcelain"], logger)
    if not status:
        logger.info("No staged changes to commit")
        return
    msg = f"content: publish {generated_count} posts on {datetime.now(ZoneInfo(DEFAULT_TIMEZONE)).date().isoformat()}"
    run_git_command(["git", "commit", "-m", msg], logger)
    if do_push:
        run_git_command(["git", "push", "origin", "main"], logger)


def generate_and_publish(
    count: int,
    max_retries_per_keyword: int,
    timezone_name: str,
    openai_model: str,
    openai_fallback_model: str,
    openai_timeout: int,
    relaxed_validation: bool,
    auto_commit: bool,
    auto_push: bool,
    dry_run: bool,
    logger: logging.Logger,
) -> int:
    if not DIST_DIR.exists():
        raise RuntimeError("dist directory not found")

    templates = find_templates()
    post_template_html = templates["post"].read_text(encoding="utf-8", errors="ignore")
    home_template_html = templates["home"].read_text(encoding="utf-8", errors="ignore")
    page_template_html = templates["page"].read_text(encoding="utf-8", errors="ignore")
    category_template_html = templates["category"].read_text(encoding="utf-8", errors="ignore")

    indexed_records, indexed_max_post_id = index_existing_posts(logger)
    manifest = merge_manifest(indexed_records, load_manifest())
    state = load_state()
    if state.get("next_post_id") is None:
        state["next_post_id"] = compute_next_post_id(manifest, indexed_max_post_id)
    else:
        state["next_post_id"] = max(int(state["next_post_id"]), indexed_max_post_id + 1)

    keywords = parse_keywords_csv(KWS_FILE)
    cursor = int(state.get("cursor") or 0)
    if cursor >= len(keywords):
        logger.warning("Keywords exhausted at cursor %d/%d", cursor, len(keywords))
        return 0

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Set environment variable and retry.")

    logger.info("Using OpenAI model=%s fallback=%s", openai_model, openai_fallback_model or "-")

    used_slugs = {str(rec.get("slug") or "").strip() for rec in manifest if rec.get("slug")}
    generated = 0
    skipped = 0
    record_pool = build_record_pool(manifest)

    while cursor < len(keywords) and generated < count:
        keyword = keywords[cursor].strip()
        cursor += 1
        if not keyword:
            skipped += 1
            continue
        language = infer_language(keyword)
        category = map_category(keyword)
        models = choose_models(openai_model, openai_fallback_model)

        success = False
        last_error = "unknown"
        for attempt in range(max_retries_per_keyword):
            model = models[attempt % len(models)]
            logger.info("Keyword `%s` attempt %d/%d model=%s", keyword, attempt + 1, max_retries_per_keyword, model)
            try:
                title, meta_description, excerpt, additional, article_html = generate_keyword_content(
                    keyword=keyword,
                    language=language,
                    category_name=category["name"],
                    api_key=api_key,
                    model=model,
                    timeout_seconds=openai_timeout,
                    record_pool=record_pool,
                    logger=logger,
                )
                ok, issues = validate_generated_content(title, article_html, keyword, additional, language)
                if not ok:
                    last_error = "; ".join(issues)
                    if relaxed_validation:
                        logger.warning(
                            "Validation failed for `%s` but continuing due --relaxed-validation: %s",
                            keyword,
                            last_error,
                        )
                    else:
                        logger.warning("Validation failed for `%s`: %s", keyword, last_error)
                        continue

                slug = unique_slug(slugify(keyword), used_slugs)
                encoded_slug = quote(slug, safe="-")
                canonical_url = f"{SITE_BASE_URL}/{encoded_slug}/"
                publish_iso = now_iso(timezone_name)
                post_id = int(state["next_post_id"])
                state["next_post_id"] = post_id + 1

                post_html = build_post_page_html(
                    post_template_html,
                    post_id=post_id,
                    title=title,
                    meta_description=meta_description,
                    canonical_url=canonical_url,
                    article_html=article_html,
                    category_name=category["name"],
                    published_iso=publish_iso,
                    modified_iso=publish_iso,
                    focus_keyword=keyword,
                    language=language,
                )
                rec = normalize_record(
                    {
                        "slug": slug,
                        "canonical_url": canonical_url,
                        "title": title,
                        "excerpt": excerpt,
                        "language": language,
                        "focus_keyword": keyword,
                        "additional_keywords": additional,
                        "category": category["name"],
                        "published_at": publish_iso,
                        "source": "generated",
                        "post_id": post_id,
                    }
                )
                if not dry_run:
                    write_post_outputs(f"/{encoded_slug}/", post_html)
                manifest = [r for r in manifest if r["canonical_url"] != canonical_url]
                manifest.append(rec)
                record_pool = build_record_pool(manifest)
                generated += 1
                success = True
                logger.info("Generated %d/%d: %s", generated, count, canonical_url)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                logger.warning("Generation error for `%s`: %s", keyword, last_error)

        if not success:
            skipped += 1
            logger.error("Skipping keyword `%s`: %s", keyword, last_error)

    if not dry_run:
        state["cursor"] = cursor
        state["last_run"] = now_iso(timezone_name)
        manifest = sort_records_desc(manifest)
        save_manifest(manifest)
        save_state(state)

        rebuild_home_and_pagination(manifest, home_template_html, page_template_html)
        write_category_pages(manifest, category_template_html)
        sitemap_urls, _ = generate_seo_files(DIST_DIR, SITE_BASE_URL)
        logger.info("SEO regenerated. sitemap urls=%d", sitemap_urls)

        if generated > 0 and auto_commit:
            auto_commit_and_push(generated, do_push=auto_push, logger=logger)
    else:
        logger.info("Dry-run complete. No files were written.")

    logger.info("Summary: generated=%d skipped=%d cursor=%d/%d", generated, skipped, cursor, len(keywords))
    return generated


def rebuild_index_only(timezone_name: str, logger: logging.Logger) -> None:
    templates = find_templates()
    home_template_html = templates["home"].read_text(encoding="utf-8", errors="ignore")
    page_template_html = templates["page"].read_text(encoding="utf-8", errors="ignore")
    category_template_html = templates["category"].read_text(encoding="utf-8", errors="ignore")

    indexed_records, indexed_max_post_id = index_existing_posts(logger)
    manifest = sort_records_desc(merge_manifest(indexed_records, load_manifest()))
    save_manifest(manifest)

    state = load_state()
    state["next_post_id"] = compute_next_post_id(manifest, indexed_max_post_id)
    state["last_run"] = now_iso(timezone_name)
    save_state(state)

    rebuild_home_and_pagination(manifest, home_template_html, page_template_html)
    write_category_pages(manifest, category_template_html)
    sitemap_urls, _ = generate_seo_files(DIST_DIR, SITE_BASE_URL)
    logger.info("Rebuild completed. sitemap urls=%d", sitemap_urls)


def main() -> int:
    args = parse_args()
    logger = setup_logging()
    try:
        if args.command == "run":
            generate_and_publish(
                count=args.count,
                max_retries_per_keyword=args.max_retries_per_keyword,
                timezone_name=args.timezone,
                openai_model=args.openai_model,
                openai_fallback_model=args.openai_fallback_model,
                openai_timeout=args.openai_timeout,
                relaxed_validation=args.relaxed_validation,
                auto_commit=args.auto_commit,
                auto_push=args.auto_push,
                dry_run=False,
                logger=logger,
            )
            return 0
        if args.command == "run-daily":
            generate_and_publish(
                count=args.count,
                max_retries_per_keyword=args.max_retries_per_keyword,
                timezone_name=args.timezone,
                openai_model=args.openai_model,
                openai_fallback_model=args.openai_fallback_model,
                openai_timeout=args.openai_timeout,
                relaxed_validation=args.relaxed_validation,
                auto_commit=True,
                auto_push=True,
                dry_run=False,
                logger=logger,
            )
            return 0
        if args.command == "dry-run":
            generate_and_publish(
                count=args.count,
                max_retries_per_keyword=args.max_retries_per_keyword,
                timezone_name=args.timezone,
                openai_model=args.openai_model,
                openai_fallback_model=args.openai_fallback_model,
                openai_timeout=args.openai_timeout,
                relaxed_validation=args.relaxed_validation,
                auto_commit=False,
                auto_push=False,
                dry_run=True,
                logger=logger,
            )
            return 0
        if args.command == "rebuild-index":
            rebuild_index_only(args.timezone, logger)
            return 0
        logger.error("Unknown command")
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
