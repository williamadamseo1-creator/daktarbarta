#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sys
from html import escape
from pathlib import Path
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ai_content_publisher as pub


BN_RE = re.compile(r"[\u0980-\u09FF]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate manual static posts from kws.csv")
    parser.add_argument("--start", type=int, required=True, help="1-based keyword index")
    parser.add_argument("--count", type=int, required=True, help="Number of keywords to generate")
    return parser.parse_args()


def read_keywords(path: Path) -> list[str]:
    rows: list[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = (row.get("kws") or "").strip()
            if kw:
                rows.append(kw)
    return rows


def infer_lang(keyword: str) -> str:
    return "bn" if BN_RE.search(keyword) else "en"


def clean_anchor_text(title: str, href: str, lang: str) -> str:
    bad = ("???" in title) or (title.count("?") >= 4)
    if not bad:
        return title
    lower = href.lower()
    if "r41" in lower:
        return "R41 বিষয়ে বিস্তারিত পোস্ট" if lang == "bn" else "Detailed post about R41"
    if "ptk-75" in lower or "sabal-pentarkan" in lower:
        return "PTK 75 বিষয়ে বিস্তারিত পোস্ট" if lang == "bn" else "Detailed post about PTK 75"
    return "বিস্তারিত পোস্ট" if lang == "bn" else "Detailed related post"


def bn_sections(keyword: str) -> list[tuple[str, str, str]]:
    return [
        (
            f"{keyword} কী এবং কেন এই বিষয়ে সচেতনতা দরকার",
            f"{keyword} সাধারণত উপসর্গভিত্তিক চিকিৎসা আলোচনায় আসে। তবে শুধু নাম দেখে ওষুধ শুরু করা নিরাপদ নয়। রোগীর সমস্যা কতদিনের, কোন ট্রিগারে বাড়ে, আগের চিকিৎসায় কী ফল হয়েছে এবং বর্তমান শারীরিক অবস্থা কেমন, এসব বিবেচনা করেই সিদ্ধান্ত নিতে হয়। একই ওষুধ সবাইকে একইভাবে উপকার দেয় না।",
            f"{keyword} নিয়ে ইন্টারনেটে অনেক অসম্পূর্ণ তথ্য থাকে। তাই নির্ভরযোগ্য পদ্ধতি হলো ক্লিনিক্যাল মূল্যায়নের ভিত্তিতে পরিকল্পনা করা। চিকিৎসার শুরুতেই লক্ষ্য ঠিক করুন: উপসর্গ কমানো, ঘুম ভালো করা, বা জীবনযাত্রার মান উন্নত করা। লক্ষ্য স্পষ্ট থাকলে ফলাফল মূল্যায়ন সহজ হয়।",
        ),
        (
            f"{keyword} ব্যবহার শুরুর আগে রোগীর চেকলিস্ট",
            "অ্যালার্জি, গ্যাস্ট্রিক সমস্যা, কিডনি-লিভার রোগ, রক্তচাপ বা ডায়াবেটিসের ওষুধ চলমান আছে কি না, এসব তথ্য চিকিৎসককে আগে জানানো জরুরি। অনেক রোগী এ তথ্য না দিলে পরে পার্শ্বপ্রতিক্রিয়া হলে কারণ বোঝা কঠিন হয়।",
            f"{keyword} শুরু করার আগে বেসলাইন নোট রাখুন: নাক/গলা/শ্বাসের উপসর্গ, ব্যথার মাত্রা, ঘুম, দুর্বলতা, এবং দৈনন্দিন কাজের সক্ষমতা। ১-২ সপ্তাহ পরে একই সূচকে তুলনা করলে বোঝা যায় চিকিৎসা কার্যকর হচ্ছে কি না।",
        ),
        (
            f"{keyword} খাওয়ার নিয়ম ও ডোজ শৃঙ্খলা",
            f"{keyword} নির্ধারিত সময়ে নিয়মিত খাওয়া সবচেয়ে গুরুত্বপূর্ণ। ডোজ মিস হলে পরেরবার দ্বিগুণ ডোজ নেওয়া উচিত নয়। অনেকেই দ্রুত ফলের আশায় ডোজ বাড়িয়ে দেন, এতে উপকারের বদলে বিভ্রান্তি তৈরি হতে পারে।",
            "যদি আংশিক উপকার হয়, নিজে নিজে ওষুধ বন্ধ না করে ফলো-আপে চিকিৎসকের সাথে পরিকল্পনা আপডেট করুন। একইসাথে একাধিক নতুন ওষুধ শুরু করলে কোনটি কাজ করছে তা বোঝা কঠিন হয়। তাই ধাপে ধাপে পরিবর্তনই নিরাপদ পদ্ধতি।",
        ),
        (
            f"{keyword} এর সম্ভাব্য পার্শ্বপ্রতিক্রিয়া",
            "কিছু ক্ষেত্রে হালকা গ্যাস্ট্রিক অস্বস্তি, মাথা হালকা লাগা, ঘুমের রুটিন পরিবর্তন বা প্রাথমিক উপসর্গে সাময়িক ওঠানামা হতে পারে। এসব লক্ষণ দীর্ঘস্থায়ী হলে চিকিৎসককে জানান।",
            f"তীব্র র‍্যাশ, শ্বাসকষ্ট, বুকব্যথা, অবিরাম বমি, অতিরিক্ত দুর্বলতা বা অস্বাভাবিক মাথা ঘোরা হলে {keyword} চালিয়ে না গিয়ে দ্রুত চিকিৎসা নিন। জরুরি লক্ষণকে কখনও অবহেলা করা উচিত নয়।",
        ),
        (
            f"{keyword} এর সাথে জীবনযাপনের পরিবর্তন",
            "ওষুধের পাশাপাশি পর্যাপ্ত ঘুম, নিয়মিত পানি পান, ধূমপান পরিহার, অতিরিক্ত ঝাল-তেল কমানো, এবং মানসিক চাপ নিয়ন্ত্রণ উপসর্গ নিয়ন্ত্রণে সহায়ক। চিকিৎসা কেবল ট্যাবলেট-কেন্দ্রিক হলে ফল স্থায়ী নাও হতে পারে।",
            f"{keyword} ব্যবহারকারী রোগীদের জন্য ট্রিগার ডায়েরি রাখা উপকারী: কোন খাবার/পরিবেশ/অভ্যাসে সমস্যা বাড়ে তা লিখে রাখলে ভবিষ্যৎ রিল্যাপ্স কমানো যায়।",
        ),
        (
            f"{keyword} নিয়ে প্রচলিত ভুল ধারণা ও সঠিক দৃষ্টিভঙ্গি",
            f"অনেকে ভাবেন {keyword} দীর্ঘদিন নিজের ইচ্ছেমতো খাওয়া নিরাপদ। বাস্তবে দীর্ঘমেয়াদি যেকোনো চিকিৎসায় পর্যবেক্ষণ জরুরি। উপসর্গ না কমলে রোগনির্ণয় পুনর্মূল্যায়ন দরকার হতে পারে।",
            "সোশ্যাল মিডিয়া বা পরিচিতজনের পরামর্শে ডোজ বদলানো ঝুঁকিপূর্ণ। আপনার ক্লিনিক্যাল অবস্থা অনুযায়ী ব্যক্তিগত চিকিৎসা পরিকল্পনাই সবচেয়ে নিরাপদ এবং কার্যকর।",
        ),
    ]


def en_sections(keyword: str) -> list[tuple[str, str, str]]:
    return [
        (
            f"{keyword}: overview and practical context",
            f"{keyword} is often discussed in symptom-based treatment plans. Still, medication decisions should be based on clinical assessment, not product name alone. Baseline symptoms, duration, trigger patterns, and prior treatment response should be reviewed before starting.",
            f"Patients should set realistic goals with {keyword}: symptom reduction, improved sleep, fewer flare-ups, and better daily function. Clear goals make follow-up decisions evidence-driven rather than emotional.",
        ),
        (
            f"How to start {keyword} safely",
            "Share complete medication history, allergy profile, and chronic conditions with your clinician. Hidden interactions are a common cause of avoidable adverse effects.",
            f"Use {keyword} exactly as advised, at consistent times, and avoid unsupervised dose escalation. If a dose is missed, do not double the next one.",
        ),
        (
            f"Expected response and monitoring while using {keyword}",
            "Some users notice gradual improvement, while others may see little early change. Objective monitoring is essential: symptom score, sleep quality, trigger exposure, and functional limits should be logged.",
            "Without structured tracking, treatment decisions become guesswork. A short follow-up interval usually gives better safety and clarity.",
        ),
        (
            f"Potential side effects of {keyword}",
            "Mild issues may include temporary gastrointestinal discomfort, headache, sleep disturbance, or transient symptom fluctuation. Persistent symptoms should be reported.",
            f"Stop {keyword} and seek urgent care for severe allergy, breathing difficulty, chest pain, persistent vomiting, or neurological warning signs.",
        ),
        (
            f"Lifestyle support that improves {keyword} outcomes",
            "Medication alone is rarely enough. Better hydration, sleep hygiene, nutrition quality, trigger control, and stress management improve overall response.",
            f"When {keyword} is integrated with lifestyle correction and timely follow-up, relapse risk and treatment confusion are lower.",
        ),
        (
            f"Common myths around {keyword}",
            f"A frequent myth is that {keyword} can be used indefinitely without supervision. In reality, long-term use should be periodically reviewed for benefit, tolerance, and diagnosis accuracy.",
            "If expected improvement does not occur, reassessment is more useful than repeatedly extending the same regimen.",
        ),
    ]


def build_article_html(keyword: str, lang: str, links: list[dict[str, str]]) -> str:
    parts: list[str] = []
    if lang == "bn":
        intro = (
            f"{keyword} নিয়ে সঠিক সিদ্ধান্ত নিতে ব্যবহার, ডোজ, পার্শ্বপ্রতিক্রিয়া, ফলো-আপ এবং "
            "জীবনযাত্রার দিকগুলো একসাথে বুঝতে হবে। এই কনটেন্ট তথ্যভিত্তিক; ব্যক্তিগত প্রেসক্রিপশনের বিকল্প নয়।"
        )
        parts.append(f"<p>{escape(intro)}</p>")
        for h, p1, p2 in bn_sections(keyword):
            parts.append(f"<h2>{escape(h)}</h2>")
            parts.append(f"<p>{escape(p1)}</p>")
            parts.append(f"<p>{escape(p2)}</p>")
    else:
        intro = (
            f"This practical guide explains {keyword}: usage pattern, safety precautions, side effects, "
            "and structured follow-up. It is informational and not a substitute for personalized care."
        )
        parts.append(f"<p>{escape(intro)}</p>")
        for h, p1, p2 in en_sections(keyword):
            parts.append(f"<h2>{escape(h)}</h2>")
            parts.append(f"<p>{escape(p1)}</p>")
            parts.append(f"<p>{escape(p2)}</p>")

    parts.append("<h2>Related Reading</h2>")
    parts.append("<ul>")
    for item in links[:5]:
        href = pub.path_from_url(item["canonical_url"])
        label = clean_anchor_text(item["title"], href, lang)
        parts.append(f'<li><a href="{escape(href)}">{escape(label)}</a></li>')
    parts.append("</ul>")

    if lang == "bn":
        parts.append("<h2>সাধারণ প্রশ্নোত্তর</h2>")
        parts.append(f"<h3>{escape(keyword)} কতদিন চলতে পারে?</h3>")
        parts.append("<p>সমস্যার ধরন, উন্নতির গতি এবং চিকিৎসকের পর্যবেক্ষণের ওপর সময় নির্ভর করে।</p>")
        parts.append("<h3>নিজে নিজে ডোজ বাড়ানো যাবে?</h3>")
        parts.append("<p>না। অনিয়ন্ত্রিত ডোজ পরিবর্তনে ঝুঁকি বাড়তে পারে এবং চিকিৎসা মূল্যায়ন কঠিন হয়।</p>")
        parts.append("<h3>কখন জরুরি চিকিৎসা নিতে হবে?</h3>")
        parts.append("<p>শ্বাসকষ্ট, তীব্র অ্যালার্জি, বুকব্যথা বা গুরুতর নতুন উপসর্গ হলে দ্রুত চিকিৎসা নিন।</p>")
        parts.append("<h2>মেডিকেল ডিসক্লেইমার</h2>")
        parts.append(
            "<p>এই কনটেন্ট শুধুমাত্র তথ্যের উদ্দেশ্যে। এটি ব্যক্তিগত চিকিৎসা, রোগনির্ণয় বা প্রেসক্রিপশনের বিকল্প নয়। "
            "জটিল বা জরুরি সমস্যায় নিবন্ধিত চিকিৎসকের পরামর্শ নিন।</p>"
        )
    else:
        parts.append("<h2>Frequently Asked Questions</h2>")
        parts.append(f"<h3>How long should {escape(keyword)} be used?</h3>")
        parts.append("<p>Duration depends on diagnosis, response trend, and supervised follow-up findings.</p>")
        parts.append("<h3>Can I increase the dose on my own?</h3>")
        parts.append("<p>No. Unsupervised dose changes can increase risk and reduce treatment clarity.</p>")
        parts.append("<h3>When should urgent care be considered?</h3>")
        parts.append("<p>Seek urgent care for severe allergy, breathing trouble, chest pain, or persistent severe symptoms.</p>")
        parts.append("<h2>Medical Disclaimer</h2>")
        parts.append(
            "<p>This content is informational only and not a substitute for professional diagnosis or treatment. "
            "Consult a licensed clinician for personalized care.</p>"
        )
    return "\n".join(parts)


def build_title(keyword: str, lang: str) -> str:
    if lang == "bn":
        return f"{keyword}: ব্যবহার, খাওয়ার নিয়ম, পার্শ্বপ্রতিক্রিয়া ও সতর্কতা"
    return f"{keyword}: Uses, Dosage, Side Effects, and Safety Guide"


def build_meta(keyword: str, lang: str) -> str:
    if lang == "bn":
        return f"{keyword} সম্পর্কে বিস্তারিত বাংলা গাইড: ব্যবহার, ডোজ, পার্শ্বপ্রতিক্রিয়া, সতর্কতা ও নিরাপদ চিকিৎসা পরিকল্পনা।"
    return f"Complete guide to {keyword}: usage pattern, dosage discipline, side effects, precautions, and safer follow-up."


def build_excerpt(keyword: str, lang: str) -> str:
    if lang == "bn":
        return f"{keyword} নিয়ে ব্যবহারবিধি, সতর্কতা, পার্শ্বপ্রতিক্রিয়া ও ফলো-আপ কৌশল এই গাইডে বাস্তবভাবে তুলে ধরা হয়েছে।"
    return f"Practical article on {keyword} covering use, side effects, precautions, and follow-up strategy."


def main() -> None:
    args = parse_args()
    logger = pub.setup_logging()
    keywords = read_keywords(pub.KWS_FILE)
    start_idx = max(0, args.start - 1)
    end_idx = min(len(keywords), start_idx + args.count)
    batch = keywords[start_idx:end_idx]
    if not batch:
        raise RuntimeError("No keywords found for the provided start/count")

    templates = pub.find_templates()
    post_template_html = templates["post"].read_text(encoding="utf-8", errors="ignore")
    home_template_html = templates["home"].read_text(encoding="utf-8", errors="ignore")
    page_template_html = templates["page"].read_text(encoding="utf-8", errors="ignore")
    category_template_html = templates["category"].read_text(encoding="utf-8", errors="ignore")

    indexed_records, indexed_max_post_id = pub.index_existing_posts(logger)
    manifest = pub.merge_manifest(indexed_records, pub.load_manifest())
    state = pub.load_state()
    state["next_post_id"] = max(int(state.get("next_post_id") or 0), pub.compute_next_post_id(manifest, indexed_max_post_id))

    now = pub.now_iso(pub.DEFAULT_TIMEZONE)
    record_pool = pub.build_record_pool(manifest)
    updated_urls: list[str] = []

    for keyword in batch:
        lang = infer_lang(keyword)
        category = pub.map_category(keyword)
        title = build_title(keyword, lang)
        meta_description = build_meta(keyword, lang)[:158]
        excerpt = build_excerpt(keyword, lang)
        additional = pub.sanitize_additional_keywords(pub.derive_additional_keywords(keyword, lang), keyword, lang)
        links = pub.select_internal_links(record_pool, keyword, additional, limit=8)
        article_html = build_article_html(keyword, lang, links)

        slug = pub.slugify(keyword)
        encoded_slug = quote(slug, safe="-")
        canonical_url = f"{pub.SITE_BASE_URL}/{encoded_slug}/"

        existing = next((r for r in manifest if r.get("canonical_url") == canonical_url), None)
        if existing and existing.get("post_id"):
            post_id = int(existing["post_id"])
        else:
            post_id = int(state["next_post_id"])
            state["next_post_id"] = post_id + 1

        post_html = pub.build_post_page_html(
            post_template_html,
            post_id=post_id,
            title=title,
            meta_description=meta_description,
            canonical_url=canonical_url,
            article_html=article_html,
            category_name=category["name"],
            published_iso=now,
            modified_iso=now,
            focus_keyword=keyword,
            language=lang,
        )
        pub.write_post_outputs(f"/{encoded_slug}/", post_html)

        rec = pub.normalize_record(
            {
                "slug": slug,
                "canonical_url": canonical_url,
                "title": title,
                "excerpt": excerpt,
                "language": lang,
                "focus_keyword": keyword,
                "additional_keywords": additional,
                "category": category["name"],
                "published_at": now,
                "source": "generated",
                "post_id": post_id,
            }
        )
        manifest = [r for r in manifest if r["canonical_url"] != canonical_url]
        manifest.append(rec)
        record_pool = pub.build_record_pool(manifest)
        updated_urls.append(canonical_url)

    state["cursor"] = max(int(state.get("cursor") or 0), end_idx)
    state["last_run"] = now
    pub.save_state(state)
    manifest = pub.sort_records_desc(manifest)
    pub.save_manifest(manifest)

    pub.rebuild_home_and_pagination(manifest, home_template_html, page_template_html)
    pub.write_category_pages(manifest, category_template_html)
    pub.generate_seo_files(pub.DIST_DIR, pub.SITE_BASE_URL)

    print("Generated URLs:")
    for url in updated_urls:
        print(url)


if __name__ == "__main__":
    main()
