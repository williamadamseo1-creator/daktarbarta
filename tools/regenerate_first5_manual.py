#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import sys
from datetime import datetime
from html import escape
from pathlib import Path
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ai_content_publisher as pub


BN_RE = re.compile(r"[\u0980-\u09FF]")


def read_first_keywords(path: Path, n: int) -> list[str]:
    rows: list[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = (row.get("kws") or "").strip()
            if not kw:
                continue
            rows.append(kw)
            if len(rows) == n:
                break
    return rows


def infer_lang(kw: str) -> str:
    return "bn" if BN_RE.search(kw) else "en"


def bn_sections(keyword: str) -> list[tuple[str, str, str]]:
    return [
        (
            f"{keyword} কী এবং কেন মানুষ এটি নিয়ে জানতে চায়",
            f"{keyword} সাধারণত উপসর্গভিত্তিক আলোচনায় আসে। অনেক রোগী নাম শুনে সরাসরি ওষুধ শুরু করেন, কিন্তু সঠিক পদ্ধতি হলো আগে সমস্যা স্পষ্ট করা: উপসর্গ কতদিন ধরে, কোন পরিস্থিতিতে বাড়ে, আগের চিকিৎসা কী ছিল, এবং অন্য কোনো দীর্ঘমেয়াদি রোগ আছে কি না। একই নামের ওষুধ সবার ক্ষেত্রে একইভাবে কাজ করে না, তাই ব্যক্তিগত মূল্যায়ন ছাড়া সিদ্ধান্ত নিলে ফল অনিশ্চিত হতে পারে।",
            f"এই কারণে {keyword} সম্পর্কে বাস্তব তথ্য জানা জরুরি। রোগীর প্রত্যাশা, চিকিৎসার লক্ষ্য, এবং কতদিন পর্যবেক্ষণ করবেন তা আগে ঠিক করলে চিকিৎসা-পরিকল্পনা পরিষ্কার থাকে। হঠাৎ ডোজ বাড়ানো, বন্ধু বা অনলাইন পরামর্শে ওষুধ পরিবর্তন করা, বা একই সাথে একাধিক নতুন ওষুধ শুরু করা এড়ানো উচিত।",
        ),
        (
            f"{keyword} ব্যবহারের আগে যে তথ্য চিকিৎসককে জানাবেন",
            "অ্যালার্জির ইতিহাস, রক্তচাপ/ডায়াবেটিস/থাইরয়েডের ওষুধ, গ্যাস্ট্রিক সমস্যা, কিডনি-লিভারের রোগ, ঘুমের সমস্যা বা মানসিক চাপ—এসব তথ্য লুকিয়ে রাখলে চিকিৎসা পরিকল্পনা দুর্বল হয়ে যায়। একটি পূর্ণ ওষুধ-তালিকা (বর্তমানে যা যা খাচ্ছেন) দেখালে মিথস্ক্রিয়ার ঝুঁকি কমে।",
            f"{keyword} শুরু করার আগে রোগীর জন্য একটি বেসলাইন নোট রাখা ভালো: শ্বাসপ্রশ্বাস, ব্যথা, ঘুম, ক্লান্তি, কাজে মনোযোগ, এবং দৈনন্দিন কার্যক্ষমতা কেমন। পরে ফলো-আপে একই সূচকে উন্নতি/অবনতি তুলনা করা যায় এবং বোঝা যায় ওষুধ সত্যিই উপকার দিচ্ছে কি না।",
        ),
        (
            f"{keyword} খাওয়ার নিয়ম: শৃঙ্খলা, সময়, এবং ডোজ",
            f"{keyword} ব্যবহারে সবচেয়ে গুরুত্বপূর্ণ বিষয় হলো ধারাবাহিকতা। চিকিৎসকের বলা সময়ে নিয়মিত সেবন করা উচিত। ডোজ মিস হলে পরেরবার দ্বিগুণ ডোজ নেওয়া নিরাপদ নয়। উপসর্গ কিছুটা কমে গেলেও হঠাৎ বন্ধ না করে পরবর্তী ফলো-আপে পরিকল্পনা পরিবর্তন করা ভালো।",
            "বেশিরভাগ ক্ষেত্রে মানুষ দ্রুত ফল না পেলে নিজে নিজে ওষুধ বাড়িয়ে দেন, এতে পার্শ্বপ্রতিক্রিয়া বা বিভ্রান্তি তৈরি হয়। উন্নতি ধীরগতির হলেও পর্যবেক্ষণ নোট ঠিকঠাক থাকলে চিকিৎসক যথাযথভাবে ডোজ/সময় ঠিক করতে পারেন।",
        ),
        (
            f"{keyword} চলাকালে সম্ভাব্য পার্শ্বপ্রতিক্রিয়া",
            "কিছু রোগীর ক্ষেত্রে হালকা গ্যাস্ট্রিক অস্বস্তি, মাথা ঝিমঝিম, ঘুমের ধরনে পরিবর্তন, বা উপসর্গে সাময়িক ওঠানামা দেখা যেতে পারে। এ ধরনের লক্ষণ হালকা হলে পর্যবেক্ষণে রাখা যায়, তবে বারবার হলে চিকিৎসককে জানানো উচিত।",
            "শ্বাসকষ্ট, তীব্র র‍্যাশ/চুলকানি, বুকব্যথা, অস্বাভাবিক দুর্বলতা, অবিরাম বমি, বা মাথা ঘুরে পড়ে যাওয়ার মতো লক্ষণ জরুরি সতর্কতা। এমন হলে নিজে সিদ্ধান্ত না নিয়ে দ্রুত চিকিৎসা নিন।",
        ),
        (
            f"{keyword} এর সাথে জীবনযাপনের যে পরিবর্তন জরুরি",
            "ওষুধের পাশাপাশি নিয়মিত ঘুম, পর্যাপ্ত পানি, কম তেল-মসলা, ধূমপান পরিহার, এবং স্ট্রেস নিয়ন্ত্রণ রোগীকে দ্রুত স্থিতিশীল হতে সাহায্য করে। যাদের বারবার উপসর্গ ফিরে আসে, তাদের জন্য ট্রিগার-ডায়েরি রাখা খুব কার্যকর।",
            f"{keyword} একা সব সমস্যার সমাধান করবে—এমন ধারণা বাস্তবসম্মত নয়। চিকিৎসকের পরিকল্পনা, জীবনযাপন, এবং ফলো-আপ একসাথে চললে ফল ভালো হয় এবং অপ্রয়োজনীয় ওষুধ-পরিবর্তন কমে।",
        ),
        (
            f"{keyword} নিয়ে সাধারণ ভুল ধারণা ও বাস্তবতা",
            f"অনেকে মনে করেন {keyword} নামের ওষুধ হলে দীর্ঘদিন ইচ্ছেমতো খাওয়া নিরাপদ। বাস্তবে যেকোনো ওষুধ দীর্ঘদিন নিলে সময়ে সময়ে পুনর্মূল্যায়ন দরকার। উপসর্গ না কমলে রোগ নির্ণয় পুনরায় যাচাই করতে হয়।",
            "আরেকটি ভুল হলো একাধিক উৎসের পরামর্শ মিশিয়ে ব্যবহার করা। এতে চিকিৎসা-রেসপন্স বোঝা কঠিন হয়। একটি চিকিৎসা পরিকল্পনা ধরে নির্দিষ্ট সময় পরে মূল্যায়ন করা নিরাপদ পদ্ধতি।",
        ),
    ]


def en_sections(keyword: str) -> list[tuple[str, str, str]]:
    return [
        (
            f"{keyword}: what it is and where people use it",
            f"{keyword} is commonly discussed in symptom-focused care. However, use should be based on proper clinical assessment, not on product name alone. Patients differ in diagnosis, severity, comorbidity, and current medication profile, so individualized planning is essential.",
            f"Before using {keyword}, establish a baseline for symptom severity, sleep quality, trigger exposure, and daily functioning. Tracking these markers helps determine whether the treatment is truly helping or if the plan should be changed.",
        ),
        (
            f"How to use {keyword} safely",
            f"Follow the prescribed timing and avoid self-adjusting dose frequency. If you miss a dose, do not double the next one. With {keyword}, consistency matters more than frequent unsupervised changes.",
            "If symptoms partially improve, continue only as advised during follow-up. Sudden discontinuation or random switching between products can reduce clarity and delay clinical decisions.",
        ),
        (
            f"Side effects and warning signs while using {keyword}",
            "Mild reactions can include temporary stomach discomfort, headache, sleep disturbance, or transient symptom fluctuation. These should still be documented and reviewed if persistent.",
            f"Stop {keyword} and seek urgent care if severe allergy, breathing trouble, chest pain, persistent vomiting, or neurological symptoms appear.",
        ),
        (
            f"Medication interactions and high-risk situations",
            "Patients should report all current drugs and supplements, especially blood pressure, diabetes, thyroid, and psychiatric medicines. Hidden interactions are a preventable cause of adverse outcomes.",
            "Pregnancy, breastfeeding, chronic kidney/liver disease, and advanced age require closer supervision. In these groups, risk-benefit review must be more frequent.",
        ),
        (
            f"Lifestyle plan to improve outcomes with {keyword}",
            "Medication-only expectations are often unrealistic. Better sleep, hydration, trigger control, nutrition quality, and stress management improve symptom control and reduce recurrence.",
            f"Use {keyword} as part of a complete care plan. Objective follow-up with your clinician is the safest way to maintain benefit and reduce avoidable risk.",
        ),
    ]


def build_article_html(keyword: str, lang: str, links: list[dict[str, str]]) -> str:
    parts: list[str] = []
    if lang == "bn":
        intro = (
            f"{keyword} নিয়ে নিরাপদ সিদ্ধান্ত নিতে ব্যবহারবিধি, সতর্কতা, পার্শ্বপ্রতিক্রিয়া, "
            "এবং ফলো-আপ কৌশল জানা জরুরি। এই গাইডটি তথ্যভিত্তিক; ব্যক্তিগত চিকিৎসার বিকল্প নয়।"
        )
        parts.append(f"<p>{escape(intro)}</p>")
        for h, p1, p2 in bn_sections(keyword):
            parts.append(f"<h2>{escape(h)}</h2>")
            parts.append(f"<p>{escape(p1)}</p>")
            parts.append(f"<p>{escape(p2)}</p>")
        parts.append("<h2>সংশ্লিষ্ট আরও পড়ুন</h2>")
    else:
        intro = (
            f"This practical guide explains {keyword}, including use pattern, safety points, "
            "side effects, and follow-up strategy. It is informational and not a personal prescription."
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
        parts.append(f'<li><a href="{escape(href)}">{escape(item["title"])}</a></li>')
    parts.append("</ul>")

    if lang == "bn":
        parts.append("<h2>সাধারণ প্রশ্নোত্তর</h2>")
        parts.append(f"<h3>{escape(keyword)} কতদিন ব্যবহার করা যায়?</h3>")
        parts.append("<p>সমস্যার ধরন ও উন্নতির গতির ওপর নির্ভর করে। চিকিৎসকের ফলো-আপ অনুযায়ী সময় নির্ধারণ করুন।</p>")
        parts.append("<h3>উপসর্গ কমলে কি নিজে নিজে বন্ধ করব?</h3>")
        parts.append("<p>না। হঠাৎ বন্ধ না করে চিকিৎসকের পরামর্শে ধাপে ধাপে পরিকল্পনা বদলান।</p>")
        parts.append("<h3>পার্শ্বপ্রতিক্রিয়া হলে কী করব?</h3>")
        parts.append("<p>তীব্র লক্ষণ হলে দ্রুত চিকিৎসা নিন এবং পরামর্শ ছাড়া ডোজ পরিবর্তন করবেন না।</p>")
        parts.append("<h2>মেডিকেল ডিসক্লেইমার</h2>")
        parts.append(
            "<p>এই কনটেন্ট শুধুমাত্র তথ্যের উদ্দেশ্যে। এটি রোগ নির্ণয়, প্রেসক্রিপশন বা ব্যক্তিগত চিকিৎসার বিকল্প নয়।"
            " জরুরি সমস্যা হলে নিকটস্থ বিশেষজ্ঞ চিকিৎসকের পরামর্শ নিন।</p>"
        )
    else:
        parts.append("<h2>Frequently Asked Questions</h2>")
        parts.append(f"<h3>How long should {escape(keyword)} be used?</h3>")
        parts.append("<p>Duration depends on diagnosis, symptom trend, and supervised follow-up findings.</p>")
        parts.append(f"<h3>Can I increase {escape(keyword)} dose on my own?</h3>")
        parts.append("<p>No. Unsupervised dose escalation may increase risk and reduce treatment clarity.</p>")
        parts.append("<h3>When should I stop and seek urgent care?</h3>")
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
        return f"{keyword} নিয়ে বিস্তারিত বাংলা গাইড: ব্যবহার, ডোজ, পার্শ্বপ্রতিক্রিয়া, সতর্কতা ও নিরাপদ ফলো-আপ কৌশল।"
    return f"Complete practical guide to {keyword}: usage pattern, dosage discipline, side effects, precautions, and safe follow-up."


def build_excerpt(keyword: str, lang: str) -> str:
    if lang == "bn":
        return f"{keyword} নিয়ে বিভ্রান্তি দূর করতে বাস্তবভিত্তিক ব্যবহারবিধি, সতর্কতা, পার্শ্বপ্রতিক্রিয়া ও চিকিৎসা ফলো-আপ আলোচনা।"
    return f"Practical guide to {keyword} covering real-world use, side effects, precautions, and follow-up strategy."


def main() -> None:
    logger = pub.setup_logging()
    kws = read_first_keywords(pub.KWS_FILE, 5)
    if len(kws) < 5:
        raise RuntimeError("kws.csv does not contain at least 5 keywords")

    templates = pub.find_templates()
    post_template_html = templates["post"].read_text(encoding="utf-8", errors="ignore")
    home_template_html = templates["home"].read_text(encoding="utf-8", errors="ignore")
    page_template_html = templates["page"].read_text(encoding="utf-8", errors="ignore")

    indexed_records, indexed_max_post_id = pub.index_existing_posts(logger)
    manifest = pub.merge_manifest(indexed_records, pub.load_manifest())
    state = pub.load_state()
    state["next_post_id"] = max(int(state.get("next_post_id") or 0), pub.compute_next_post_id(manifest, indexed_max_post_id))

    record_pool = pub.build_record_pool(manifest)
    now = pub.now_iso(pub.DEFAULT_TIMEZONE)

    updated_urls: list[str] = []
    for kw in kws:
        lang = infer_lang(kw)
        title = build_title(kw, lang)
        meta_description = build_meta(kw, lang)[:158]
        excerpt = build_excerpt(kw, lang)
        additional = pub.sanitize_additional_keywords(pub.derive_additional_keywords(kw, lang), kw, lang)
        links = pub.select_internal_links(record_pool, kw, additional, limit=5)
        article_html = build_article_html(kw, lang, links)

        slug = pub.slugify(kw)
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
            category_name="নাক",
            published_iso=now,
            modified_iso=now,
            focus_keyword=kw,
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
                "focus_keyword": kw,
                "additional_keywords": additional,
                "category": "নাক",
                "published_at": now,
                "source": "generated",
                "post_id": post_id,
            }
        )
        manifest = [r for r in manifest if r["canonical_url"] != canonical_url]
        manifest.append(rec)
        record_pool = pub.build_record_pool(manifest)
        updated_urls.append(canonical_url)

        # Cleanup accidental duplicate slug variants like "-2" if present.
        dup_slug = f"{slug}-2"
        dup_canonical = f"{pub.SITE_BASE_URL}/{quote(dup_slug, safe='-')}/"
        manifest = [r for r in manifest if r.get("canonical_url") != dup_canonical]
        for out in pub.url_path_to_output_paths(f"/{quote(dup_slug, safe='-')}/"):
            if out.parent.exists():
                import shutil

                shutil.rmtree(out.parent, ignore_errors=True)

    state["cursor"] = max(int(state.get("cursor") or 0), 5)
    state["last_run"] = datetime.now().isoformat(timespec="seconds")
    pub.save_state(state)
    pub.save_manifest(pub.sort_records_desc(manifest))
    pub.rebuild_home_and_pagination(pub.sort_records_desc(manifest), home_template_html, page_template_html)
    pub.generate_seo_files(pub.DIST_DIR, pub.SITE_BASE_URL)

    print("Updated first 5 posts:")
    for url in updated_urls:
        print(url)


if __name__ == "__main__":
    main()
