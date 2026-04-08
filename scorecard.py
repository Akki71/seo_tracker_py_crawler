"""
scorecard.py — Build the SEO pass/fail scorecard from crawled page data.
"""

def _health_check(col, val, pd):
    v = str(val) if val else ""
    if col == "status":
        if val == 200 or str(val).startswith("200"): return "pass"
        if isinstance(val, int) and val >= 400: return "fail"
        return "neutral"
    if col == "canonical_status":
        return "pass" if v == "Correct" else ("fail" if v in ("Missing",) or "Mismatch" in v else "neutral")
    if col == "duplicate_status":
        return "pass" if v == "Unique" else ("fail" if "Duplicate" in v else "neutral")
    if col == "thin_content":
        return "pass" if v == "No" else ("fail" if v == "Yes" else "neutral")
    if col == "current_title":
        return "fail" if not v else "pass"
    if col == "title_length":
        try:
            n = int(val)
            return "pass" if 30 <= n <= 60 else "fail"
        except: return "neutral"
    if col == "current_meta_description":
        return "fail" if not v else "pass"
    if col == "meta_desc_length":
        try:
            n = int(val)
            return "pass" if 70 <= n <= 160 else "fail"
        except: return "neutral"
    if col == "current_h1":
        return "fail" if not v else "pass"
    if col == "google_analytics":
        return "pass" if v == "Yes" else "fail"
    if col == "og_tags":
        return "pass" if v == "Present" else "fail"
    if col == "og_title_current":
        return "fail" if v in ("Missing", "") else "pass"
    if col == "og_description_current":
        return "fail" if v in ("Missing", "") else "pass"
    if col == "schema_markup":
        return "pass" if v == "Present" else "fail"
    if col == "image_alt_status":
        if "Missing" in v: return "fail"
        if v == "All Present": return "pass"
        return "neutral"
    if col == "seo_score":
        try:
            n = int(val)
            return "pass" if n >= 70 else ("neutral" if n >= 50 else "fail")
        except: return "neutral"
    if col == "mobile_score" or col == "desktop_score":
        try:
            n = int(val)
            return "pass" if n >= 70 else ("neutral" if n >= 50 else "fail")
        except: return "neutral"
    if col == "spam_malware_flags":
        return "pass" if v == "Clean" else ("fail" if v else "neutral")
    return "neutral"


SCORECARD_FIELDS = [
    ("status",                  "HTTP Status 200 OK"),
    ("canonical_status",        "Canonical Tag Correct"),
    ("duplicate_status",        "Unique Content (No Duplicate)"),
    ("thin_content",            "Content Depth (300+ words)"),
    ("current_title",           "Title Tag Present"),
    ("title_length",            "Title Length (30-60 chars)"),
    ("current_meta_description","Meta Description Present"),
    ("meta_desc_length",        "Meta Description Length (70-160 chars)"),
    ("current_h1",              "H1 Tag Present"),
    ("google_analytics",        "Google Analytics Installed"),
    ("og_tags",                 "Open Graph Tags Present"),
    ("og_title_current",        "OG Title Set"),
    ("og_description_current",  "OG Description Set"),
    ("schema_markup",           "Schema Markup (JSON-LD) Present"),
    ("image_alt_status",        "All Images Have ALT Text"),
    ("spam_malware_flags",      "No Spam/Malware Detected"),
    ("seo_score",               "SEO Score >= 70"),
    ("mobile_score",            "Mobile PageSpeed >= 70"),
    ("desktop_score",           "Desktop PageSpeed >= 70"),
]


def build_scorecard(pages, robots_status, sitemap_status,
                    llm_status, gbp_status, broken_links_list):
    """Returns (scorecard_results, global_checks)."""
    pages_200 = [p for p in pages if str(p.get("status","")).startswith("200") or p.get("status") == 200]
    total = len(pages_200) or 1

    results = []
    for field, label in SCORECARD_FIELDS:
        pass_c = fail_c = 0
        for p in pages_200:
            h = _health_check(field, p.get(field,""), p)
            if h == "pass": pass_c += 1
            elif h == "fail": fail_c += 1
        pct = (pass_c / total) * 100
        status = "PASSED" if pct >= 80 else ("WARNING" if pct >= 50 else "FAILED")
        results.append((label, pass_c, fail_c, total, pct, status))

    global_checks = [
        ("robots.txt Present",              "Present" in robots_status),
        ("sitemap.xml Present",             "Valid" in sitemap_status),
        ("llms.txt Present",                "Present" in llm_status),
        ("Google Business Profile",         gbp_status == "Present"),
        ("No Broken Links",                 len(broken_links_list) == 0),
    ]

    return results, global_checks
