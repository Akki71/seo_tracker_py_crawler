"""
scorecard.py — Build the SEO pass/fail scorecard from crawled page data.
All 24 health checks from old code fully restored.
"""


def _health_check(col, val, pd):
    """Evaluate whether a field value is healthy (pass), unhealthy (fail), or neutral."""
    v = str(val) if val else ""

    if col == "status":
        if val == 200 or str(val).startswith("200"): return "pass"
        if isinstance(val, int) and val >= 400: return "fail"
        if "Timeout" in v or "Error" in v: return "fail"
        return "neutral"

    if col == "canonical_status":
        if v == "Correct": return "pass"
        if v == "Missing" or "Mismatch" in v: return "fail"
        return "neutral"

    if col == "duplicate_status":
        if v == "Unique": return "pass"
        if "Duplicate" in v: return "fail"
        return "neutral"

    if col == "thin_content":
        if v == "No": return "pass"
        if v == "Yes": return "fail"
        return "neutral"

    if col == "current_title":
        return "fail" if not v or v == "" else "pass"

    if col == "title_length":
        try:
            n = int(val)
            if 30 <= n <= 60: return "pass"
            if n == 0: return "fail"
            return "fail"   # too short or too long
        except: return "neutral"

    if col == "current_meta_description":
        return "fail" if not v or v == "" else "pass"

    if col == "meta_desc_length":
        try:
            n = int(val)
            if 70 <= n <= 160: return "pass"
            if n == 0: return "fail"
            return "fail"
        except: return "neutral"

    if col == "current_h1":
        return "fail" if not v or v == "" else "pass"

    if col == "google_analytics":
        return "pass" if v == "Yes" else "fail"

    if col == "google_search_console":           # ← RESTORED
        return "pass" if v == "Yes" else "fail"

    if col == "og_tags":
        return "pass" if v == "Present" else "fail"

    if col == "og_title_current":
        return "fail" if v == "Missing" or not v else "pass"

    if col == "og_description_current":
        return "fail" if v == "Missing" or not v else "pass"

    if col == "og_image_current":               # ← RESTORED
        return "fail" if v == "Missing" or not v else "pass"

    if col == "schema_markup":
        return "pass" if v == "Present" else "fail"

    if col == "image_alt_status":
        if "Missing" in v: return "fail"
        if v == "All Present": return "pass"
        if v == "No Images": return "neutral"
        return "neutral"

    if col == "images_missing_alt":             # ← RESTORED
        try:
            n = int(val)
            return "pass" if n == 0 else "fail"
        except: return "neutral"

    if col == "seo_score":
        try:
            n = int(val) if isinstance(val, (int, float)) else int(val)
            if n >= 70: return "pass"
            if n >= 50: return "neutral"
            return "fail"
        except: return "neutral"

    if col == "seo_grade":                      # ← RESTORED
        if v in ("A+", "A", "B"): return "pass"
        if v in ("D", "F"): return "fail"
        return "neutral"

    if col == "mobile_score" or col == "desktop_score":
        try:
            n = int(val)
            if n >= 70: return "pass"
            if n >= 50: return "neutral"
            return "fail"
        except: return "neutral"

    if col == "word_count":                     # ← RESTORED
        try:
            n = int(val)
            return "pass" if n >= 300 else "fail"
        except: return "neutral"

    if col == "redirect_suggestion":            # ← RESTORED
        return "fail" if v else "neutral"

    if col == "spam_malware_flags":
        if v == "Clean": return "pass"
        if v: return "fail"
        return "neutral"

    return "neutral"


# All 25 scorecard fields (24 from old + desktop_score kept from new)
SCORECARD_FIELDS = [
    ("status",                   "HTTP Status 200 OK"),
    ("canonical_status",         "Canonical Tag Correct"),
    ("duplicate_status",         "Unique Content (No Duplicate)"),
    ("thin_content",             "Content Depth (300+ words)"),
    ("word_count",               "Word Count >= 300"),                   # RESTORED
    ("current_title",            "Title Tag Present"),
    ("title_length",             "Title Length (30-60 chars)"),
    ("current_meta_description", "Meta Description Present"),
    ("meta_desc_length",         "Meta Description Length (70-160 chars)"),
    ("current_h1",               "H1 Tag Present"),
    ("google_analytics",         "Google Analytics Installed"),
    ("google_search_console",    "Google Search Console Verified"),       # RESTORED
    ("og_tags",                  "Open Graph Tags Present"),
    ("og_title_current",         "OG Title Set"),
    ("og_description_current",   "OG Description Set"),
    ("og_image_current",         "OG Image Set"),                        # RESTORED
    ("schema_markup",            "Schema Markup (JSON-LD) Present"),
    ("image_alt_status",         "All Images Have ALT Text"),
    ("images_missing_alt",       "Images Missing ALT Count = 0"),        # RESTORED
    ("seo_score",                "SEO Score >= 70"),
    ("seo_grade",                "SEO Grade A or B"),                    # RESTORED
    ("mobile_score",             "Mobile PageSpeed >= 70"),
    ("desktop_score",            "Desktop PageSpeed >= 70"),
    ("redirect_suggestion",      "No Broken Redirect Needed"),           # RESTORED
    ("spam_malware_flags",       "No Spam/Malware Detected"),
]


def build_scorecard(pages, robots_status, sitemap_status,
                    llm_status, gbp_status, broken_links_list):
    """
    Returns (scorecard_results, global_checks).

    scorecard_results: list of (label, pass_count, fail_count, total, pct, status)
    global_checks:     list of (label, bool)
    """
    pages_200 = [
        p for p in pages
        if str(p.get("status", "")).startswith("200") or p.get("status") == 200
    ]
    total = len(pages_200) or 1

    results = []
    for field, label in SCORECARD_FIELDS:
        pass_c = fail_c = 0
        for p in pages_200:
            h = _health_check(field, p.get(field, ""), p)
            if h == "pass":
                pass_c += 1
            elif h == "fail":
                fail_c += 1
        pct    = (pass_c / total) * 100
        status = "PASSED" if pct >= 80 else ("WARNING" if pct >= 50 else "FAILED")
        results.append((label, pass_c, fail_c, total, round(pct, 1), status))

    global_checks = [
        ("robots.txt Present",    "Present" in robots_status),
        ("sitemap.xml Present",   "Valid"   in sitemap_status),
        ("llms.txt Present",      "Present" in llm_status),
        ("Google Business Profile", gbp_status == "Present"),
        ("No Broken Links",       len(broken_links_list) == 0),
    ]

    return results, global_checks