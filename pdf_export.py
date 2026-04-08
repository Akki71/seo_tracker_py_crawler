"""
pdf_export.py — Generate the PDF audit report.
Adapted from original seo_crawler.py for API use (no stdin, no interactive prompts).
"""

import os, re, json, base64, io
from datetime import datetime
from collections import OrderedDict

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from PIL import Image as PILImage, ImageDraw, ImageFont

ATL_PRIMARY   = "#1A237E"
ATL_SECONDARY = "#283593"
ATL_LIGHT     = "#C5CAE9"
LOGO_PATH       = "aquiltechlabs_logo.png"
WATERMARK_PATH  = "_watermark_atl.png"

# ── Font registration ──────────────────────────────────────────────────────────

UNICODE_FONT = "Helvetica"
try:
    dv = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    dvb = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    if os.path.exists(dv):
        pdfmetrics.registerFont(TTFont("DejaVuSans", dv))
        pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", dvb))
        UNICODE_FONT = "DejaVuSans"
except Exception:
    pass

# ── Watermark ──────────────────────────────────────────────────────────────────

def _make_watermark():
    try:
        wm = PILImage.new("RGBA", (500, 200), (0,0,0,0))
        draw = ImageDraw.Draw(wm)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        except Exception:
            font = ImageFont.load_default()
        draw.text((30, 70), "AquilTechLabs", fill=(26,35,126,25), font=font)
        wm.save(WATERMARK_PATH, "PNG")
        return True
    except Exception:
        return False

_make_watermark()
_watermark_exists = os.path.exists(WATERMARK_PATH)

# ── Logo ───────────────────────────────────────────────────────────────────────

if not os.path.exists(LOGO_PATH):
    try:
        img = PILImage.new("RGBA", (300,80), (0,0,0,0))
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        except Exception:
            font = ImageFont.load_default()
        draw.text((10,25), "AquilTechLabs", fill=(26,35,126,255), font=font)
        img.save(LOGO_PATH, "PNG")
    except Exception:
        pass

_logo_exists = os.path.exists(LOGO_PATH)

# ── Header/Footer callback ─────────────────────────────────────────────────────

def _make_header_footer(domain: str):
    def add_logo_footer(canvas_obj, doc):
        canvas_obj.saveState()
        page_w, page_h = A4

        # Top bar
        canvas_obj.setFillColor(colors.HexColor(ATL_PRIMARY))
        canvas_obj.rect(0, page_h - 3, page_w, 3, fill=1, stroke=0)

        # Header bg
        canvas_obj.setFillColor(colors.HexColor("#F5F6FA"))
        canvas_obj.rect(0, page_h - 0.85*inch, page_w, 0.82*inch, fill=1, stroke=0)

        # Header divider
        canvas_obj.setStrokeColor(colors.HexColor(ATL_PRIMARY))
        canvas_obj.setLineWidth(1.5)
        canvas_obj.line(0.4*inch, page_h - 0.85*inch, page_w - 0.4*inch, page_h - 0.85*inch)

        # ATL logo / text
        if _logo_exists:
            try:
                canvas_obj.drawImage(LOGO_PATH, 0.5*inch, page_h - 0.75*inch,
                                     width=1.4*inch, height=0.45*inch,
                                     preserveAspectRatio=True, mask='auto')
            except Exception:
                canvas_obj.setFont('Helvetica-Bold', 10)
                canvas_obj.setFillColor(colors.HexColor(ATL_PRIMARY))
                canvas_obj.drawString(0.5*inch, page_h - 0.55*inch, "AquilTechLabs")
        else:
            canvas_obj.setFont('Helvetica-Bold', 10)
            canvas_obj.setFillColor(colors.HexColor(ATL_PRIMARY))
            canvas_obj.drawString(0.5*inch, page_h - 0.55*inch, "AquilTechLabs")

        # Brand name right
        brand_display = domain.replace("www.","").split(".")[0].upper()
        canvas_obj.setFont('Helvetica-Bold', 10)
        canvas_obj.setFillColor(colors.HexColor("#2C3E50"))
        canvas_obj.drawRightString(page_w - 0.5*inch, page_h - 0.55*inch, brand_display)

        # Watermark
        if _watermark_exists:
            try:
                canvas_obj.drawImage(WATERMARK_PATH,
                                     page_w/2 - 3.0*inch, page_h/2 - 1.0*inch,
                                     width=6.0*inch, height=2.0*inch,
                                     preserveAspectRatio=True, mask='auto')
            except Exception:
                pass

        # Side borders
        canvas_obj.setStrokeColor(colors.HexColor(ATL_LIGHT))
        canvas_obj.setLineWidth(0.5)
        canvas_obj.line(0.3*inch, 0.5*inch, 0.3*inch, page_h - 0.85*inch)
        canvas_obj.line(page_w - 0.3*inch, 0.5*inch, page_w - 0.3*inch, page_h - 0.85*inch)

        # Bottom bar
        canvas_obj.setFillColor(colors.HexColor(ATL_PRIMARY))
        canvas_obj.rect(0, 0, page_w, 3, fill=1, stroke=0)

        # Footer
        canvas_obj.setStrokeColor(colors.HexColor(ATL_PRIMARY))
        canvas_obj.setLineWidth(1)
        canvas_obj.line(0.4*inch, 0.45*inch, page_w - 0.4*inch, 0.45*inch)
        canvas_obj.setFont('Helvetica', 6.5)
        canvas_obj.setFillColor(colors.HexColor("#7F8C8D"))
        canvas_obj.drawString(0.5*inch, 0.25*inch, "AquilTechLabs SEO Audit Report")
        canvas_obj.drawCentredString(page_w/2, 0.25*inch, f"{domain} | {datetime.now().strftime('%Y-%m-%d')}")
        canvas_obj.drawRightString(page_w - 0.5*inch, 0.25*inch, f"Page {doc.page}")
        canvas_obj.restoreState()
    return add_logo_footer

# ── Styles ─────────────────────────────────────────────────────────────────────

styles = getSampleStyleSheet()
styles.add(ParagraphStyle('CellKey',   parent=styles['Normal'], fontSize=7.5, leading=9, fontName='Helvetica-Bold', textColor=colors.HexColor("#2C3E50")))
styles.add(ParagraphStyle('CellValue', parent=styles['Normal'], fontSize=7.5, leading=9, fontName=UNICODE_FONT))
styles.add(ParagraphStyle('PageTitle', parent=styles['Heading2'], fontSize=11, leading=13, fontName='Helvetica-Bold', textColor=colors.HexColor("#2C3E50")))
styles.add(ParagraphStyle('TOCEntry',  parent=styles['Normal'], fontSize=8, leading=11, fontName='Helvetica', textColor=colors.HexColor("#2980B9")))
styles.add(ParagraphStyle('RecoBody',  parent=styles['Normal'], fontSize=9, leading=12, fontName='Helvetica', spaceBefore=3, spaceAfter=3))
styles.add(ParagraphStyle('SummaryTitle', parent=styles['Heading1'], fontSize=20, leading=24, fontName='Helvetica-Bold', textColor=colors.HexColor("#2C3E50"), alignment=TA_CENTER))

def _safe(val, max_len=500):
    s = str(val) if val else ""
    s = s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    return s[:max_len] + ("..." if len(s) > max_len else "")

def _kv_table(data_dict, skip_keys=None):
    skip_keys = skip_keys or set()
    rows = []
    for k, v in data_dict.items():
        if k.startswith("_") or k in skip_keys: continue
        label = f"<b>{k.replace('_',' ').title()}</b>"
        rows.append([
            Paragraph(label, styles["CellKey"]),
            Paragraph(_safe(v), styles["CellValue"])
        ])
    if not rows: return None
    t = Table(rows, colWidths=[2.4*inch, 3.8*inch])
    t.setStyle(TableStyle([
        ('GRID',       (0,0),(-1,-1), 0.5, colors.HexColor("#BDC3C7")),
        ('VALIGN',     (0,0),(-1,-1), 'TOP'),
        ('BACKGROUND', (0,0),(0,-1),  colors.HexColor("#ECF0F1")),
        ('LEFTPADDING',(0,0),(-1,-1), 5),
        ('RIGHTPADDING',(0,0),(-1,-1),5),
        ('TOPPADDING', (0,0),(-1,-1), 3),
        ('BOTTOMPADDING',(0,0),(-1,-1),3),
    ]))
    return t


# ── Main export function ────────────────────────────────────────────────────────

def generate_pdf(pages, broken_links, images, scorecard_results, global_checks,
                 keyword_data, blog_topics_data, backlink_strategy_data,
                 six_month_plan_data, internal_linking_data,
                 keyword_url_map_data, axo_data,
                 base_url, domain, timestamp,
                 site_recommendation_text, detected_location,
                 robots_status, sitemap_status, llm_status, gbp_status) -> str:

    os.makedirs("output", exist_ok=True)
    pdf_file = f"output/{domain}_{timestamp}_SEO.pdf"

    pages_200 = [p for p in pages if str(p.get("status","")).startswith("200") or p.get("status") == 200]
    missing_imgs = [i for i in images if i.get("alt_status") == "Missing"]
    scores = [p.get("seo_score",0) for p in pages_200 if isinstance(p.get("seo_score"),(int,float))]
    avg_score = sum(scores)/len(scores) if scores else 0

    elements = []

    # ── Cover page ─────────────────────────────────────────────────────────────
    elements.append(Spacer(1, 30))
    elements.append(Paragraph("AquilTechLabs", styles["SummaryTitle"]))
    elements.append(Paragraph("SEO Audit Report", styles["SummaryTitle"]))
    elements.append(Spacer(1, 15))
    elements.append(Paragraph(f"Website: {_safe(base_url)}", styles["Normal"]))
    elements.append(Paragraph(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles["Normal"]))
    elements.append(Paragraph(f"Pages Crawled: {len(pages)}", styles["Normal"]))
    elements.append(Paragraph(f"Target Location: {_safe(detected_location or 'Global')}", styles["Normal"]))
    elements.append(Spacer(1, 10))

    # Summary table
    summary_rows = [
        [Paragraph("<b>Metric</b>", styles["CellKey"]),   Paragraph("<b>Value</b>", styles["CellKey"])],
        [Paragraph("Pages 200 OK", styles["CellKey"]),    Paragraph(str(len(pages_200)), styles["CellValue"])],
        [Paragraph("Pages 404",    styles["CellKey"]),    Paragraph(str(len([p for p in pages if str(p.get("status",""))=="404"])), styles["CellValue"])],
        [Paragraph("Broken Links", styles["CellKey"]),    Paragraph(str(len(broken_links)), styles["CellValue"])],
        [Paragraph("robots.txt",   styles["CellKey"]),    Paragraph(_safe(robots_status), styles["CellValue"])],
        [Paragraph("sitemap.xml",  styles["CellKey"]),    Paragraph(_safe(sitemap_status), styles["CellValue"])],
        [Paragraph("llms.txt",     styles["CellKey"]),    Paragraph(_safe(llm_status), styles["CellValue"])],
        [Paragraph("Images Missing ALT", styles["CellKey"]), Paragraph(str(len(missing_imgs)), styles["CellValue"])],
        [Paragraph("Avg SEO Score", styles["CellKey"]),   Paragraph(f"{avg_score:.0f}/100", styles["CellValue"])],
    ]
    st = Table(summary_rows, colWidths=[2.8*inch, 3.4*inch])
    st.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor("#BDC3C7")),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('BACKGROUND',(0,0),(0,-1),colors.HexColor("#ECF0F1")),
        ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
    ]))
    elements.append(st)
    elements.append(PageBreak())

    # ── Table of Contents ──────────────────────────────────────────────────────
    elements.append(Paragraph('<a name="toc"/>All Crawled Pages', styles["PageTitle"]))
    elements.append(Spacer(1, 8))
    toc_rows = [[
        Paragraph("<b>#</b>", styles["CellKey"]),
        Paragraph("<b>URL</b>", styles["CellKey"]),
        Paragraph("<b>Status</b>", styles["CellKey"]),
        Paragraph("<b>SEO Grade</b>", styles["CellKey"]),
    ]]
    for idx, pd in enumerate(pages):
        anchor = f"page_{idx}"
        url_display = _safe(pd.get("url",""), 65)
        toc_rows.append([
            Paragraph(str(idx+1), styles["CellValue"]),
            Paragraph(f'<a href="#{anchor}" color="#2980B9">{url_display}</a>', styles["TOCEntry"]),
            Paragraph(str(pd.get("status","N/A")), styles["CellValue"]),
            Paragraph(str(pd.get("seo_grade","N/A")), styles["CellValue"]),
        ])
    toc_t = Table(toc_rows, colWidths=[0.4*inch, 4.0*inch, 0.7*inch, 0.7*inch])
    toc_t.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.3,colors.HexColor("#D5D8DC")),
        ('BACKGROUND',(0,0),(-1,0),colors.HexColor("#2C3E50")),
        ('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('FONTSIZE',(0,0),(-1,-1),7),
        ('TOPPADDING',(0,0),(-1,-1),2),
        ('BOTTOMPADDING',(0,0),(-1,-1),2),
    ]))
    elements.append(toc_t)
    elements.append(PageBreak())

    # ── Per-page reports ───────────────────────────────────────────────────────
    SKIP_KEYS = {"_content","_screenshot_path","_aeo_faq_list","_body_copy_data",
                 "total_images","images_missing_alt","image_alt_status",
                 "image_optimization_tips","og_image_current","ai_og_image_url"}
    for idx, pd in enumerate(pages):
        anchor = f"page_{idx}"
        elements.append(Paragraph(
            f'<a name="{anchor}"/>Page {idx+1}: {_safe(pd.get("url",""), 80)}',
            styles["PageTitle"]
        ))
        elements.append(Spacer(1, 6))
        t = _kv_table(pd, skip_keys=SKIP_KEYS)
        if t: elements.append(t)
        elements.append(PageBreak())

    # ── Broken links ───────────────────────────────────────────────────────────
    if broken_links:
        elements.append(Paragraph("Broken Links Report", styles["PageTitle"]))
        bl_rows = [[
            Paragraph("<b>Source</b>", styles["CellKey"]),
            Paragraph("<b>Broken URL</b>", styles["CellKey"]),
            Paragraph("<b>Status</b>", styles["CellKey"]),
        ]]
        for bl in broken_links[:200]:
            bl_rows.append([
                Paragraph(_safe(bl.get("source_page",""), 55), styles["CellValue"]),
                Paragraph(_safe(bl.get("broken_url",""), 55), styles["CellValue"]),
                Paragraph(_safe(bl.get("status","")), styles["CellValue"]),
            ])
        bl_t = Table(bl_rows, colWidths=[2.2*inch, 2.5*inch, 0.8*inch])
        bl_t.setStyle(TableStyle([
            ('GRID',(0,0),(-1,-1),0.5,colors.HexColor("#BDC3C7")),
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor("#C0392B")),
            ('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('FONTSIZE',(0,0),(-1,-1),7),
        ]))
        elements.append(bl_t)
        elements.append(PageBreak())

    # ── AI Recommendations ─────────────────────────────────────────────────────
    if site_recommendation_text:
        elements.append(Paragraph("AI Site-Wide Recommendations", styles["PageTitle"]))
        elements.append(Spacer(1, 8))
        for line in site_recommendation_text.split("\n"):
            line = line.strip()
            if not line:
                elements.append(Spacer(1, 4))
            elif line.startswith("- ") or line.startswith("* "):
                elements.append(Paragraph(f"&bull; {_safe(line[2:])}", styles["RecoBody"]))
            else:
                elements.append(Paragraph(_safe(line), styles["RecoBody"]))
        elements.append(PageBreak())

    # ── Scorecard ──────────────────────────────────────────────────────────────
    elements.append(Paragraph("SEO Health Scorecard", styles["PageTitle"]))
    elements.append(Spacer(1, 8))
    sc_rows = [[
        Paragraph("<b>Parameter</b>", styles["CellKey"]),
        Paragraph("<b>Pass</b>", styles["CellKey"]),
        Paragraph("<b>Fail</b>", styles["CellKey"]),
        Paragraph("<b>Rate</b>", styles["CellKey"]),
        Paragraph("<b>Status</b>", styles["CellKey"]),
    ]]
    sc_styles = [
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor("#BDC3C7")),
        ('BACKGROUND',(0,0),(-1,0),colors.HexColor("#2C3E50")),
        ('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTSIZE',(0,0),(-1,-1),7.5),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),3),
        ('BOTTOMPADDING',(0,0),(-1,-1),3),
        ('ALIGN',(1,1),(-1,-1),'CENTER'),
    ]
    for ri, (label, pc, fc, tot, pct, status) in enumerate(scorecard_results, 1):
        txt = f'<font color="{"#1E8449" if status=="PASSED" else "#C0392B" if status=="FAILED" else "#B7950B"}"><b>{status}</b></font>'
        bg = colors.HexColor("#D5F5E3" if status=="PASSED" else "#FADBD8" if status=="FAILED" else "#FCF3CF")
        sc_rows.append([
            Paragraph(label, styles["CellValue"]),
            Paragraph(str(pc), styles["CellValue"]),
            Paragraph(str(fc), styles["CellValue"]),
            Paragraph(f"{pct:.0f}%", styles["CellValue"]),
            Paragraph(txt, styles["CellValue"]),
        ])
        sc_styles.append(('BACKGROUND',(0,ri),(-1,ri),bg))
    sc_t = Table(sc_rows, colWidths=[2.5*inch, 0.6*inch, 0.6*inch, 0.6*inch, 0.9*inch])
    sc_t.setStyle(TableStyle(sc_styles))
    elements.append(sc_t)

    # ── Build PDF ──────────────────────────────────────────────────────────────
    doc = SimpleDocTemplate(
        pdf_file, pagesize=A4,
        topMargin=1.1*inch, bottomMargin=0.7*inch,
        leftMargin=0.6*inch, rightMargin=0.6*inch
    )
    hf = _make_header_footer(domain)
    doc.build(elements, onFirstPage=hf, onLaterPages=hf)
    return pdf_file
