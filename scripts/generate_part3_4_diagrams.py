from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def font(size: int, bold: bool = False):
    path = "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf"
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


TITLE = font(32, True)
TEXT = font(20)
SMALL = font(16)


def box(draw, xy, text, fill="#F9FAFB", outline="#6B7280"):
    x, y, w, h = xy
    draw.rounded_rectangle((x, y, x + w, y + h), radius=14, fill=fill, outline=outline, width=2)
    lines = text.split("\n")
    total = len(lines) * 26
    yy = y + (h - total) // 2
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=TEXT)
        draw.text((x + (w - (bbox[2] - bbox[0])) / 2, yy), line, font=TEXT, fill="#111827")
        yy += 26


def arrow(draw, a, b, color="#374151"):
    draw.line([a, b], fill=color, width=3)
    x, y = b
    sx, sy = a
    if abs(x - sx) >= abs(y - sy):
        d = 1 if x > sx else -1
        pts = [(x, y), (x - 14 * d, y - 8), (x - 14 * d, y + 8)]
    else:
        d = 1 if y > sy else -1
        pts = [(x, y), (x - 8, y - 14 * d), (x + 8, y - 14 * d)]
    draw.polygon(pts, fill=color)


def canvas(title: str, out_dir: Path, filename: str):
    img = Image.new("RGB", (1600, 850), "white")
    draw = ImageDraw.Draw(img)
    draw.text((60, 40), title, font=TITLE, fill="#111827")
    out_dir.mkdir(parents=True, exist_ok=True)
    return img, draw, out_dir / filename


def part3_joins_overview():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("Metric View join patterns", out, "figure_01_join_patterns.png")
    box(draw, (80, 360, 280, 110), "Metric View\nsemantic contract", "#E0F2FE", "#0284C7")
    patterns = [
        ((520, 120, 360, 95), "Star schema\nfact -> dimensions", "#DCFCE7", "#16A34A"),
        ((520, 290, 360, 95), "Snowflake schema\nmulti-hop dimensions", "#EEF2FF", "#4F46E5"),
        ((520, 460, 360, 95), "One-to-many\nspine -> facts", "#FEF3C7", "#D97706"),
        ((520, 630, 360, 95), "Bridge pattern\nmultiple fact tables", "#FCE7F3", "#DB2777"),
    ]
    for xy, text, fill, outline in patterns:
        box(draw, xy, text, fill, outline)
        arrow(draw, (360, 415), (xy[0], xy[1] + xy[3] // 2))
    draw.text((980, 355), "Goal: model relationships once\nso users and agents do not write joins.", font=TEXT, fill="#111827")
    img.save(path)


def part3_cardinality():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("Join cardinality controls how measures behave", out, "figure_02_join_cardinality.png")
    box(draw, (100, 210, 360, 150), "many_to_one\n\nDimension lookup\nFields allowed\nMeasures allowed", "#DCFCE7", "#16A34A")
    box(draw, (100, 500, 360, 150), "one_to_many\n\nFact expansion\nFields not allowed\nMeasures allowed", "#FEF3C7", "#D97706")
    box(draw, (690, 335, 420, 180), "Metric View engine\naggregates at the right grain\nand avoids source row duplication", "#E0F2FE", "#0284C7")
    arrow(draw, (460, 285), (690, 395))
    arrow(draw, (460, 575), (690, 455))
    box(draw, (1210, 360, 280, 130), "Predictable\nKPI results", "#EEF2FF", "#4F46E5")
    arrow(draw, (1110, 425), (1210, 425))
    img.save(path)


def part4_advanced_overview():
    out = Path("assets/part4_advanced_semantics")
    img, draw, path = canvas("Advanced Metric View semantics", out, "figure_01_advanced_semantics.png")
    box(draw, (80, 345, 300, 130), "Metric View\ntrusted KPI layer", "#E0F2FE", "#0284C7")
    items = [
        ((540, 120, 330, 100), "LOD\npercent of what?", "#EEF2FF", "#4F46E5"),
        ((540, 295, 330, 100), "Windows\nover what time frame?", "#DCFCE7", "#16A34A"),
        ((540, 470, 330, 100), "Composability\nbuild from measures", "#FEF3C7", "#D97706"),
        ((540, 645, 330, 100), "Agent metadata\nbusiness language", "#FCE7F3", "#DB2777"),
    ]
    for xy, text, fill, outline in items:
        box(draw, xy, text, fill, outline)
        arrow(draw, (380, 410), (xy[0], xy[1] + xy[3] // 2))
    draw.text((1010, 350), "Goal: make calculation rules\nexplicit, reusable, and AI-readable.", font=TEXT, fill="#111827")
    img.save(path)


def part3_star_schema_zoom():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("Star schema join", out, "figure_03_star_schema_zoom.png")
    box(draw, (90, 350, 330, 120), "Fact table\ncredit_exposure_fact", "#E0F2FE", "#0284C7")
    dims = [
        ((620, 190, 310, 105), "Dimension\ndim_product", "#DCFCE7", "#16A34A"),
        ((620, 370, 310, 105), "Dimension\ndim_risk_grade", "#DCFCE7", "#16A34A"),
        ((620, 550, 310, 105), "Dimension\ndim_branch", "#DCFCE7", "#16A34A"),
    ]
    for xy, text, fill, outline in dims:
        box(draw, xy, text, fill, outline)
        arrow(draw, (420, 410), (xy[0], xy[1] + xy[3] // 2))
    box(draw, (1120, 350, 360, 130), "Metric View fields\nproduct_line\nrisk_band\nbranch_name", "#EEF2FF", "#4F46E5")
    arrow(draw, (930, 410), (1120, 410))
    draw.text((90, 520), "Use when the fact table has IDs\nand dimensions provide labels.", font=TEXT, fill="#111827")
    img.save(path)


def part3_snowflake_zoom():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("Snowflake schema join", out, "figure_04_snowflake_schema_zoom.png")
    box(draw, (80, 350, 300, 120), "Fact table\ncredit_exposure_fact", "#E0F2FE", "#0284C7")
    box(draw, (520, 350, 300, 120), "Dimension\ndim_branch", "#DCFCE7", "#16A34A")
    box(draw, (960, 350, 300, 120), "Subdimension\ndim_region", "#FEF3C7", "#D97706")
    box(draw, (1320, 350, 220, 120), "Field\nregion_name", "#EEF2FF", "#4F46E5")
    arrow(draw, (380, 410), (520, 410))
    arrow(draw, (820, 410), (960, 410))
    arrow(draw, (1260, 410), (1320, 410))
    draw.text((80, 520), "Use when dimensions are normalized into multiple levels.", font=TEXT, fill="#111827")
    img.save(path)


def part3_rely_zoom():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("many_to_one and rely", out, "figure_05_many_to_one_rely_zoom.png")
    box(draw, (100, 170, 360, 145), "Source row\ncredit_exposure_fact\n\nE001\nP_CARD", "#E0F2FE", "#0284C7")
    box(draw, (610, 170, 360, 145), "Dimension row\ndim_product\n\nP_CARD\nCredit Card", "#DCFCE7", "#16A34A")
    arrow(draw, (460, 242), (610, 242))
    box(draw, (1120, 145, 360, 195), "rely.at_most_one_match\n\nPromise:\nE001 matches at most\none product row", "#FCE7F3", "#DB2777")
    arrow(draw, (970, 242), (1120, 242))
    draw.text((100, 390), "Use for dimension lookups. Do not use rely if the relationship can fan out.", font=TEXT, fill="#111827")
    img.save(path)


def part3_one_to_many_zoom():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("One-to-many join", out, "figure_06_one_to_many_zoom.png")
    box(draw, (90, 150, 350, 100), "customer_spine\nC001  Mass Affluent", "#E0F2FE", "#0284C7")
    box(draw, (90, 310, 350, 100), "customer_spine\nC002  Private Banking", "#E0F2FE", "#0284C7")
    box(draw, (720, 105, 360, 100), "loan_applications\nA001  C001  Approved", "#FEF3C7", "#D97706")
    box(draw, (720, 245, 360, 100), "loan_applications\nA002  C001  Declined", "#FEF3C7", "#D97706")
    box(draw, (720, 385, 360, 100), "loan_applications\nA003  C002  Approved", "#FEF3C7", "#D97706")
    arrow(draw, (440, 200), (720, 155))
    arrow(draw, (440, 200), (720, 295))
    arrow(draw, (440, 360), (720, 435))
    box(draw, (1170, 210, 350, 150), "Meaning\n\nC001 is still one customer\nbut has two applications", "#DCFCE7", "#16A34A")
    arrow(draw, (1080, 295), (1170, 285))
    draw.text((90, 545), "Use when one source entity has many related fact rows.", font=TEXT, fill="#111827")
    img.save(path)


def part3_nested_zoom():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("Nested one-to-many join", out, "figure_07_nested_one_to_many_zoom.png")
    box(draw, (70, 170, 320, 100), "customer_spine\nC001  Mass Affluent", "#E0F2FE", "#0284C7")
    box(draw, (500, 120, 330, 100), "loan_applications\nA001  C001  Approved", "#FEF3C7", "#D97706")
    box(draw, (500, 300, 330, 100), "loan_applications\nA002  C001  Declined", "#FEF3C7", "#D97706")
    box(draw, (940, 120, 360, 100), "application_decisions\nD001  A001  50,000", "#DCFCE7", "#16A34A")
    box(draw, (940, 300, 360, 100), "application_decisions\nD002  A002  0", "#DCFCE7", "#16A34A")
    arrow(draw, (390, 220), (500, 170))
    arrow(draw, (390, 220), (500, 350))
    arrow(draw, (830, 170), (940, 170))
    arrow(draw, (830, 350), (940, 350))
    box(draw, (1340, 210, 210, 120), "Nested path\napplications.decisions", "#F3F4F6", "#6B7280")
    arrow(draw, (1300, 260), (1340, 270))
    draw.text((70, 520), "Use when facts sit multiple levels below the source. Reference nested columns with full dot paths.", font=TEXT, fill="#111827")
    img.save(path)


def part3_sibling_zoom():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("Sibling one-to-many joins", out, "figure_08_sibling_one_to_many_zoom.png")
    box(draw, (90, 235, 330, 100), "customer_spine\nC001  Mass Affluent", "#E0F2FE", "#0284C7")
    box(draw, (620, 120, 360, 90), "loan_applications\nA001  C001", "#FEF3C7", "#D97706")
    box(draw, (620, 245, 360, 90), "loan_applications\nA002  C001", "#FEF3C7", "#D97706")
    box(draw, (620, 410, 360, 90), "service_cases\nS001  C001  Dispute", "#FCE7F3", "#DB2777")
    box(draw, (620, 535, 360, 90), "service_cases\nS002  C001  Limit Increase", "#FCE7F3", "#DB2777")
    arrow(draw, (420, 285), (620, 165))
    arrow(draw, (420, 285), (620, 290))
    arrow(draw, (420, 285), (620, 455))
    arrow(draw, (420, 285), (620, 580))
    box(draw, (1150, 285, 370, 145), "Sibling branches\naggregate independently\nno applications x cases", "#DCFCE7", "#16A34A")
    arrow(draw, (980, 290), (1150, 330))
    arrow(draw, (980, 455), (1150, 385))
    draw.text((90, 690), "Use when independent fact branches share the same source entity. Branches aggregate separately.", font=TEXT, fill="#111827")
    img.save(path)


def part3_bridge_zoom():
    out = Path("assets/part3_metric_view_joins")
    img, draw, path = canvas("Bridge pattern for multiple fact tables", out, "figure_09_bridge_pattern_zoom.png")
    box(draw, (610, 330, 360, 150), "Bridge\nvalid product + branch pairs", "#E0F2FE", "#0284C7")
    box(draw, (90, 180, 360, 120), "Fact\ncredit_exposure_fact", "#FEF3C7", "#D97706")
    box(draw, (90, 540, 360, 120), "Fact\nfraud_event_fact", "#FEF3C7", "#D97706")
    arrow(draw, (450, 240), (610, 380))
    arrow(draw, (450, 600), (610, 430))
    box(draw, (1160, 330, 350, 150), "Measures\nexposure + fraud loss\nsame shared dimensions", "#DCFCE7", "#16A34A")
    arrow(draw, (970, 405), (1160, 405))
    draw.text((90, 700), "Use when multiple fact tables share dimensions but no single fact table should be the source spine.", font=TEXT, fill="#111827")
    img.save(path)


if __name__ == "__main__":
    part3_joins_overview()
    part3_cardinality()
    part3_star_schema_zoom()
    part3_snowflake_zoom()
    part3_rely_zoom()
    part3_one_to_many_zoom()
    part3_nested_zoom()
    part3_sibling_zoom()
    part3_bridge_zoom()
    part4_advanced_overview()
    print("Generated Part 3 and Part 4 diagrams.")
