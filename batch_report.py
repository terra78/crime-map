"""
バッチレポート生成モジュール
- generate_csv_monthly / generate_csv_yearly : CSV出力（UTF-8 BOM、Excel対応）
- generate_pdf_monthly / generate_pdf_yearly : PDF出力（日本語フォント自動検出）
"""
import csv
import os
from pathlib import Path
from database import SessionLocal
from models import Report


# ── DBヘルパー ────────────────────────────────────────────────────────────────

def _approved_monthly(year: int, month: int):
    from sqlalchemy import extract
    db = SessionLocal()
    try:
        return (
            db.query(Report)
            .filter(
                Report.status.in_(["ai_approved", "human_approved"]),
                extract("year",  Report.occurred_at) == year,
                extract("month", Report.occurred_at) == month,
            )
            .order_by(Report.occurred_at)
            .all()
        )
    finally:
        db.close()


def _approved_yearly(year: int):
    from sqlalchemy import extract
    db = SessionLocal()
    try:
        return (
            db.query(Report)
            .filter(
                Report.status.in_(["ai_approved", "human_approved"]),
                extract("year", Report.occurred_at) == year,
            )
            .order_by(Report.occurred_at)
            .all()
        )
    finally:
        db.close()


# ── CSV ───────────────────────────────────────────────────────────────────────

def _to_rows(reports) -> list[dict]:
    rows = []
    for r in reports:
        row = {
            "id":          r.id,
            "title":       r.title or "",
            "address":     r.address or "",
            "occurred_at": str(r.occurred_at) if r.occurred_at else "",
            "status":      r.status,
            "ai_score":    r.ai_score if r.ai_score is not None else "",
            "source_url":  r.source_url or "",
            "archive_url": r.archive_url or "",
            "created_at":  str(r.created_at) if r.created_at else "",
        }
        for k, v in (r.data or {}).items():
            row[f"data_{k}"] = v
        rows.append(row)
    return rows


def _write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("no data\n", encoding="utf-8-sig")
        return
    fieldnames = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def generate_csv_monthly(year: int, month: int, path: Path) -> None:
    _write_csv(_to_rows(_approved_monthly(year, month)), path)


def generate_csv_yearly(year: int, path: Path) -> None:
    _write_csv(_to_rows(_approved_yearly(year)), path)


# ── PDF（reportlab） ──────────────────────────────────────────────────────────

# 日本語フォント候補（Linux/Render + macOS）
_JP_FONT_CANDIDATES = [
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/noto-cjk/NotoSansCJKjp-Regular.otf",
    "/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc",
    # macOS
    "/System/Library/Fonts/Supplemental/Arial Unicode MS.ttf",
    "/Library/Fonts/Arial Unicode.ttf",
]
_JP_FONT_NAME = "Helvetica"  # フォントが見つからない場合のフォールバック


def _ensure_jp_font() -> str:
    """日本語フォントを登録して返す。見つからなければ Helvetica を返す。"""
    global _JP_FONT_NAME
    if _JP_FONT_NAME != "Helvetica":
        return _JP_FONT_NAME

    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        for p in _JP_FONT_CANDIDATES:
            if os.path.exists(p):
                try:
                    pdfmetrics.registerFont(TTFont("JpFont", p))
                    _JP_FONT_NAME = "JpFont"
                    return _JP_FONT_NAME
                except Exception:
                    continue
    except Exception:
        pass
    return "Helvetica"


def _count_by(reports, field: str) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for r in reports:
        val = (r.data or {}).get(field) or "不明"
        counts[val] = counts.get(val, 0) + 1
    return sorted(counts.items(), key=lambda x: -x[1])


def _build_pdf_story(reports, title: str):
    from reportlab.lib          import colors
    from reportlab.lib.units    import mm
    from reportlab.lib.styles   import ParagraphStyle
    from reportlab.platypus     import Paragraph, Spacer, Table, TableStyle

    font = _ensure_jp_font()

    def style(name, size, space_after=4, bold=False):
        return ParagraphStyle(
            name, fontName=font, fontSize=size, spaceAfter=space_after,
            leading=size * 1.5,
        )

    hdr_color  = colors.HexColor("#2a2d3a")
    row_colors = [colors.white, colors.HexColor("#f5f5f5")]
    base_ts    = [
        ("FONTNAME",      (0, 0), (-1, -1), font),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("BACKGROUND",    (0, 0), (-1,  0), hdr_color),
        ("TEXTCOLOR",     (0, 0), (-1,  0), colors.white),
        ("GRID",          (0, 0), (-1, -1), 0.4, colors.grey),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), row_colors),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
    ]

    story = []

    # タイトル・総件数
    story.append(Paragraph(title, style("title", 16, 6)))
    story.append(Paragraph(f"総件数: {len(reports)} 件", style("sub", 10, 10)))

    if not reports:
        story.append(Paragraph("対象期間にデータがありません。", style("body", 10)))
        return story

    # 種別内訳テーブル
    story.append(Paragraph("■ 種別内訳", style("h2", 11, 4)))
    ct_rows = [["種別", "件数"]] + [[k, str(v)] for k, v in _count_by(reports, "crime_type")]
    t = Table(ct_rows, colWidths=[100 * mm, 30 * mm])
    t.setStyle(TableStyle(base_ts))
    story.append(t)
    story.append(Spacer(1, 6 * mm))

    # 国籍内訳テーブル
    story.append(Paragraph("■ 国籍別内訳", style("h2", 11, 4)))
    nat_rows = [["国籍", "件数"]] + [[k, str(v)] for k, v in _count_by(reports, "nationality")]
    t2 = Table(nat_rows, colWidths=[100 * mm, 30 * mm])
    t2.setStyle(TableStyle(base_ts))
    story.append(t2)
    story.append(Spacer(1, 8 * mm))

    # 投稿一覧テーブル
    story.append(Paragraph("■ 投稿一覧", style("h2", 11, 4)))
    list_headers = ["ID", "発生日", "住所", "種別", "国籍", "タイトル"]
    list_rows = [list_headers]
    for r in reports:
        d = r.data or {}
        list_rows.append([
            str(r.id),
            str(r.occurred_at) if r.occurred_at else "",
            (r.address or "")[:28],
            d.get("crime_type", "")[:14],
            d.get("nationality", "")[:10],
            (r.title or "")[:30],
        ])
    col_w = [12 * mm, 22 * mm, 44 * mm, 36 * mm, 26 * mm, 42 * mm]
    t3 = Table(list_rows, colWidths=col_w, repeatRows=1)
    t3.setStyle(TableStyle(base_ts + [("FONTSIZE", (0, 1), (-1, -1), 8)]))
    story.append(t3)

    return story


def _write_pdf(reports, title: str, path: Path) -> None:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units    import mm
    from reportlab.platypus     import SimpleDocTemplate

    doc = SimpleDocTemplate(
        str(path), pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=20 * mm, bottomMargin=20 * mm,
        title=title,
    )
    doc.build(_build_pdf_story(reports, title))


def generate_pdf_monthly(year: int, month: int, path: Path) -> None:
    _write_pdf(
        _approved_monthly(year, month),
        f"{year}年{month}月 事件レポート",
        path,
    )


def generate_pdf_yearly(year: int, path: Path) -> None:
    _write_pdf(
        _approved_yearly(year),
        f"{year}年 事件レポート（年次）",
        path,
    )
