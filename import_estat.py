#!/usr/bin/env python3
"""
警察庁オープンデータ CSVインポートスクリプト

使い方:
    python import_estat.py <CSVファイルパス> [--year 2023] [--dry-run]

対応フォーマット:
    [自動判別A] NPA月次統計 (r08_1-1.csv 等):
        第3表: 刑法犯総数 都道府県別
        第6表: 重要犯罪・重要窃盗犯 都道府県別（罪種別）
    [自動判別B] e-Stat 第6表: 日付,地方,area_code,都道府県,...,罪種,認知件数,...
    [自動判別C] e-Stat 第3表: 都道府県,認知件数【件】,検挙件数【件】,...
"""
import sys
import csv
import argparse
from pathlib import Path

# ── 都道府県コード・座標マスター ──────────────────────────────────────────────
# (code, name, lat, lng)
PREFECTURE_MASTER = [
    ("01", "北海道",   43.0642, 141.3469),
    ("02", "青森県",   40.8244, 140.7400),
    ("03", "岩手県",   39.7036, 141.1527),
    ("04", "宮城県",   38.2688, 140.8721),
    ("05", "秋田県",   39.7186, 140.1023),
    ("06", "山形県",   38.2404, 140.3636),
    ("07", "福島県",   37.7608, 140.4747),
    ("08", "茨城県",   36.3418, 140.4468),
    ("09", "栃木県",   36.5657, 139.8836),
    ("10", "群馬県",   36.3911, 139.0608),
    ("11", "埼玉県",   35.8570, 139.6489),
    ("12", "千葉県",   35.6047, 140.1233),
    ("13", "東京都",   35.6894, 139.6917),
    ("14", "神奈川県", 35.4475, 139.6425),
    ("15", "新潟県",   37.9023, 139.0235),
    ("16", "富山県",   36.6953, 137.2113),
    ("17", "石川県",   36.5944, 136.6256),
    ("18", "福井県",   36.0652, 136.2219),
    ("19", "山梨県",   35.6642, 138.5681),
    ("20", "長野県",   36.6513, 138.1810),
    ("21", "岐阜県",   35.3912, 136.7223),
    ("22", "静岡県",   34.9769, 138.3831),
    ("23", "愛知県",   35.1802, 136.9066),
    ("24", "三重県",   34.7303, 136.5086),
    ("25", "滋賀県",   35.0045, 135.8686),
    ("26", "京都府",   35.0211, 135.7556),
    ("27", "大阪府",   34.6863, 135.5200),
    ("28", "兵庫県",   34.6913, 135.1830),
    ("29", "奈良県",   34.6851, 135.8328),
    ("30", "和歌山県", 34.2261, 135.1675),
    ("31", "鳥取県",   35.5036, 134.2383),
    ("32", "島根県",   35.4722, 133.0505),
    ("33", "岡山県",   34.6617, 133.9344),
    ("34", "広島県",   34.3963, 132.4596),
    ("35", "山口県",   34.1861, 131.4706),
    ("36", "徳島県",   34.0658, 134.5593),
    ("37", "香川県",   34.3401, 134.0434),
    ("38", "愛媛県",   33.8416, 132.7657),
    ("39", "高知県",   33.5597, 133.5311),
    ("40", "福岡県",   33.6064, 130.4183),
    ("41", "佐賀県",   33.2494, 130.2988),
    ("42", "長崎県",   32.7503, 129.8777),
    ("43", "熊本県",   32.7898, 130.7417),
    ("44", "大分県",   33.2382, 131.6126),
    ("45", "宮崎県",   31.9110, 131.4239),
    ("46", "鹿児島県", 31.5602, 130.5581),
    ("47", "沖縄県",   26.2124, 127.6809),
]

# 都道府県名 → (code, lat, lng)
_PREF_BY_NAME = {
    name: (code, lat, lng)
    for code, name, lat, lng in PREFECTURE_MASTER
}
# 正式名に揺れを吸収する別名マップ
_PREF_ALIAS = {
    "北海道": "北海道",
    "東京":   "東京都",
    "大阪":   "大阪府",
    "京都":   "京都府",
    "神奈川": "神奈川県",
    "和歌山": "和歌山県",
    "鹿児島": "鹿児島県",
}


def _lookup_pref(name: str):
    """都道府県名から (code, lat, lng) を返す。見つからなければ None。"""
    name = name.strip()
    if name in _PREF_BY_NAME:
        return _PREF_BY_NAME[name]
    # 別名解決
    canonical = _PREF_ALIAS.get(name)
    if canonical and canonical in _PREF_BY_NAME:
        return _PREF_BY_NAME[canonical]
    # 部分一致（最後の手段）
    for full_name, val in _PREF_BY_NAME.items():
        if name in full_name or full_name in name:
            return val
    return None


# ── 罪種マッピング（警察庁分類 → 既存 crime_type） ───────────────────────────
CRIME_TYPE_MAP = {
    # 凶悪犯
    "殺人":             "殺人・傷害致死",
    "強盗":             "暴行・傷害",
    "放火":             "その他",
    "不同意性交等":     "性犯罪",
    "強姦":             "性犯罪",
    "強制性交等":       "性犯罪",
    # 粗暴犯
    "暴行":             "暴行・傷害",
    "傷害":             "暴行・傷害",
    "傷害致死":         "殺人・傷害致死",
    "脅迫":             "暴行・傷害",
    "恐喝":             "暴行・傷害",
    "凶器準備集合":     "暴行・傷害",
    # 窃盗犯
    "窃盗":             "窃盗・万引き",
    "侵入盗":           "窃盗・万引き",
    "乗り物盗":         "窃盗・万引き",
    "非侵入盗":         "窃盗・万引き",
    "ひったくり":       "窃盗・万引き",
    "すり":             "窃盗・万引き",
    "自動車盗":         "窃盗・万引き",
    # 知能犯
    "詐欺":             "詐欺",
    "横領":             "詐欺",
    "偽造":             "詐欺",
    "背任":             "詐欺",
    "汚職":             "詐欺",
    # 風俗犯
    "わいせつ":         "性犯罪",
    "強制わいせつ":     "性犯罪",
    "不同意わいせつ":   "性犯罪",
    "賭博":             "その他",
    # その他
    "住居侵入":         "その他",
    "器物損壊":         "その他",
    "公務執行妨害":     "その他",
    "略取誘拐":         "その他",
    "人身売買":         "その他",
    "占有離脱物横領":   "その他",
}

# 大分類キーワード → crime_type（細分類にマッチしない場合のフォールバック）
_CATEGORY_FALLBACK = {
    "凶悪犯":   "殺人・傷害致死",
    "粗暴犯":   "暴行・傷害",
    "窃盗犯":   "窃盗・万引き",
    "知能犯":   "詐欺",
    "風俗犯":   "性犯罪",
    "重要犯罪": "その他",
}


def _map_crime_type(category: str) -> str:
    s = category.strip()
    if s in CRIME_TYPE_MAP:
        return CRIME_TYPE_MAP[s]
    for key, val in _CATEGORY_FALLBACK.items():
        if key in s:
            return val
    return "その他"


# ── CSV 読み込み（文字コード自動判別） ────────────────────────────────────────

def _read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    for enc in ("utf-8-sig", "utf-8", "shift-jis", "cp932"):
        try:
            text = path.read_text(encoding=enc)
            reader = csv.reader(text.splitlines())
            rows = [r for r in reader if any(c.strip() for c in r)]
            if rows:
                return rows[0], rows[1:]
        except (UnicodeDecodeError, ValueError):
            continue
    raise ValueError(f"文字コードを判別できませんでした: {path}")


# ── フォーマット判別・パース ──────────────────────────────────────────────────

def _parse_int(s: str) -> int | None:
    s = s.strip().replace(",", "").replace("−", "").replace("-", "").replace("－", "")
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def parse_rows(headers: list[str], rows: list[list[str]], default_year: int | None):
    """
    CSVの行を解析して dict のリストを返す。
    各 dict: year, prefecture_name, crime_category, count_recognized,
             count_cleared, count_arrested
    """
    h = [c.strip() for c in headers]
    records = []

    # ── フォーマット A: e-Stat 第6表（日付列あり）──────────────────────────
    # 日付,地方,area_code,都道府県,...,罪種,認知件数,検挙件数,検挙人員
    if "日付" in h and "罪種" in h:
        idx = {name: h.index(name) for name in h}
        for row in rows:
            if len(row) <= max(idx.values()):
                continue
            year_str = row[idx["日付"]][:4]
            year = int(year_str) if year_str.isdigit() else default_year
            pref = row[idx.get("都道府県", idx.get("prefecture", 0))].strip()
            crime = row[idx["罪種"]].strip()
            recognized = _parse_int(row[idx["認知件数"]]) if "認知件数" in idx else None
            cleared    = _parse_int(row[idx["検挙件数"]]) if "検挙件数" in idx else None
            arrested   = _parse_int(row[idx.get("検挙人員", "")]) if "検挙人員" in idx else None
            # 「全国」「管区」行はスキップ
            if not pref or pref in ("全国", "計") or "管区" in pref:
                continue
            records.append(dict(year=year, prefecture_name=pref,
                                crime_category=crime,
                                count_recognized=recognized,
                                count_cleared=cleared,
                                count_arrested=arrested))
        return records

    # ── フォーマット B: 都道府県列 + 罪種列あり（フォーマットA以外）──────────
    pref_col   = next((i for i, c in enumerate(h) if "都道府県" in c or c == "prefecture"), None)
    crime_col  = next((i for i, c in enumerate(h) if "罪種" in c or "category" in c.lower()), None)
    recog_col  = next((i for i, c in enumerate(h) if "認知件数" in c), None)
    clear_col  = next((i for i, c in enumerate(h) if "検挙件数" in c), None)
    arrest_col = next((i for i, c in enumerate(h) if "検挙人員" in c), None)
    year_col   = next((i for i, c in enumerate(h) if "年" == c or "year" in c.lower()), None)

    if pref_col is not None and recog_col is not None:
        for row in rows:
            if not row or len(row) <= recog_col:
                continue
            pref  = row[pref_col].strip()
            crime = row[crime_col].strip() if crime_col is not None else "総数"
            year  = int(row[year_col]) if year_col is not None and row[year_col].isdigit() \
                    else default_year
            if not pref or pref in ("全国", "計") or "管区" in pref:
                continue
            records.append(dict(
                year=year,
                prefecture_name=pref,
                crime_category=crime,
                count_recognized=_parse_int(row[recog_col]),
                count_cleared=_parse_int(row[clear_col]) if clear_col else None,
                count_arrested=_parse_int(row[arrest_col]) if arrest_col else None,
            ))
        return records

    raise ValueError(
        f"対応していないCSVフォーマットです。\nヘッダー: {headers}\n"
        "対応フォーマット: e-Stat第6表（日付・罪種列あり）または都道府県・認知件数列あり"
    )


# ── DB インサート ─────────────────────────────────────────────────────────────

def _insert(records: list[dict], dry_run: bool) -> tuple[int, int]:
    from database import SessionLocal
    from models import PrefectureStats
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    skipped = 0

    # レコードを行リストに変換しつつ、都道府県名不明をスキップ
    rows: list[dict] = []
    for rec in records:
        pref_info = _lookup_pref(rec["prefecture_name"])
        if pref_info is None:
            print(f"  [SKIP] 都道府県名不明: {rec['prefecture_name']}")
            skipped += 1
            continue
        code, lat, lng = pref_info
        prefecture_name = next(n for c, n, *_ in PREFECTURE_MASTER if c == code)
        crime_type = _map_crime_type(rec["crime_category"])
        rows.append({
            "year":             rec["year"],
            "prefecture_code":  code,
            "prefecture_name":  prefecture_name,
            "crime_category":   rec["crime_category"],
            "crime_type":       crime_type,
            "count_recognized": rec["count_recognized"],
            "count_cleared":    rec.get("count_cleared"),
            "count_arrested":   rec.get("count_arrested"),
            "location":         f"SRID=4326;POINT({lng} {lat})",
            "source":           "npa_estat",
        })

    # CSV内重複除去（同一 year+prefecture_code+crime_category は後勝ち）
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row["year"], row["prefecture_code"], row["crime_category"])
        seen[key] = row
    deduped = list(seen.values())
    intra_dups = len(rows) - len(deduped)
    if intra_dups:
        print(f"  [INFO] CSV内重複 {intra_dups} 件を除去")
    skipped += intra_dups

    if dry_run:
        return len(deduped), skipped

    # UPSERT（ON CONFLICT DO UPDATE）
    db = SessionLocal()
    try:
        stmt = pg_insert(PrefectureStats).values(deduped)
        stmt = stmt.on_conflict_do_update(
            constraint="prefecture_stats_year_prefecture_code_crime_category_key",
            set_={
                "crime_type":       stmt.excluded.crime_type,
                "count_recognized": stmt.excluded.count_recognized,
                "count_cleared":    stmt.excluded.count_cleared,
                "count_arrested":   stmt.excluded.count_arrested,
                "location":         stmt.excluded.location,
                "source":           stmt.excluded.source,
            },
        )
        db.execute(stmt)
        db.commit()
    finally:
        db.close()

    return len(deduped), skipped


# ── NPA月次統計フォーマット（第X表形式）パーサー ─────────────────────────────

def _read_npa_csv(path: Path) -> list[list[str]]:
    """NPA月次統計CSVを読む（Shift-JIS、csv.readerで複数行セルも正しく処理）"""
    for enc in ("shift-jis", "cp932", "utf-8-sig", "utf-8"):
        try:
            with open(path, encoding=enc, newline="", errors="replace") as f:
                return list(csv.reader(f))
        except (UnicodeDecodeError, ValueError):
            continue
    raise ValueError(f"文字コードを判別できません: {path}")


def _year_from_reiwa_filename(filename: str) -> int | None:
    """ファイル名 r08_... から西暦を推定（令和n年 = 2018+n）"""
    import re
    m = re.match(r"[rR](\d+)_", Path(filename).name)
    return 2018 + int(m.group(1)) if m else None


def _pref_from_row(col0: str, col1: str) -> str | None:
    """第3表/第6表の行から都道府県名を返す。スキップ行はNone"""
    c0, c1 = col0.strip(), col1.strip()
    if c0 == "北海道" and c1 == "計":
        return "北海道"
    if c0 == "東京都" and not c1:
        return "東京都"
    if c1.endswith(("府", "県")):
        return c1
    return None


def _parse_npa_table_section(rows: list[list[str]], start: int, end: int,
                              year: int, crime_category: str) -> list[dict]:
    """第3表/第6表の1セクションを解析してrecordリストを返す"""
    # データ行: col[0]が都道府県または地方名、col[2]が認知件数（当年）
    records = []
    for row in rows[start:end]:
        if len(row) < 11:
            continue
        pref = _pref_from_row(row[0], row[1])
        if not pref:
            continue
        try:
            recognized = _parse_int(row[2])
            cleared    = _parse_int(row[6])
            arrested   = _parse_int(row[10])
        except IndexError:
            continue
        if recognized is None:
            continue
        records.append(dict(
            year             = year,
            prefecture_name  = pref,
            crime_category   = crime_category,
            count_recognized = recognized,
            count_cleared    = cleared,
            count_arrested   = arrested,
        ))
    return records


def parse_npa_monthly(path: Path, year: int | None) -> list[dict]:
    """
    NPA月次統計CSV（r08_1-1.csv 形式）を解析する。
    第3表（刑法犯総数）と第6表（重要犯罪別）の都道府県別データを取得。
    """
    rows = _read_npa_csv(path)

    # 年をファイル名から推定（--year 未指定時）
    if year is None:
        year = _year_from_reiwa_filename(str(path))
    if year is None:
        raise ValueError("年を特定できません。--year オプションで指定してください。")

    records = []

    # セクション境界を検出
    section_starts: list[tuple[int, str]] = []  # (行番号, セクション名)
    for i, row in enumerate(rows):
        if not row or not row[0]:
            continue
        cell0 = row[0].strip()
        # 第3表: 刑法犯総数 都道府県別
        if cell0 == "第３表" or (cell0.startswith("第") and "刑法犯総数" in "".join(row[:6])):
            section_starts.append((i, "刑法犯総数"))
        # 第4表: 窃盗犯総数 都道府県別
        elif cell0 == "第４表" and any("窃盗" in c for c in row[:6]):
            section_starts.append((i, "窃盗犯"))
        # 第6表: 重要犯罪 各罪種（括弧内の犯罪名を抽出）
        elif cell0 == "第６表":
            header_text = "".join(row)
            import re
            m = re.search(r"[（(]([^）)]+)[）)]", header_text)
            if m:
                crime_name = m.group(1).strip()
                # "総数" や "住宅対象" などの集計行は除外
                if crime_name not in ("重要犯罪総数", "重要窃盗犯総数",
                                       "侵入盗−住宅対象", "侵入盗−その他"):
                    section_starts.append((i, crime_name))

    if not section_starts:
        raise ValueError("第3表・第6表のセクションが見つかりませんでした。")

    # 各セクションを解析
    for idx, (start_row, crime_cat) in enumerate(section_starts):
        # 次セクションの直前まで、または末尾まで
        end_row = section_starts[idx + 1][0] if idx + 1 < len(section_starts) else len(rows)
        # ヘッダー行をスキップ（データ行: col[2]が数値になる行まで読み飛ばす）
        data_start = start_row + 1
        for j in range(start_row + 1, min(start_row + 15, end_row)):
            if len(rows[j]) > 2 and _parse_int(rows[j][2]) is not None:
                data_start = j
                break
        recs = _parse_npa_table_section(rows, data_start, end_row, year, crime_cat)
        records.extend(recs)
        print(f"      {crime_cat}: {len(recs)} 件")

    return records


def _is_npa_monthly_format(path: Path) -> bool:
    """ファイルの先頭を見てNPA月次統計フォーマットか判定"""
    for enc in ("shift-jis", "cp932", "utf-8-sig", "utf-8"):
        try:
            first = path.read_text(encoding=enc, errors="replace")[:200]
            return "第１表" in first or "第1表" in first
        except Exception:
            continue
    return False


# ── エントリポイント ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="警察庁 CSV インポート（e-Stat / NPA月次統計 対応）")
    parser.add_argument("csv_file", help="インポートする CSV ファイルのパス")
    parser.add_argument("--year", type=int, default=None,
                        help="CSV に年列がない場合の年（例: 2026）")
    parser.add_argument("--dry-run", action="store_true",
                        help="DB への書き込みをせず件数だけ表示")
    args = parser.parse_args()

    path = Path(args.csv_file)
    if not path.exists():
        print(f"エラー: ファイルが見つかりません: {path}", file=sys.stderr)
        sys.exit(1)

    # ── フォーマット自動判別 ──────────────────────────────────────────────────
    if _is_npa_monthly_format(path):
        print(f"[フォーマット] NPA月次統計（第X表形式）を検出")
        print(f"[1/3] CSV 解析中: {path}")
        try:
            records = parse_npa_monthly(path, args.year)
        except ValueError as e:
            print(f"エラー: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"[フォーマット] e-Stat形式を検出")
        print(f"[1/3] CSV 読み込み中: {path}")
        headers, rows = _read_csv(path)
        print(f"      ヘッダー: {headers}")
        print(f"      データ行数: {len(rows)}")
        print("[2/3] パース中...")
        try:
            records = parse_rows(headers, rows, args.year)
        except ValueError as e:
            print(f"エラー: {e}", file=sys.stderr)
            sys.exit(1)

    print(f"      解析レコード数: {len(records)}")

    if not records:
        print("取り込み対象レコードがありません。")
        return

    print("      サンプル (最初の3件):")
    for r in records[:3]:
        print(f"        {r}")

    label = "[DRY-RUN] " if args.dry_run else ""
    print(f"[3/3] {label}DB インサート中...")
    inserted, skipped = _insert(records, args.dry_run)
    print(f"      挿入: {inserted} 件 / スキップ（重複・不明）: {skipped} 件")
    if args.dry_run:
        print("      ※ --dry-run モードのため実際には書き込まれていません")
    else:
        print("      完了！")


if __name__ == "__main__":
    main()
