#!/usr/bin/env python3
"""
警察庁オープンデータ（e-Stat CSV）インポートスクリプト

使い方:
    python import_estat.py <CSVファイルパス> [--year 2023] [--dry-run]

対応フォーマット:
    e-Stat 第6表: 日付,地方,area_code,都道府県,...,罪種,認知件数,検挙件数,検挙人員
    e-Stat 第3表: 都道府県,認知件数【件】,検挙件数【件】,...
    警察白書形式: 都道府県,罪種,認知件数,...（ヘッダーを自動判別）
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

    inserted = skipped = 0
    db = SessionLocal()
    try:
        for rec in records:
            pref_info = _lookup_pref(rec["prefecture_name"])
            if pref_info is None:
                print(f"  [SKIP] 都道府県名不明: {rec['prefecture_name']}")
                skipped += 1
                continue
            code, lat, lng = pref_info

            crime_type = _map_crime_type(rec["crime_category"])

            # 重複チェック（year + prefecture_code + crime_category）
            exists = db.query(PrefectureStats).filter_by(
                year=rec["year"],
                prefecture_code=code,
                crime_category=rec["crime_category"],
            ).first()
            if exists:
                skipped += 1
                continue

            if not dry_run:
                row = PrefectureStats(
                    year            = rec["year"],
                    prefecture_code = code,
                    prefecture_name = pref_info[0] if False else  # 名前は別途解決
                                      next(n for c, n, *_ in PREFECTURE_MASTER if c == code),
                    crime_category  = rec["crime_category"],
                    crime_type      = crime_type,
                    count_recognized= rec["count_recognized"],
                    count_cleared   = rec["count_cleared"],
                    count_arrested  = rec["count_arrested"],
                    location        = f"SRID=4326;POINT({lng} {lat})",
                )
                db.add(row)
            inserted += 1

        if not dry_run:
            db.commit()
    finally:
        db.close()
    return inserted, skipped


# ── エントリポイント ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="警察庁 e-Stat CSV インポート")
    parser.add_argument("csv_file", help="インポートする CSV ファイルのパス")
    parser.add_argument("--year", type=int, default=None,
                        help="CSV に年列がない場合の年（例: 2023）")
    parser.add_argument("--dry-run", action="store_true",
                        help="DB への書き込みをせず件数だけ表示")
    args = parser.parse_args()

    path = Path(args.csv_file)
    if not path.exists():
        print(f"エラー: ファイルが見つかりません: {path}", file=sys.stderr)
        sys.exit(1)

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

    # サンプル表示
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
