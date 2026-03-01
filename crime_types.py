"""
犯罪種別の正典定義（日本の警察庁・警視庁統計分類に準拠）

第1階層 (crime_law)    : 刑法犯 / 特別法犯 / 重点犯罪
第2階層 (crime_category): 凶悪犯 / 粗暴犯 / 窃盗犯 / 知能犯 / 風俗犯 /
                          その他の刑法犯 / 特別法犯 / 重点犯罪
第3階層 (incident_type) : 個別罪名

このモジュールはバックエンド全体（クローラー・e-Statインポート・APIなど）で共有する。
フロントエンドの対応定義は lib/crimeTypes.ts に同期すること。
"""

from typing import Dict, List

# ── 階層定義 ─────────────────────────────────────────────────────────────────
# { 第1階層: { 第2階層: [第3階層, ...], ... }, ... }

CRIME_HIERARCHY: Dict[str, Dict[str, List[str]]] = {
    "刑法犯": {
        "凶悪犯": [
            "殺人",
            "強盗",
            "放火",
            "強制性交等",
        ],
        "粗暴犯": [
            "暴行",
            "傷害",
            "脅迫",
            "恐喝",
        ],
        "窃盗犯": [
            "空き巣",
            "侵入盗",
            "車上ねらい",
            "ひったくり",
            "自転車盗",
            "自動車盗",
            "万引き",
        ],
        "知能犯": [
            "詐欺",
            "横領",
            "偽造",
            "背任",
        ],
        "風俗犯": [
            "賭博",
            "わいせつ物頒布",
            "公然わいせつ",
        ],
        "その他の刑法犯": [
            "器物損壊",
            "住居侵入",
            "業務妨害",
            "その他刑法犯",
        ],
    },
    "特別法犯": {
        "特別法犯": [
            "道路交通法違反",
            "覚醒剤取締法違反",
            "銃砲刀剣類所持等取締法違反",
            "軽犯罪法違反",
            "児童買春・ポルノ禁止法違反",
        ],
    },
    "重点犯罪": {
        "重点犯罪": [
            "特殊詐欺",
            "組織犯罪",
            "サイバー犯罪",
            "DV・ストーカー事案",
            "児童虐待関連事案",
        ],
    },
}

# ── 派生データ ────────────────────────────────────────────────────────────────

# 第3階層 → 第2階層 (incident_type → crime_category)
INCIDENT_TO_CATEGORY: Dict[str, str] = {
    crime: category
    for law_group in CRIME_HIERARCHY.values()
    for category, crimes in law_group.items()
    for crime in crimes
}

# 第3階層 → 第1階層 (incident_type → crime_law)
INCIDENT_TO_LAW: Dict[str, str] = {
    crime: law
    for law, law_group in CRIME_HIERARCHY.items()
    for crimes in law_group.values()
    for crime in crimes
}

# 第2階層リスト（順序保持）
ALL_CATEGORIES: List[str] = [
    category
    for law_group in CRIME_HIERARCHY.values()
    for category in law_group.keys()
]

# 第3階層リスト（フラット・順序保持）
ALL_INCIDENT_TYPES: List[str] = [
    crime
    for law_group in CRIME_HIERARCHY.values()
    for crimes in law_group.values()
    for crime in crimes
]

# AI抽出プロンプト用の選択肢文字列
INCIDENT_TYPES_FOR_PROMPT: str = "|".join(ALL_INCIDENT_TYPES)

# ── ユーティリティ ────────────────────────────────────────────────────────────

def get_crime_category(incident_type: str) -> str:
    """第3階層の罪名から第2階層のカテゴリを返す。不明なら 'その他の刑法犯'。"""
    return INCIDENT_TO_CATEGORY.get(incident_type, "その他の刑法犯")


def get_crime_law(incident_type: str) -> str:
    """第3階層の罪名から第1階層の法区分を返す。不明なら '刑法犯'。"""
    return INCIDENT_TO_LAW.get(incident_type, "刑法犯")


# ── e-Stat / NPA 罪種マッピング ───────────────────────────────────────────────
# 警察庁統計の罪種名 → 第3階層 incident_type への変換テーブル

ESTAT_TO_INCIDENT: Dict[str, str] = {
    # 凶悪犯
    "殺人":             "殺人",
    "殺人既遂":         "殺人",
    "強盗":             "強盗",
    "強盗致死傷":       "強盗",
    "放火":             "放火",
    "不同意性交等":     "強制性交等",
    "強姦":             "強制性交等",
    "強制性交等":       "強制性交等",
    # 粗暴犯
    "暴行":             "暴行",
    "傷害":             "傷害",
    "傷害致死":         "傷害",
    "脅迫":             "脅迫",
    "恐喝":             "恐喝",
    "凶器準備集合":     "暴行",
    # 窃盗犯
    "窃盗":             "万引き",       # 汎用窃盗はフォールバック
    "侵入盗":           "侵入盗",
    "空き巣":           "空き巣",
    "乗り物盗":         "自動車盗",
    "自動車盗":         "自動車盗",
    "自転車盗":         "自転車盗",
    "車上ねらい":       "車上ねらい",
    "ひったくり":       "ひったくり",
    "すり":             "ひったくり",
    "非侵入盗":         "万引き",
    "万引き":           "万引き",
    # 知能犯
    "詐欺":             "詐欺",
    "横領":             "横領",
    "遺失物等横領":     "横領",
    "占有離脱物横領":   "横領",
    "偽造":             "偽造",
    "文書偽造":         "偽造",
    "背任":             "背任",
    "汚職":             "背任",
    # 風俗犯
    "わいせつ":         "公然わいせつ",
    "強制わいせつ":     "公然わいせつ",
    "不同意わいせつ":   "公然わいせつ",
    "公然わいせつ":     "公然わいせつ",
    "わいせつ物頒布等": "わいせつ物頒布",
    "賭博":             "賭博",
    # その他の刑法犯
    "住居侵入":         "住居侵入",
    "器物損壊":         "器物損壊",
    "公務執行妨害":     "業務妨害",
    "業務妨害":         "業務妨害",
    "略取誘拐":         "その他刑法犯",
    "人身売買":         "その他刑法犯",
    # 特別法犯
    "道路交通法違反":   "道路交通法違反",
    "覚醒剤":           "覚醒剤取締法違反",
    "覚醒剤取締法":     "覚醒剤取締法違反",
    "銃刀法":           "銃砲刀剣類所持等取締法違反",
    "軽犯罪法":         "軽犯罪法違反",
    "児童買春":         "児童買春・ポルノ禁止法違反",
    "児童ポルノ":       "児童買春・ポルノ禁止法違反",
    # 重点犯罪
    "特殊詐欺":         "特殊詐欺",
    "サイバー犯罪":     "サイバー犯罪",
}

# 第2階層キーワードからのフォールバック
CATEGORY_FALLBACK_INCIDENT: Dict[str, str] = {
    "凶悪犯":   "殺人",
    "粗暴犯":   "暴行",
    "窃盗犯":   "万引き",
    "知能犯":   "詐欺",
    "風俗犯":   "公然わいせつ",
    "重要犯罪": "その他刑法犯",
    "重点犯罪": "特殊詐欺",
}


def map_estat_to_incident(npa_crime_name: str) -> str:
    """
    警察庁統計の罪種名を第3階層 incident_type に変換する。
    完全一致 → カテゴリキーワード部分一致 → 'その他刑法犯' の順で試みる。
    """
    s = npa_crime_name.strip()
    if s in ESTAT_TO_INCIDENT:
        return ESTAT_TO_INCIDENT[s]
    for key, val in CATEGORY_FALLBACK_INCIDENT.items():
        if key in s:
            return val
    return "その他刑法犯"
