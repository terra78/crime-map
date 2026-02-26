"""
魚拓（archive.org）自動保存モジュール

公的機関（警察庁・裁判所・官公庁等）以外のURLを自動でarchive.orgに保存する
"""

import httpx
import re

# 魚拓不要な公的ドメイン（消えないサイト）
OFFICIAL_DOMAINS = [
    'npa.go.jp',        # 警察庁
    'courts.go.jp',     # 裁判所
    'moj.go.jp',        # 法務省
    'e-gov.go.jp',      # e-Gov
    'cao.go.jp',        # 内閣府
    'kantei.go.jp',     # 首相官邸
    'metro.tokyo.lg.jp',# 東京都
    'pref.',            # 都道府県（pref.xxx.lg.jp）
    '.lg.go.jp',        # 地方自治体
    '.go.jp',           # 省庁全般
]


def needs_archive(url: str) -> bool:
    """魚拓保存が必要かどうかを判定"""
    if not url:
        return False
    url_lower = url.lower()
    for domain in OFFICIAL_DOMAINS:
        if domain in url_lower:
            return False
    return True


async def save_to_archive(url: str) -> str | None:
    """
    archive.orgにURLを保存してアーカイブURLを返す
    失敗した場合はNoneを返す
    """
    save_url = f"https://web.archive.org/save/{url}"
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; CrimeMapArchiver/1.0)",
            }
            res = await client.get(save_url, headers=headers)

            # archive.orgはリダイレクト先のURLがアーカイブURL
            # Content-Location ヘッダーにアーカイブパスが入る
            content_location = res.headers.get("Content-Location", "")
            if content_location:
                return f"https://web.archive.org{content_location}"

            # リダイレクト先URLから取得
            if res.url and "web.archive.org/web/" in str(res.url):
                return str(res.url)

            # レスポンスボディからアーカイブURLを抽出
            match = re.search(r'web\.archive\.org/web/\d+/' + re.escape(url), res.text)
            if match:
                return f"https://{match.group()}"

    except Exception as e:
        print(f"[Archive] 保存失敗: {url} - {e}")

    return None
