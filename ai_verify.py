import os
import httpx
import anthropic
from dotenv import load_dotenv

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


async def fetch_url_content(url: str) -> str:
    """URLのページ内容を取得"""
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            r = await http.get(url, follow_redirects=True)
            return r.text[:3000]
    except Exception as e:
        return f"取得失敗: {e}"


async def verify_report(report) -> tuple[float, str]:
    """
    投稿内容とソースURLを照合してAIがスコアリング
    戻り値: (score: 0.0〜1.0, reason: str)
    """
    page_content = await fetch_url_content(report.source_url)

    prompt = f"""以下の投稿内容とソースページを比較して、投稿の信頼性を評価してください。

投稿内容:
- タイトル: {report.title}
- 説明: {report.description}
- 発生日: {report.occurred_at}
- データ: {report.data}

ソースページ内容（抜粋）:
{page_content}

以下の基準で評価してください:
1. 投稿内容がソースに基づいているか
2. 誇張・歪曲・切り取りがないか
3. 個人を特定できる情報が含まれていないか
4. 差別的・扇動的な表現がないか

必ずJSONのみで返してください（説明文不要）:
{{"score": 0.0から1.0の数値, "reason": "判定理由を1〜2文で"}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}]
        )
        import json, re
        text = message.content[0].text
        # JSON部分だけ抽出
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            result = json.loads(match.group())
            return float(result["score"]), result["reason"]
    except Exception as e:
        pass

    return 0.5, "AI検証に失敗しました。手動確認が必要です。"
