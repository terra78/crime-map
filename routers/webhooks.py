"""
routers/webhooks.py - Clerk Webhook ハンドラ
=============================================
Clerk からの email.created イベントを受け取り、Resend API でメールを送信する。

必要な環境変数:
  CLERK_WEBHOOK_SECRET  - Clerk ダッシュボード → Webhooks → Signing Secret
  RESEND_API_KEY        - Resend ダッシュボード → API Keys
  RESEND_FROM_EMAIL     - 送信元メールアドレス（例: noreply@yourdomain.com）
                          ※ Resend でドメイン認証済みのアドレス
                          ※ テスト中は "onboarding@resend.dev" も使用可

設定手順:
  1. Resend ダッシュボード → Domains → ドメインを追加して DNS 認証
  2. Resend ダッシュボード → API Keys → キー作成（"Sending access"）
  3. Clerk ダッシュボード → Webhooks → エンドポイント追加:
       URL: https://crime-map-api-gqe3.onrender.com/webhooks/clerk
       Events: email.created にチェック
  4. Signing Secret をコピーして CLERK_WEBHOOK_SECRET に設定
  5. Clerk ダッシュボード → Customization → Emails → 各テンプレートで
     「Delivered by Clerk」をオフ（対象: Verification code, Magic link,
      Password reset, Invitation 等）
"""

import os
import logging
import httpx
from fastapi import APIRouter, Request, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

CLERK_WEBHOOK_SECRET: str | None = os.getenv("CLERK_WEBHOOK_SECRET")
RESEND_API_KEY:       str | None = os.getenv("RESEND_API_KEY")
RESEND_FROM_EMAIL:    str        = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")

RESEND_API_URL = "https://api.resend.com/emails"


def _verify_svix_signature(payload: bytes, headers: dict) -> dict:
    """
    Svix（Clerk の Webhook 署名ライブラリ）でリクエストを検証する。
    検証成功時はペイロードの dict を返す。失敗時は HTTPException(400) を送出。
    """
    if not CLERK_WEBHOOK_SECRET:
        raise HTTPException(
            status_code=500,
            detail="CLERK_WEBHOOK_SECRET が設定されていません",
        )
    try:
        from svix.webhooks import Webhook, WebhookVerificationError
        wh = Webhook(CLERK_WEBHOOK_SECRET)
        return wh.verify(payload, headers)
    except ImportError:
        # svix 未インストール時: 署名検証をスキップして警告（開発環境のみ許容）
        import json
        logger.warning("svix パッケージが未インストールのため署名検証をスキップします（本番環境では必ず pip install svix）")
        return json.loads(payload)
    except Exception as e:
        logger.warning("Webhook 署名検証失敗: %s", e)
        raise HTTPException(status_code=400, detail="Invalid webhook signature")


async def _send_via_resend(to: str, subject: str, html: str, plain: str | None = None) -> bool:
    """
    Resend API でメールを送信する。成功したら True を返す。
    """
    if not RESEND_API_KEY:
        logger.error("RESEND_API_KEY が設定されていません")
        return False

    payload: dict = {
        "from": RESEND_FROM_EMAIL,
        "to": [to],
        "subject": subject,
        "html": html,
    }
    if plain:
        payload["text"] = plain

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            RESEND_API_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
        )

    if resp.status_code in (200, 201):
        logger.info("Resend 送信成功: to=%s subject=%s", to, subject)
        return True
    else:
        logger.error("Resend 送信失敗: status=%d body=%s", resp.status_code, resp.text)
        return False


@router.post("/clerk")
async def clerk_webhook(request: Request):
    """
    Clerk からの Webhook を受け取る。
    email.created イベント → Resend でメール送信。
    その他のイベントは 200 OK を返してスキップ。
    """
    # リクエストボディと Svix 署名ヘッダーを取得
    payload = await request.body()
    svix_headers = {
        "svix-id":        request.headers.get("svix-id", ""),
        "svix-timestamp": request.headers.get("svix-timestamp", ""),
        "svix-signature": request.headers.get("svix-signature", ""),
    }

    # 署名検証
    event = _verify_svix_signature(payload, svix_headers)

    event_type = event.get("type", "")
    logger.debug("Clerk Webhook 受信: type=%s", event_type)

    if event_type != "email.created":
        # email.created 以外は無視（ただし 200 を返す）
        return {"received": True, "processed": False, "type": event_type}

    # email.created ペイロードを解析
    data = event.get("data", {})
    to_email  = data.get("to_email_address")
    subject   = data.get("subject", "")
    html_body = data.get("body", "")
    plain_body = data.get("body_plain")

    if not to_email:
        logger.error("email.created に to_email_address がありません: %s", data)
        raise HTTPException(status_code=400, detail="Missing to_email_address")

    # Resend で送信
    success = await _send_via_resend(to_email, subject, html_body, plain_body)

    return {
        "received": True,
        "processed": True,
        "success": success,
        "to": to_email,
    }
