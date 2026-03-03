import os
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api", tags=["contact"])

CONTACT_TYPES = [
    "記事の削除依頼(記事IDを添えてください)",
    "ソースのリンク切れ報告",
    "機能要望",
    "その他",
]


class ContactForm(BaseModel):
    contact_type: str
    detail: str


@router.post("/contact")
async def submit_contact(body: ContactForm):
    if body.contact_type not in CONTACT_TYPES:
        raise HTTPException(400, "Invalid contact type")
    if not body.detail.strip():
        raise HTTPException(400, "詳細を入力してください")

    resend_api_key = os.getenv("RESEND_API_KEY")
    to_email       = os.getenv("CONTACT_TO_EMAIL", "s.tera78@gmail.com")
    from_email     = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")

    if resend_api_key:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {resend_api_key}"},
                    json={
                        "from":    from_email,
                        "to":      to_email,
                        "subject": f"[犯罪マップ お問い合わせ] {body.contact_type}",
                        "text":    f"種別: {body.contact_type}\n\n詳細:\n{body.detail}",
                    },
                    timeout=10,
                )
                if resp.status_code >= 400:
                    print(f"[Contact] Resend error {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"[Contact] Email send failed: {e}")

    return {"status": "ok"}
