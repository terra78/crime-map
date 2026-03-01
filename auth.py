"""
auth.py - Clerk JWT 検証モジュール
=====================================
Clerk が発行する JWT を PyJWT + PyJWKClient で検証し、
FastAPI の依存性（Depends）として利用できる関数を提供する。

必要な環境変数:
  CLERK_JWKS_URL  - Clerk の JWKS エンドポイント
                    例: https://xxxxx.clerk.accounts.dev/.well-known/jwks.json
  CLERK_ISSUER    - Clerk インスタンスの発行者 URL
                    例: https://xxxxx.clerk.accounts.dev

CLERK_JWKS_URL / CLERK_ISSUER の確認方法:
  Clerk ダッシュボード → API Keys → Frontend API URL
  Frontend API URL が "https://xxxx.clerk.accounts.dev" なら
    CLERK_JWKS_URL = https://xxxx.clerk.accounts.dev/.well-known/jwks.json
    CLERK_ISSUER   = https://xxxx.clerk.accounts.dev
"""

import os
import logging
from functools import lru_cache

import jwt
from jwt import PyJWKClient
from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)

CLERK_JWKS_URL: str | None = os.getenv("CLERK_JWKS_URL")
CLERK_ISSUER:   str | None = os.getenv("CLERK_ISSUER")


@lru_cache(maxsize=1)
def _get_jwks_client() -> PyJWKClient:
    """
    PyJWKClient をシングルトンで返す。
    CLERK_JWKS_URL が未設定の場合は RuntimeError を送出。
    """
    if not CLERK_JWKS_URL:
        raise RuntimeError(
            "環境変数 CLERK_JWKS_URL が設定されていません。"
            " Clerk ダッシュボードの Frontend API URL を確認してください。"
        )
    return PyJWKClient(CLERK_JWKS_URL, cache_keys=True)


def _decode_token(token: str) -> dict | None:
    """
    Clerk JWT を検証してペイロードを返す。
    検証失敗・例外発生時は None を返す（エラーはログに記録）。
    """
    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=CLERK_ISSUER,        # iss クレームを検証
            options={
                "verify_aud": False,    # Clerk デフォルト JWT には aud がない
                "verify_exp": True,     # 有効期限は必ず検証
            },
        )
        return payload
    except jwt.ExpiredSignatureError:
        logger.debug("Clerk JWT: トークンの有効期限切れ")
    except jwt.InvalidIssuerError:
        logger.debug("Clerk JWT: issuer 不一致")
    except jwt.PyJWTError as e:
        logger.debug("Clerk JWT: 検証エラー: %s", e)
    except RuntimeError as e:
        logger.error("Clerk JWT: 設定エラー: %s", e)
    except Exception as e:
        logger.warning("Clerk JWT: 予期しないエラー: %s", e)
    return None


# ── FastAPI 依存性 ────────────────────────────────────────────────────────────

async def get_current_user_optional(request: Request) -> str | None:
    """
    任意認証の依存性。

    Authorization: Bearer <token> ヘッダーが存在し有効なら
    Clerk user_id（"user_xxxx" 形式の sub クレーム）を返す。
    ヘッダーなし or トークン無効の場合は None を返す（エラーにしない）。

    使用例:
        @router.post("/api/reports")
        async def create_report(
            user_id: str | None = Depends(get_current_user_optional),
        ):
            ...
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[len("Bearer "):]
    payload = _decode_token(token)
    if payload is None:
        return None
    return payload.get("sub")  # "user_xxxxxxxxxxxxxxxxxxxxxxx"


async def get_current_user_required(request: Request) -> str:
    """
    認証必須の依存性。

    有効な Clerk JWT がない場合は 401 Unauthorized を返す。

    使用例:
        @router.get("/api/reports/me")
        async def get_my_reports(
            user_id: str = Depends(get_current_user_required),
        ):
            ...
    """
    user_id = await get_current_user_optional(request)
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="Authentication required. Please include a valid Clerk session token.",
        )
    return user_id
