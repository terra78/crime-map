from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import get_db
from models import Comment, Report
from auth import get_current_user_required

router = APIRouter(prefix="/api/reports", tags=["comments"])


class CommentCreate(BaseModel):
    content: str
    user_name: Optional[str] = None
    user_avatar: Optional[str] = None
    parent_id: Optional[int] = None


def _fmt(c: Comment) -> dict:
    return {
        "id":          c.id,
        "report_id":   c.report_id,
        "user_id":     c.user_id,
        "user_name":   c.user_name,
        "user_avatar": c.user_avatar,
        "content":     c.content,
        "parent_id":   c.parent_id,
        "created_at":  str(c.created_at),
    }


@router.get("/{report_id}/comments")
def list_comments(report_id: int, db: Session = Depends(get_db)):
    """記事に紐づくコメント一覧（返信も含む、作成日昇順）"""
    comments = (
        db.query(Comment)
        .filter(Comment.report_id == report_id)
        .order_by(Comment.created_at.asc())
        .all()
    )
    return [_fmt(c) for c in comments]


@router.post("/{report_id}/comments")
def create_comment(
    report_id: int,
    body: CommentCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_required),
):
    """コメント投稿（ログイン必須）"""
    if not db.query(Report).filter(Report.id == report_id).first():
        raise HTTPException(404, "report not found")

    # 親コメントが指定された場合は同じreport_idか確認
    if body.parent_id:
        parent = db.query(Comment).filter(Comment.id == body.parent_id).first()
        if not parent or parent.report_id != report_id:
            raise HTTPException(400, "invalid parent_id")

    comment = Comment(
        report_id=report_id,
        user_id=user_id,
        user_name=body.user_name,
        user_avatar=body.user_avatar,
        content=body.content,
        parent_id=body.parent_id,
    )
    db.add(comment)
    db.commit()
    db.refresh(comment)
    return _fmt(comment)
