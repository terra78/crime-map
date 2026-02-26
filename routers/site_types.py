from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db
from models import SiteType

router = APIRouter(prefix="/api/site_types", tags=["site_types"])


@router.get("")
def list_site_types(db: Session = Depends(get_db)):
    return db.query(SiteType).all()


@router.get("/{slug}")
def get_site_type(slug: str, db: Session = Depends(get_db)):
    st = db.query(SiteType).filter(SiteType.slug == slug).first()
    if not st:
        return {"error": "not found"}, 404
    return {
        "id":          st.id,
        "slug":        st.slug,
        "name":        st.name,
        "description": st.description,
        "fields":      st.fields,
    }
