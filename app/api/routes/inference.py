import logging

from datetime import date
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Inference

from app.api.routes.device import device_exists

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/inference", tags=["Inference"])


class InferenceBase(BaseModel):
    device_id: str
    deployment_id: int
    session_date: date
    completed: bool


class InferenceList(InferenceBase):
    id: int


class InferencePatch(InferenceBase):
    task_arn: str | None


class InferenceFull(InferenceBase):
    id: int
    task_arn: str | None


@router.get(
    "/",
    summary="List inferences.",
    response_model=list[InferenceList]
)
async def get_inferences(
    db: DbDependency,
    device_id: str | None = None,
    date: date | None = None,
    completed: bool | None = None,
    deleted: bool = False,
    offset: int = 0,
    limit: int = 100
):
    query = (
        select(Inference).
        where(Inference.deleted == deleted).
        limit(limit).
        offset(offset)
    )
    if device_id:
        query = query.where(Inference.device_id == device_id)
    if date:
        query = query.where(Inference.date == date)
    if completed:
        query = query.where(Inference.completed == completed)

    inferences = db.exec(query).all()
    return inferences


@router.get(
    "/{id}",
    summary="Inference details.",
    response_model=InferenceFull
)
async def get_inference(db: DbDependency, id: int):
    return get_inference_by_id(db, id)


@router.put(
    "/{id}",
    summary="Update inference.",
    response_model=InferenceFull
)
async def update_inference(
    db: DbDependency, id: int, body: InferencePatch
):
    check_valid_inference(db, body, id)
    current_inference = get_inference_by_id(db, id)
    try:
        revised_inference = body.model_dump(exclude_unset=True)
        current_inference.sqlmodel_update(revised_inference)
        db.add(current_inference)
        db.commit()
        db.refresh(current_inference)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update inference: {e}")
    return current_inference


def get_inference_by_id(db: Session, id: int, deleted: bool = False):
    inference = db.exec(
        select(Inference).
        where(Inference.id == id).
        where(Inference.deleted == deleted)
    ).first()
    if not inference:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No inference found with id {id}.")
    return inference


def check_valid_inference(db: Session, inference: InferenceBase):
    # Check foreign key validity.
    if not device_exists(db, inference.device_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {inference.device_id} not found."
        )
