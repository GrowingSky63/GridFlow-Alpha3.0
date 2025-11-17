from fastapi import APIRouter
from . import *

router = APIRouter()

@router.get("/admin/check_update", tags=["BDGD"])
def check_update():
    ... # Calls bdgd_manager.check_update()

@router.get("/admin/update", tags=["BDGD"])
def update():
    ... # Calls bdgd_manager.update()

@router.get("/admin/status", tags=["BDGD"])
def status():
    ... # 'Updating...', 'Up to date.', 'Deprecated.', etc.