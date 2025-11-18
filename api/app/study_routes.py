from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
import asyncio
from typing import Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from services.utils import to_camel
from services.study.study_manager import RegionOfInterest, SubstationOfInterest, main as create_new_study, study_folder_exists
from . import *

router = APIRouter()

# Fila de estudos em processamento
study_queue: Dict[str, dict] = {}

# Executor para rodar tarefas bloqueantes
executor = ThreadPoolExecutor(max_workers=1)  # Apenas 1 worker para processar um estudo por vez

# Lock para garantir processamento sequencial
processing_lock = asyncio.Lock()

async def process_study(study_name: str, poi_coord: tuple):
    """Processa a criação do estudo em background"""
    async with processing_lock:  # Garante que apenas um estudo rode por vez
        try:
            study_queue[study_name]["status"] = "processing"
            study_queue[study_name]["started_at"] = datetime.now().isoformat()
            
            # Obtém dados de forma assíncrona se possível, ou roda no executor
            loop = asyncio.get_event_loop()
            
            region_of_interest: RegionOfInterest = await loop.run_in_executor(
                executor,
                bdgd_manager.interface.get_region_by_poi,
                poi_coord
            )
            
            substation_of_interest: SubstationOfInterest = await loop.run_in_executor(
                executor,
                bdgd_manager.interface.get_substation_by_poi,
                poi_coord
            )
            
            # Executa a criação do estudo em thread separada para não bloquear
            await loop.run_in_executor(
                executor,
                create_new_study,
                study_name,
                poi_coord,
                region_of_interest,
                substation_of_interest['cod_id']
            )
            
            study_queue[study_name]["status"] = "completed"
            study_queue[study_name]["completed_at"] = datetime.now().isoformat()
            
        except Exception as e:
            study_queue[study_name]["status"] = "failed"
            study_queue[study_name]["error"] = str(e)
            study_queue[study_name]["failed_at"] = datetime.now().isoformat()

@router.get("/new", tags=["BDGD"])
async def new_study(
    background_tasks: BackgroundTasks,
    study_name: str = Query(..., description="Nome do estudo a ser criado."),
    poi: str = Query(..., description="Ponto de interesse no formato 'latitude,longitude' (ex: '-25.55,-49.72').")
):
    study_name = to_camel(study_name)

    if study_folder_exists(STUDIES_FOLDER_PATH, study_name):
        raise HTTPException(
            400,
            f"Já existe um estudo com o nome '{study_name}'. Escolha outro nome."
        )

    if study_name in study_queue and study_queue[study_name]["status"] in ["queued", "processing"]:
        raise HTTPException(
            400,
            f"O estudo '{study_name}' já está na fila de processamento."
        )

    try:
        lat_str, lon_str = str(poi).split(',')
        lat = float(lat_str.strip())
        lon = float(lon_str.strip())
    except Exception as e:
        raise HTTPException(
            400,
            f"{poi} não é um par de coordenadas válido. Use 'latitude,longitude' (ex: '-25.55,-49.72'). Detalhe: {e}"
        )
    
    poi_coord = (lon, lat)
    
    # Adiciona à fila
    study_queue[study_name] = {
        "status": "queued",
        "poi": poi_coord,
        "queued_at": datetime.now().isoformat()
    }
    
    # Agenda processamento em background
    background_tasks.add_task(process_study, study_name, poi_coord)
    
    return {
        "message": f"Estudo '{study_name}' adicionado à fila de processamento.",
        "study_name": study_name,
        "status": "queued"
    }

@router.get("/status/{study_name}", tags=["BDGD"])
async def get_study_status(study_name: str):
    """Verifica o status de um estudo na fila"""
    study_name = to_camel(study_name)
    
    if study_name not in study_queue:
        raise HTTPException(404, f"Estudo '{study_name}' não encontrado na fila.")
    
    return {
        "study_name": study_name,
        **study_queue[study_name]
    }

@router.get("/queue", tags=["BDGD"])
async def get_queue_status():
    """Lista todos os estudos na fila"""
    return {
        "total": len(study_queue),
        "studies": study_queue
    }