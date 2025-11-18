from fastapi import APIRouter, HTTPException, Query

from services.utils import to_camel
from services.study.study_manager import RegionOfInterest, SubstationOfInterest, main as create_new_study, study_folder_exists
from . import *

router = APIRouter()

@router.get("/new", tags=["BDGD"])
def new_study(
    study_name: str = Query(..., description="Nome do estudo a ser criado."),
    poi: str = Query(..., description="Ponto de interesse no formato 'latitude,longitude' (ex: '-25.55,-49.72').")
):
  study_name = to_camel(study_name)

  if study_folder_exists(STUDIES_FOLDER_PATH, study_name):
    raise HTTPException(
      400,
      f"Já existe um estudo com o nome '{study_name}'. Escolha outro nome."
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

  study_name = to_camel(study_name)
  region_of_interest: RegionOfInterest = bdgd_manager.interface.get_region_by_poi(poi_coord)
  substation_of_interest: SubstationOfInterest = bdgd_manager.interface.get_substation_by_poi(poi_coord)

  create_new_study(
    study_name,
    poi_coord,
    region_of_interest,
    substation_of_interest['cod_id']
  )
  return {
    "message": f"Estudo '{study_name}' criado com sucesso."
  }