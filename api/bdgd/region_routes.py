from fastapi import APIRouter, HTTPException, Query
from app.bdgd_manager import BDGDManager

router = APIRouter()

bdgd_manager = BDGDManager()

@router.get("/region", tags=["BDGD"])
def get_region(
    id: str | None = Query(None, description="Filtrar por ID da região."),
    name: str | None = Query(None, description="Filtrar por nome da região (bdgd_name)."),
    poi: str | None = Query(None, description="Ponto de Interesse no formato 'longitude,latitude' (ex: '-49.72,-25.55')."),
    limit: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, o limite de registros para retornar."),
    offset: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, por qual registro deve começar para retornar.")
):
    unique_param: list[tuple[str, str]] = [param for param in [("id", id), ("name", name), ("poi", poi)] if param[1] is not None] # type: ignore
    if len(unique_param) > 1:
        raise HTTPException(400, f"{', '.join([f'{k}: {v}' for k, v in unique_param])} não podem ser usados juntos pois são parâmetros únicos.")
    if len(unique_param) < 1:
        return bdgd_manager.interface.get_all_region(limit=limit, offset=offset)
    
    param = unique_param[0]

    match param[0]:
        case "id":
            content = bdgd_manager.interface.get_region_by_id(param[1])
        case "name":
            content = bdgd_manager.interface.get_region_by_bdgd_name(param[1])
        case "poi":
            try:
                lat, lon = param[1].split(',')
                lat = float(lat.strip())
                lon = float(lon.strip())
            except Exception as e:
                raise ValueError(f"{param[1]} não é um par de coordenadas válido. Tente 'latitude,longitude' (ex: '-25.55,-49.72').\n{e}")
            poi_coord = (lon, lat)
            content = bdgd_manager.interface.get_region_by_poi(poi_coord)
        case _:
            raise HTTPException(400, f"Parâmetro {param[0]} inválido.")
    if content is not None:
        return content
    raise HTTPException(404, f"Área de atuação não encontrada")
