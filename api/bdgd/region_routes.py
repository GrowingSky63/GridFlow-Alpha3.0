from fastapi import APIRouter, HTTPException, Query
from . import *

router = APIRouter()

@router.get("/region", tags=["BDGD"])
def get_region(
    id: int | None = Query(None, description="Buscar por ID interno da região (GridFlow)."),
    bdgd_id: str | None = Query(None, description="Buscar por ID da região (id de download na ANEEL)."),
    cod_id: str | None = Query(None, description="Buscar por ID da região (id no BDGD)."),
    name: str | None = Query(None, description="Buscar por nome da região (bdgd_name)."),
    dist: str | None = Query(None, description="Buscar pelo código da distribuidora."),
    poi: str | None = Query(None, description="Buscar a região que atua no Ponto de Interesse no formato 'latitude,longitude' (ex: '-25.55,-49.72')."),
    geometry: bool = Query(True, description="Opção para trazer ou não a geometria da região."),
    simplify_tolerance: float = Query(0, description="Opção para aumentar a tolerancia da simplificação da regioão (mais tolerância, mais simples)"),
    limit: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, o limite de registros para retornar."),
    offset: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, por qual registro deve começar para retornar.")
):
    
    # Parâmetros exclusivos (apenas um pode ser usado por requisição)
    unique_candidates = [
        ("id", id),
        ("bdgd_id", bdgd_id),
        ("cod_id", cod_id),
        ("name", name),
        ("dist", dist),
        ("poi", poi)
    ]

    unique_params = [(k, v) for k, v in unique_candidates if v is not None]

    if len(unique_params) > 1:
        raise HTTPException(
            400,
            f"{', '.join([f'{k}: {v}' for k, v in unique_params])} não podem ser usados juntos; use apenas um filtro exclusivo."
        )

    if len(unique_params) == 0:
        # Listagem paginada
        return bdgd_manager.interface.get_all_regions(
            limit,
            offset,
            True,
            geometry,
            simplify_tolerance
        )

    param_name, param_value = unique_params[0]

    match param_name:
        case "id":
            content = bdgd_manager.interface.get_region_by_id(
                param_value,
                True,
                geometry,
                simplify_tolerance
            )
        case "bdgd_id":
            content = bdgd_manager.interface.get_region_by_bdgd_id(
                param_value,
                True,
                geometry,
                simplify_tolerance
            )
        case "cod_id":
            content = bdgd_manager.interface.get_region_by_cod_id(
                param_value,
                True,
                geometry,
                simplify_tolerance
            )
        case "name":
            content = bdgd_manager.interface.get_region_by_bdgd_name(
                param_value,
                True,
                geometry,
                simplify_tolerance
            )
        case "dist":
            content = bdgd_manager.interface.get_region_by_dist(
                param_value,
                True,
                geometry,
                simplify_tolerance
            )
        case "poi":
            try:
                lat_str, lon_str = str(param_value).split(',')
                lon = float(lon_str.strip())
                lat = float(lat_str.strip())
            except Exception as e:
                raise HTTPException(
                    400,
                    f"{param_value} não é um par de coordenadas válido. Use 'latitude,longitude' (ex: '-25.55,-49.72'). Detalhe: {e}"
                )
            poi_coord = (lon, lat)  # POINT(lon lat)
            content = bdgd_manager.interface.get_region_by_poi(
                poi_coord,
                True,
                geometry,
                simplify_tolerance
            )
        case _:
            raise HTTPException(400, f"Parâmetro {param_name} inválido.")

    if content is not None:
        return content

    raise HTTPException(404, "Área de atuação não encontrada")
