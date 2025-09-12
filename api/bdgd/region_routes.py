from fastapi import APIRouter, HTTPException, Query
from app.bdgd_manager import BDGDManager

router = APIRouter()

bdgd_manager = BDGDManager()

@router.get("/region", tags=["BDGD"])
def get_region(
    id: int | None = Query(None, description="Buscar por ID interno da região (GridFlow)."),
    bdgd_id: str | None = Query(None, description="Buscar por ID da região (id de download na ANEEL)."),
    cod_id: str | None = Query(None, description="Buscar por ID da região (id no BDGD)."),
    name: str | None = Query(None, description="Buscar por nome da região (bdgd_name)."),
    dist: str | None = Query(None, description="Buscar pelo código da distribuidora."),
    poi: str | None = Query(None, description="Buscar a região que atua no Ponto de Interesse no formato 'latitude,longitude' (ex: '-25.55,-49.72')."),
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
        return bdgd_manager.interface.get_all_regions(limit=limit, offset=offset)

    param_name, param_value = unique_params[0]

    match param_name:
        case "id":
            content = bdgd_manager.interface.get_region_by_id(int(param_value))
        case "bdgd_id":
            content = bdgd_manager.interface.get_region_by_bdgd_id(str(param_value))
        case "cod_id":
            content = bdgd_manager.interface.get_region_by_cod_id(str(param_value))
        case "name":
            content = bdgd_manager.interface.get_region_by_bdgd_name(str(param_value))
        case "dist":
            content = bdgd_manager.interface.get_region_by_dist(str(param_value))
        case "poi":
            try:
                lat_str, lon_str = str(param_value).split(',')
                lon = float(lon_str.strip())
                lat = float(lat_str.strip())
            except Exception as e:
                raise HTTPException(
                    400,
                    f"{param_value} não é um par de coordenadas válido. Use 'longitude,latitude' (ex: '-49.72,-25.55'). Detalhe: {e}"
                )
            poi_coord = (lon, lat)  # POINT(lon lat)
            content = bdgd_manager.interface.get_region_by_poi(poi_coord)
        case _:
            raise HTTPException(400, f"Parâmetro {param_name} inválido.")

    if content is not None:
        return content

    raise HTTPException(404, "Área de atuação não encontrada")
