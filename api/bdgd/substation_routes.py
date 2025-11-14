from fastapi import APIRouter, HTTPException, Query
from services.bdgd_manager import BDGDManager

router = APIRouter()

bdgd_manager = BDGDManager()

@router.get("/substation", tags=["BDGD"])
def get_region(
    id: int | None = Query(None, description="Buscar por ID interno da subestação (GridFlow)."),
    cod_id: str | None = Query(None, description="Buscar por ID da subestação (id no BDGD)."),
    name: str | None = Query(None, description="Buscar por nome da subestação (bdgd_name)."),
    dist: str | None = Query(None, description="Buscar pelo código da distribuidora."),
    poi: str | None = Query(None, description="Buscar a subestação mais próxima do Ponto de Interesse no formato 'latitude,longitude' (ex: '-25.55,-49.72')."),
    geometry: bool = Query(True, description="Opção para trazer ou não a geometria da subestação."),
    include_trhvs: bool = Query(False, description="Se true, inclui lista de transformadores (UNTRAT) em 'untrats'."),
    limit: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, o limite de registros para retornar."),
    offset: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, por qual registro deve começar para retornar.")
):
    
    # Parâmetros exclusivos (apenas um pode ser usado por requisição)
    unique_candidates = [
        ("id", id),
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
        substations = bdgd_manager.interface.get_all_substations(limit=limit, offset=offset, geometry=geometry)
        if include_trhvs:
            # Converte RowMapping -> dict para permitir mutação
            substations = [dict(r) for r in substations]
            pairs = [(s['cod_id'], s['dist']) for s in substations]
            grouped = bdgd_manager.interface.get_trhvs_grouped_by_substations(pairs, geometry=geometry)
            for s in substations:
                s['untrats'] = grouped.get((s['cod_id'], s['dist']), [])
        return substations

    param_name, param_value = unique_params[0]

    match param_name:
        case "id":
            content = bdgd_manager.interface.get_substation_by_id(int(param_value), geometry=geometry)
        case "cod_id":
            content = bdgd_manager.interface.get_substation_by_cod_id(str(param_value), geometry=geometry)
        case "name":
            content = bdgd_manager.interface.get_substation_by_name(str(param_value), geometry=geometry)
        case "dist":
            content = bdgd_manager.interface.get_substations_by_dist(str(param_value), geometry=geometry)
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
            content = bdgd_manager.interface.get_substation_by_poi(poi_coord, geometry=geometry)
        case _:
            raise HTTPException(400, f"Parâmetro {param_name} inválido.")

    # 'content' pode ser único ou lista
    if content is not None and include_trhvs:
        if isinstance(content, list):
            content = [dict(r) for r in content]
            pairs = [(s['cod_id'], s['dist']) for s in content]
            grouped = bdgd_manager.interface.get_trhvs_grouped_by_substations(pairs, geometry=geometry)
            for s in content:
                s['untrats'] = grouped.get((s['cod_id'], s['dist']), [])
        else:
            # único registro (RowMapping) -> dict
            if not isinstance(content, dict):
                content = dict(content)
            pairs = [(content['cod_id'], content['dist'])]
            grouped = bdgd_manager.interface.get_trhvs_grouped_by_substations(pairs, geometry=geometry)
            content['untrats'] = grouped.get((content['cod_id'], content['dist']), [])
    return content

    raise HTTPException(404, "Área de atuação não encontrada")
