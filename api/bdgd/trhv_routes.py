from fastapi import APIRouter, HTTPException, Query
from app.bdgd_manager import BDGDManager

router = APIRouter()

bdgd_manager = BDGDManager()

@router.get("/trhv", tags=["BDGD"])
def get_region(
    id: int | None = Query(None, description="Buscar por ID interno do transformador (GridFlow)."),
    cod_id: str | None = Query(None, description="Buscar por ID do transformador (id no BDGD)."),
    name: str | None = Query(None, description="Buscar por nome do transformador (bdgd_name)."),
    dist: str | None = Query(None, description="Buscar pelo código da distribuidora."),
    geometry: bool = Query(True, description="Opção para trazer ou não a geometria da do transformador."),
    limit: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, o limite de registros para retornar."),
    offset: int | None = Query(None, description="Caso não seja utilizado nenhum filtro, por qual registro deve começar para retornar.")
):
    
    # Parâmetros exclusivos (apenas um pode ser usado por requisição)
    unique_candidates = [
        ("id", id),
        ("cod_id", cod_id),
        ("name", name),
        ("dist", dist)
    ]
    unique_params = [(k, v) for k, v in unique_candidates if v is not None]

    if len(unique_params) > 1:
        raise HTTPException(
            400,
            f"{', '.join([f'{k}: {v}' for k, v in unique_params])} não podem ser usados juntos; use apenas um filtro exclusivo."
        )

    if len(unique_params) == 0:
        # Listagem paginada
        return bdgd_manager.interface.get_all_trhvs(limit=limit, offset=offset, geometry=geometry)

    param_name, param_value = unique_params[0]

    match param_name:
        case "id":
            content = bdgd_manager.interface.get_trhv_by_id(int(param_value), geometry=geometry)
        case "cod_id":
            content = bdgd_manager.interface.get_trhv_by_cod_id(str(param_value), geometry=geometry)
        case "name":
            content = bdgd_manager.interface.get_trhv_by_name(str(param_value), geometry=geometry)
        case "dist":
            content = bdgd_manager.interface.get_trhvs_by_dist(str(param_value), geometry=geometry)
        case _:
            raise HTTPException(400, f"Parâmetro {param_name} inválido.")

    if content is not None:
        return content

    raise HTTPException(404, "Área de atuação não encontrada")
