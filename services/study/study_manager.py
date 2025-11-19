from tempfile import TemporaryDirectory
from typing import TypedDict
import geopandas as gpd
from pyogrio import list_layers
import os, dotenv
from services.bdgd.bdgd_downloader import BDGDDownloader

class RegionOfInterest(TypedDict):
  id: int
  bdgd_id: str
  cod_id: str
  bdgd_full_name: str
  bdgd_name: str
  bdgd_date: str
  dist: str

class SubstationOfInterest(TypedDict):
  id: int
  cod_id: str
  dist: str
  name: str
  distance: float

dotenv.load_dotenv()
CRS = 'EPSG:4674'

def open_gdb_layer(gdb_path: str, layer_name: str) -> gpd.GeoDataFrame:
  gdf = gpd.read_file(gdb_path, layer=layer_name)
  if isinstance(gdf, gpd.GeoDataFrame):
    return gdf.to_crs(CRS)
  gdf = gpd.GeoDataFrame(data=gdf)
  return gdf

def get_all_gdb_in_folder(folder_path: str) -> list[str]:
  return [path for path in os.listdir(folder_path) if path.endswith('.gdb')]

def find_gdb_by_name(folder_path: str, bdgd_name: str) -> str:
  gdbs = [gdb for gdb in get_all_gdb_in_folder(folder_path) if bdgd_name.lower() in gdb.lower()]
  if not gdbs or len(gdbs) < 1:
    raise FileNotFoundError('No BDGD found for the specified POI.')
  gdbs.sort()
  return gdbs[-1]

def layers_exists(gdb_path: str, layers: list[str]) -> bool:
  actual_layers_in_gdb = {name for name, *_ in list_layers(gdb_path)}
  bdgd_layers_set = set(layers)
  missing_layers = bdgd_layers_set - actual_layers_in_gdb
  if len(missing_layers) > 0:
    return False
  return True

def filter_bdgd_layer_by_substation_cod_id(bdgd_layer: gpd.GeoDataFrame, substation_cod_id: str) -> gpd.GeoDataFrame:
  if 'SUB' not in bdgd_layer.columns:
    data = bdgd_layer.loc[bdgd_layer['COD_ID'] == substation_cod_id]
  else:
    data = bdgd_layer.loc[bdgd_layer['SUB'] == substation_cod_id]
  return gpd.GeoDataFrame(data=data)

def layer_mapper(gdb_path: str, layers: list[str], substation_cod_id_filter: str | None = None, layers_to_filter: list[str] | None = None) -> dict[str, gpd.GeoDataFrame]:
  """
  This method maps the layers from a GDB file, to a dictionary, where keys are the layer names and values are the GeoDataFrames.
  """
  if substation_cod_id_filter is None and layers_to_filter is not None:
    raise ValueError('layers_to_filter can only be provided if substation_cod_id_filter is also provided.')
  if substation_cod_id_filter is not None and layers_to_filter is None:
    layers_to_filter = layers
  layer_dict: dict[str, gpd.GeoDataFrame] = {}
  for layer_name in layers:
    gdf = open_gdb_layer(gdb_path, layer_name)
    print(f'Layer {layer_name} is a {type(gdf)} pre-filtering.')
    if substation_cod_id_filter is not None and layer_name in layers_to_filter: # type: ignore
      gdf = filter_bdgd_layer_by_substation_cod_id(gdf, substation_cod_id_filter)
      print(f'Layer {layer_name} after filter, is a {type(gdf)}.')
    layer_dict[layer_name] = gdf
    print(f'Layer {layer_name} after mapping is a {type(layer_dict[layer_name])}.')
  return layer_dict

def create_poi_gdf(poi: tuple[float, float], desc: str) -> gpd.GeoDataFrame:
  poi_gdf = gpd.GeoDataFrame(
    {'description': [desc]},
    geometry=gpd.points_from_xy([poi[1]], [poi[0]]),
    crs=CRS
  )
  return poi_gdf

def study_folder_exists(study_folder_path: str, study_name: str) -> bool:
  study_folder_path = os.path.join(study_folder_path, study_name)
  return os.path.isdir(study_folder_path)

def poi_gpkg_exists(study_folder_path: str, study_name: str) -> bool:
  poi_gpkg_path = os.path.join(study_folder_path, f'POI_{study_name}.gpkg')
  return os.path.isfile(poi_gpkg_path)

def filtered_gpkg_exists(study_folder_path: str, substation_cod_id: str, bdgd_name: str) -> bool:
  filtered_gpkg_path = os.path.join(study_folder_path, f'{substation_cod_id}_{bdgd_name}.gpkg')
  return os.path.isfile(filtered_gpkg_path)

def create_study_folder(studies_folder_path: str, study_name: str) -> str:
  study_folder_path = os.path.join(studies_folder_path, study_name)
  print(f"Tentando criar: {study_folder_path}")
  print(f"Diretório pai existe? {os.path.isdir(studies_folder_path)}")
  os.makedirs(study_folder_path, exist_ok=True)
  print(f"Diretório criado? {os.path.isdir(study_folder_path)}")
  return study_folder_path

def create_poi_gpkg(study_folder_path: str, study_name: str, poi_gdf: gpd.GeoDataFrame) -> str:
  poi_gpkg_path = os.path.join(study_folder_path, f'POI_{study_name}.gpkg')
  poi_gdf.to_file(poi_gpkg_path, driver='GPKG', layer='Pontos de Interesse')
  return poi_gpkg_path

def create_filtered_gpkg_by_substation_cod_id(study_folder_path: str, substation_cod_id: str, bdgd_name: str, filtered_gdfs_by_substation_cod_id: dict[str, gpd.GeoDataFrame]) -> str:
  filtered_gpkg_path = os.path.join(study_folder_path, f'{substation_cod_id}_{bdgd_name}.gpkg')
  for layer_name, gdf in filtered_gdfs_by_substation_cod_id.items():
    gdf.to_file(
      filtered_gpkg_path,
      driver='GPKG',
      layer=layer_name.strip('_tab').lower(),
      mode='a'
    )
  return filtered_gpkg_path


STUDIES_FOLDER_PATH = os.path.normpath(os.environ.get('STUDIES_FOLDER_PATH', ''))
GRIDFLOW_BDGD_API_URL = os.environ.get('GRIDFLOW_BDGD_API_URL', 'http://172.25.0.232:8000/api/bdgd')

BDGD_LAYERS = [
  'BAR',
  'CRVCRG',
  'CTMT',
  'SEGCON',
  'SSDBT',
  'SSDMT',
  'SUB',
  'UCBT_tab',
  'UCMT_tab',
  'UGBT_tab',
  'UGMT_tab',
  'UNREAT',
  'UNREMT',
  'UNSEMT',
  'UNTRAT',
  'UNTRMT'
]
BDGD_LAYERS.sort()
BDGD_LAYERS_TO_FILTER = list(set(BDGD_LAYERS) - {'CRVCRG', 'SEGCON'})
BDGD_LAYERS_TO_FILTER.sort()

def main(
  study_name: str,
  poi: tuple[float, float],
  bdgd: RegionOfInterest,
  substation_cod_id: str
  ) -> None:
  with TemporaryDirectory('gridflow-bdgd-') as tempdir:
    with BDGDDownloader(bdgd['bdgd_id'], bdgd['bdgd_name'], tempdir, True) as gdb_path:
      if not layers_exists(gdb_path, BDGD_LAYERS):
        raise ValueError('One or more required layers are missing in the BDGD GDB.')

      filtered_gdfs_by_substation_cod_id = layer_mapper(
        gdb_path,
        BDGD_LAYERS,
        substation_cod_id,
        BDGD_LAYERS_TO_FILTER
      )

  study_folder_path = create_study_folder(STUDIES_FOLDER_PATH, study_name)
  create_poi_gpkg(
    study_folder_path,
    study_name,
    create_poi_gdf(poi, 'Ponto de Conexão')
  ) if not poi_gpkg_exists(
    study_folder_path,
    study_name
  ) else None
  create_filtered_gpkg_by_substation_cod_id(
    study_folder_path,
    substation_cod_id,
    bdgd['bdgd_name'],
    filtered_gdfs_by_substation_cod_id
  ) if not filtered_gpkg_exists(
    study_folder_path,
    substation_cod_id,
    bdgd['bdgd_name']
  ) else None