import os
from typing import Literal
import heapq

import pandas as pd
from pyproj import Geod
from shapely import Geometry
from utils import to_camel
from pyogrio import list_layers
import geopandas as gpd
from dataclasses import dataclass
from shapely.geometry import Point, LineString, MultiLineString

STUDIES_FOLDER_PATH = r"\\172.25.0.250\Fontesul\Engenharia\estudosQGIS"
CRS = 'EPSG:4674'

@dataclass
class SemiPath:
  connection: tuple[str, str]
  cod_id: str
  ctmt: str
  path_type: Literal['ssdmt', 'unsemt']
  geometry: Geometry

def create_semi_path_gdf(gdf: gpd.GeoDataFrame, layer_name: str) -> gpd.GeoDataFrame:
  if gdf.crs != CRS:
    raise ValueError(f'Sistema de referência inválido: {gdf.crs}. Esperado EPSG:4674.')
  required_cols = {'COD_ID', 'CTMT', 'PAC_1', 'PAC_2', 'geometry'}
  missing = required_cols - set(gdf.columns)
  if missing:
    raise KeyError(f'Colunas ausentes para normalização: {sorted(missing)}')

  if layer_name not in ('ssdmt', 'unsemt'):
    raise ValueError(f'path_type inválido: "{layer_name}". Esperado "ssdmt" ou "unsemt".')

  copied_gdf = gdf.copy()
  # Garante tipos e remove linhas inválidas
  # gdf = gdf.dropna(subset=['PAC_1', 'PAC_2', 'geometry'])
  copied_gdf['cod_id'] = copied_gdf['COD_ID'].astype(str)
  copied_gdf['ctmt'] = copied_gdf['CTMT'].astype(str)
  copied_gdf['connection'] = list(zip(copied_gdf['PAC_1'].astype(str), copied_gdf['PAC_2'].astype(str)))
  copied_gdf['path_type'] = layer_name

  return copied_gdf[['connection', 'cod_id', 'ctmt', 'path_type', 'geometry']]

def concat_gdfs(gdfs: list[gpd.GeoDataFrame]):
 return gpd.GeoDataFrame(
   pd.concat(gdfs),
   geometry='geometry',
   crs=CRS
 )

def find_gpkgs(study_folder_path: str):
  return [file for file in os.listdir(study_folder_path) if file.endswith('.gpkg')]

def find_layers(gpkg_path: str) -> list[str]:
  return [name for name, *_ in list_layers(gpkg_path)]

def get_needed_layers(gpkg_path: str) -> list[str]:
  active_gpkg_layer_names = find_layers(gpkg_path)

  if len(active_gpkg_layer_names) < 2:
    raise RuntimeError(f'O GeoPackage "{os.path.basename(gpkg_path)}" não possui camadas suficientes (>= 2). Camadas encontradas: {active_gpkg_layer_names}')
  
  required = {
    'ssdmt',
    'unsemt',
    'untrat',
    'untrmt',
    'bar',
    'ctmt',
    'ucbt',
    'ucmt',
    'ugbt',
    'ugmt',
    'segcon',
    'crvcrg'
  }
  
  layer_names_lc = {n.lower() for n in active_gpkg_layer_names}
  missing = required - layer_names_lc
  if missing:
    raise ValueError(
      f'Camadas obrigatórias ausentes no GeoPackage "{os.path.basename(gpkg_path)}": {sorted(missing)}. '
      f'Disponíveis: {sorted(active_gpkg_layer_names)}'
    )
  return active_gpkg_layer_names

def select_active_gpkg(folder: str) -> str:
  gpkg_names = find_gpkgs(folder)
  if len(gpkg_names) < 1:
    raise FileNotFoundError(f'Nenhum arquivo .gpkg encontrado em: {folder}')
  elif len(gpkg_names) > 1:
    active_gpkg_name = gpkg_names[int(input(f'GeoPackages encontrados em {folder}:\n{"\n".join([f"{idx+1} - {gpkg}" for idx, gpkg in enumerate(gpkg_names)])}\nSelecione um para prosseguir...\n'))-1]
  else:
    active_gpkg_name = gpkg_names[0]

  return os.path.join(folder, active_gpkg_name)


def find_nearest_semi_path(semi_paths_gdf: gpd.GeoDataFrame, point: Point) -> pd.Series:

  def to_xy(coord: tuple[float, ...]) -> tuple[float, float]:
    # força 2D para evitar erro de tipagem quando há Z
    return (float(coord[0]), float(coord[1]))

  def min_end_distance(geom):
    if geom is None or geom.is_empty:
      return float('inf')

    ends: list[tuple[float, float]] = []
    if isinstance(geom, LineString):
      ends = [to_xy(geom.coords[0]), to_xy(geom.coords[-1])]
    elif isinstance(geom, MultiLineString):
      for line in geom.geoms:
        if len(line.coords) >= 2:
          ends.append(to_xy(line.coords[0]))
          ends.append(to_xy(line.coords[-1]))
    else:
      # Fallback: distância do ponto ao próprio geom (se não for linear)
      return geom.distance(point)

    return min(Point(xy).distance(point) for xy in ends)

  distances = semi_paths_gdf['geometry'].apply(min_end_distance)
  # usa posição para garantir Series no retorno
  nearest_pos = int(distances.to_numpy().argmin())
  return semi_paths_gdf.iloc[nearest_pos]

def search_semi_path_by_pac(semi_paths_gdf: gpd.GeoDataFrame, pac_code: str) -> pd.Series:
  mask = semi_paths_gdf['connection'].map(
    lambda conn: conn[0] == pac_code or conn[1] == pac_code
  )
  matches = semi_paths_gdf.loc[mask]
  if matches.empty:
    raise ValueError(f'Nenhum semi-caminho encontrado com o código PAC: {pac_code}')
  return matches.iloc[0]

def _to_xy(coord: tuple[float, ...]) -> tuple[float, float]:
  return (float(coord[0]), float(coord[1]))

def _geom_endpoints_xy(geom) -> tuple[tuple[float, float], tuple[float, float]]:
  # Tenta pegar os extremos topológicos (boundary)
  b = getattr(geom, "boundary", None)
  if b is not None:
    if getattr(b, "geom_type", "") == "MultiPoint" and len(b.geoms) >= 2:
      return _to_xy(b.geoms[0].coords[0]), _to_xy(b.geoms[-1].coords[0])
    if getattr(b, "geom_type", "") == "Point":
      p = _to_xy(b.coords[0])
      return p, p
  # Fallback por tipo
  if isinstance(geom, LineString):
    return _to_xy(geom.coords[0]), _to_xy(geom.coords[-1])
  if isinstance(geom, MultiLineString):
    first = geom.geoms[0]
    last = geom.geoms[-1]
    return _to_xy(first.coords[0]), _to_xy(last.coords[-1])
  c = _to_xy(geom.centroid.coords[0])
  return c, c

def _geodesic_length_m(geom) -> float:
  geod = Geod(ellps="WGS84")
  def line_len(line: LineString) -> float:
    xs, ys = zip(*[(float(x), float(y)) for x, y in line.coords])
    # CRS está em EPSG:4674 (lon, lat)
    return float(geod.line_length(xs, ys))
  if isinstance(geom, LineString):
    return line_len(geom)
  if isinstance(geom, MultiLineString):
    return sum(line_len(ls) for ls in geom.geoms)
  return 0.0

def _row_pac_endpoints(row) -> dict[str, tuple[float, float]]:
  a, b = row['connection']
  p0, p1 = _geom_endpoints_xy(row['geometry'])
  return {a: p0, b: p1}

def _choose_nearest_endpoint_pac(row: pd.Series, pt: Point) -> str:
  mapping = _row_pac_endpoints(row)
  # menor distância euclidiana no CRS atual (graus). Para rigor, use geodésica:
  geod = Geod(ellps="WGS84")
  def d_m(xy):
    lon, lat = xy
    _, _, dist = geod.inv(lon, lat, float(pt.x), float(pt.y))
    return dist
  return min(mapping.items(), key=lambda kv: d_m(kv[1]))[0]

def add_resistence_to_semi_paths(semi_paths_gdf: gpd.GeoDataFrame, layer_ssdmt: gpd.GeoDataFrame, layer_segcon: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  ssdmt = layer_ssdmt[['COD_ID', 'COMP', 'TIP_CND']].copy()
  segcon = layer_segcon[['COD_ID', 'R1']].copy()

  # Normaliza tipos para join por string
  ssdmt['COD_ID'] = ssdmt['COD_ID'].astype(str)
  ssdmt['TIP_CND'] = ssdmt['TIP_CND'].astype(str)
  segcon['COD_ID'] = segcon['COD_ID'].astype(str)

  comp_by_cod = ssdmt.set_index('COD_ID')['COMP'].astype(float)
  tip_by_cod  = ssdmt.set_index('COD_ID')['TIP_CND']
  r1_by_tip   = segcon.set_index('COD_ID')['R1'].astype(float)

  out = semi_paths_gdf.copy()
  is_ssdmt = out['path_type'].str.lower().eq('ssdmt')

  tip = out.loc[is_ssdmt, 'cod_id'].map(tip_by_cod)
  comp = out.loc[is_ssdmt, 'cod_id'].map(comp_by_cod)
  r1   = tip.map(r1_by_tip)

  out.loc[is_ssdmt, 'resistence'] = (comp / 1000.0) * r1
  out.loc[~is_ssdmt, 'resistence'] = 0.0
  out['resistence'] = out['resistence'].astype(float).fillna(0.0)
  return out

def _build_graph(semi_paths_gdf: gpd.GeoDataFrame, layer_ssdmt: gpd.GeoDataFrame, layer_segcon: gpd.GeoDataFrame):
  # Enriquecimento com a resistência (ohms)
  semi_paths_gdf_with_resistence = add_resistence_to_semi_paths(semi_paths_gdf, layer_ssdmt, layer_segcon)

  gdf = semi_paths_gdf_with_resistence.reset_index(drop=True)
  adj: dict[str, list[tuple[str, float, int]]] = {}
  node_xy: dict[str, tuple[float, float]] = {}
  pair_to_rows: dict[frozenset[str], list[int]] = {}

  for i, row in gdf.iterrows():
    a, b = row['connection']
    w = float(row['resistence'])  # ssdmt já vem calculado; unsemt = 0
    adj.setdefault(a, []).append((b, w, i))  # type: ignore
    adj.setdefault(b, []).append((a, w, i))  # type: ignore
    pmap = _row_pac_endpoints(row)
    node_xy.setdefault(a, pmap[a])
    node_xy.setdefault(b, pmap[b])
    pair_to_rows.setdefault(frozenset((a, b)), []).append(i)  # type: ignore

  return gdf, adj, node_xy, pair_to_rows

def _dijkstra(adj: dict[str, list[tuple[str, float, int]]], start: str, goal: str):
  dist: dict[str, float] = {start: 0.0}
  prev: dict[str, str] = {}
  pq: list[tuple[float, str]] = [(0.0, start)]
  visited: set[str] = set()
  while pq:
    d, u = heapq.heappop(pq)
    if u in visited:
      continue
    visited.add(u)
    if u == goal:
      break
    
    # BREAKPOINT 1: Quando processar o nó da bifurcação
    if u == '210614077':
      print(f"\n🔍 BIFURCAÇÃO DETECTADA em {u}")
      print(f"   Distância acumulada até aqui: {d}")
      print(f"   Vizinhos disponíveis:")
      for v, w, idx in adj.get(u, []):
        print(f"      → {v}: peso={w:.6f}, idx={idx}")
    
    for v, w, _ in adj.get(u, []):
      nd = d + w
      
      # BREAKPOINT 2: Quando avaliar cada vizinho da bifurcação
      if u == '210614077':
        current_dist = dist.get(v, float('inf'))
        will_update = nd < current_dist
        print(f"\n   Avaliando aresta {u} → {v}:")
        print(f"      Peso da aresta: {w:.6f}")
        print(f"      Nova distância: {nd:.6f}")
        print(f"      Distância atual de {v}: {current_dist:.6f}")
        print(f"      {'✅ ATUALIZA' if will_update else '❌ IGNORA'}")
      
      if nd < dist.get(v, float('inf')):
        dist[v] = nd
        prev[v] = u
        heapq.heappush(pq, (nd, v))
        
        # BREAKPOINT 3: Quando inserir na fila de prioridade
        if u == '210614077':
          print(f"      Inserido na fila: ({nd:.6f}, {v})")
  
  return prev, dist

def _reconstruct(prev: dict[str, str], start: str, target: str) -> list[str]:
  path = [target]
  while path[-1] != start:
    if path[-1] not in prev:
      return []
    path.append(prev[path[-1]])
  path.reverse()
  return path

def _closest_reachable_node(dist: dict[str, float], node_xy: dict[str, tuple[float, float]], goal_xy: tuple[float, float]) -> str | None:
  geod = Geod(ellps="WGS84")
  best, bestd = None, float('inf')
  for n in dist.keys():
    xy = node_xy.get(n)
    if not xy:
      continue
    lon, lat = xy
    _, _, d = geod.inv(lon, lat, goal_xy[0], goal_xy[1])
    if d < bestd:
      bestd, best = d, n
  return best

def _nodes_to_rows_path(gdf: gpd.GeoDataFrame, nodes: list[str], pair_to_rows: dict[frozenset[str], list[int]]) -> gpd.GeoDataFrame:
  if len(nodes) < 2:
    return gpd.GeoDataFrame(columns=gdf.columns, geometry='geometry', crs=gdf.crs)
  row_ids: list[int] = []
  for u, v in zip(nodes[:-1], nodes[1:]):
    candidates = pair_to_rows.get(frozenset((u, v)), [])
    if not candidates:
      continue
    # Se houver múltiplos, escolha o menor comprimento
    best = min(candidates, key=lambda i: _geodesic_length_m(gdf.loc[i, 'geometry']))
    row_ids.append(best)
  return gdf.loc[row_ids].reset_index(drop=True)

def shortest_path_by_dijkstra(start_pac: str, dest_pac: str, semi_paths_gdf: gpd.GeoDataFrame, layer_ssdmt: gpd.GeoDataFrame, layer_segcon: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, bool]:
  gdf, adj, node_xy, pair_to_rows = _build_graph(semi_paths_gdf, layer_ssdmt, layer_segcon)
  prev, dist = _dijkstra(adj, start_pac, dest_pac)
  # Caminho completo?
  path_nodes = _reconstruct(prev, start_pac, dest_pac)
  if path_nodes:
    return _nodes_to_rows_path(gdf, path_nodes, pair_to_rows), True
  # Fallback: nó alcançável mais próximo do destino
  # Recupera XY do destino a partir de qualquer linha que contenha o PAC de destino
  rows_with_dest = gdf[gdf['connection'].map(lambda c: dest_pac in c)]
  if rows_with_dest.empty:
    return gpd.GeoDataFrame(columns=gdf.columns, geometry='geometry', crs=gdf.crs), False
  dest_xy = _row_pac_endpoints(rows_with_dest.iloc[0])[dest_pac]
  best = _closest_reachable_node(dist, node_xy, dest_xy)
  if best is None:
    return gpd.GeoDataFrame(columns=gdf.columns, geometry='geometry', crs=gdf.crs), False
  partial_nodes = _reconstruct(prev, start_pac, best)
  return _nodes_to_rows_path(gdf, partial_nodes, pair_to_rows), False

def get_resistence_by_segment_cod_id(cod_id: str, layer_ssdmt: gpd.GeoDataFrame, layer_segcon: gpd.GeoDataFrame):
  segcon_cod_id: str = layer_ssdmt.loc[layer_ssdmt['COD_ID'] == cod_id, 'TIP_CND'].iloc[0]
  ssdmt_lenght: float = layer_ssdmt.loc[layer_ssdmt['COD_ID'] == cod_id, 'COMP'].iloc[0]
  segcon_resistence: float = layer_segcon.loc[layer_segcon['COD_ID'] == segcon_cod_id, 'R1'].iloc[0]
  return (ssdmt_lenght / 1000) * segcon_resistence

def main(study_name: str, point_of_interest: tuple[float, float] | str, point_of_destination: tuple[float, float] | str):
  study_name = to_camel(study_name)
  study_folder_path = os.path.join(STUDIES_FOLDER_PATH, study_name)
  active_gpkg_path = select_active_gpkg(study_folder_path)
  active_gpkg_layer_names = get_needed_layers(active_gpkg_path)

  layer_gdfs: dict[str, gpd.GeoDataFrame] = {layer: gpd.read_file(active_gpkg_path, layer=layer) for layer in active_gpkg_layer_names}

  # Junta segmentos e seccionadoras no mesmo dataframe
  semi_paths_gdf = concat_gdfs([
    create_semi_path_gdf(layer_gdfs['ssdmt'], 'ssdmt'),
    create_semi_path_gdf(layer_gdfs['unsemt'], 'unsemt')
  ])

  # Determina o semi-caminho mais próximo e o PAC de início/fim apropriado
  if isinstance(point_of_interest, tuple):
    nearest_element_of_poi = find_nearest_semi_path(semi_paths_gdf, Point(point_of_interest))
    start_pac = _choose_nearest_endpoint_pac(nearest_element_of_poi, Point(point_of_interest))
  else:
    nearest_element_of_poi = search_semi_path_by_pac(semi_paths_gdf, point_of_interest)
    start_pac = point_of_interest

  if isinstance(point_of_destination, tuple):
    nearest_element_of_pod = find_nearest_semi_path(semi_paths_gdf, Point(point_of_destination))
    dest_pac = _choose_nearest_endpoint_pac(nearest_element_of_pod, Point(point_of_destination))
  else:
    nearest_element_of_pod = search_semi_path_by_pac(semi_paths_gdf, point_of_destination)
    dest_pac = point_of_destination
  
  # Filtra os semi_paths pelo circuito em questão
  ctmt_cod_id = nearest_element_of_poi['ctmt']
  semi_paths_gdf = semi_paths_gdf.loc[semi_paths_gdf['ctmt'] == ctmt_cod_id]

  path_gdf, complete = shortest_path_by_dijkstra(start_pac, dest_pac, semi_paths_gdf, layer_gdfs['ssdmt'], layer_gdfs['segcon'])
  layer_name = 'rota_completa' if complete else 'rota_parcial'
  if not path_gdf.empty:
    path_gdf.to_file('output.gpkg', layer=layer_name, driver='GPKG')

if __name__ == '__main__':
  # study_name = 'Paulo Gallo'
  # point_of_interest = (-53.436704, -24.4522547)
  # point_of_destination = '616104591'
  # main(study_name, point_of_interest, point_of_destination)

  study_name = 'Paulo Ribas'
  point_of_interest = (-50.426937, -24.963988)
  point_of_destination = '210598506'
  main(study_name, point_of_interest, point_of_destination)
