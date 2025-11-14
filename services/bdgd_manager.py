import re
from .bdgd_interface import BDGDDBInterface
from .bdgd_downloader import BDGDDownloader, BDGDListDownloader
import geopandas as gpd
import pandas as pd
from typing import Literal
from tqdm import tqdm
from datetime import datetime
from tempfile import TemporaryDirectory

class BDGDManager:
    """
    Main class to ensure and manage BDGDs. Instantiation will ensure that each most recent search
    needed BDGDs layers in the ANEEL online list is in the database. After instantiation, this class
    permits to download all needed layers of especific BDGDs, using the search layer ones to find the
    BDGD of interest.
    """
    def __init__(self, verbose: bool = False):
        self.interface = BDGDDBInterface()
        self.verbose = verbose
        self.bdgd_list_df = self.get_bdgd_list_df()
        self.download_and_save_all_bdgd_search_layers(2024)
        self.layers_of_interest = [
            'CTMT', 'SEGCON', 'UNTRMT',
            'RAMLIG', 'SSDMT', 'SSDBT',
            'UCAT_tab', 'UCMT_tab', 'UCBT_tab',
            'UGAT_tab', 'UGMT_tab', 'UGBT_tab'
        ]

    # Download do catálogo de bdgds nos dados abertos da ANEEL
    def get_bdgd_list_df(self) -> pd.DataFrame:
        """
        Método para download e normalização da lista de arquivos disponíveis nos dados abertos da ANEEL
        """
        with TemporaryDirectory(prefix='gridflow-bdgd-list-') as temp_folder:
            with BDGDListDownloader(temp_folder) as f:
                return self.normalize_df_bdgd_list(pd.read_csv(f))

    def parse_title(self, title: str) -> tuple[str, str, datetime]:
        """
        Retorna uma tupla com as informações extraídas do título.
        (`bdgd_name`, `dist`, `bdgd_date`)
        """
        date_pattern = r'20\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])'
        match = re.search(date_pattern, title)
        if not match:
            raise ValueError("Data no formato esperado não encontrada no título.")
        bdgd_date = datetime.strptime(match.group(), '%Y-%m-%d')
        dist = title[:match.start()].strip('_').split('_')[-1]
        bdgd_name = title[:title.index(dist)].strip('_')
        return bdgd_name, dist, bdgd_date

    def normalize_df_bdgd_list(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Método para normalização do df responsável por guardar o índice de arquivos disponíveis nos dados abertos da ANEEL
        """
        document_type = 'File Geodatabase'
        required_tags = {'BDGD', 'SIG-R', 'Distribuicao'}
        df = raw_df
        df = df[df['type'] == document_type].reset_index(drop=True)
        df['tags'] = df['tags'].apply(lambda i: set(i.split(',')))
        df = df[df['tags'].apply(lambda tags: required_tags.issubset(tags))]
        df['tags'] = df['tags'].apply(lambda tags: tags - required_tags)
        df['bdgd_name'], df['dist'], df['bdgd_date'] = zip(*df['title'].apply(self.parse_title))
        df = df[['id', 'title', 'bdgd_name', 'dist', 'bdgd_date']]
        df = df.sort_values(by='bdgd_date', ascending=False).reset_index(drop=True)
        return df

    # Download dos bdgds nos dados abertos da ANEEL
    def download_and_save_bdgd_search_layers(self, bdgd_row: pd.Series):
        if self.interface.region_exists(bdgd_row['dist']):
            self.interface.update_bdgd_search_layers_on_db(bdgd_row['dist'])
        
        bdgd_search_gdfs = self.get_all_search_layers_to_gdf(bdgd_row)
        if not bdgd_search_gdfs:
            return
        print(f'\033[31m\n{bdgd_row['title']}\033[m')
        self.interface.save_bdgd_search_layers_to_db(bdgd_search_gdfs)

    def download_and_save_all_bdgd_search_layers(self, year: int):
        filtered_by_year = self.bdgd_list_df.loc[self.bdgd_list_df['bdgd_date'].dt.year == year]
        iterator = filtered_by_year.iterrows()
        if self.verbose:
            iterator = tqdm(
                iterator,
                total=len(filtered_by_year),
                desc="Verificando atualizações",
                leave=False
            )
        for _, row in iterator:
            self.download_and_save_bdgd_search_layers(row)

    def download_and_save_entire_bdgd_by_poi(self, poi: tuple[float, float]):
        region_of_interest = self.interface.get_region_by_poi(poi, mapped = False)
        
        if not region_of_interest:
            return

        self.interface.create_bdgd_schema(region_of_interest) # type: ignore
        
        schema_name = f'{region_of_interest.bdgd_name}_{region_of_interest.bdgd_date.year}'

        with TemporaryDirectory(prefix='gridflow-bdgd-') as temp_folder:
            with BDGDDownloader(region_of_interest.bdgd_id, region_of_interest.bdgd_full_name, temp_folder, True, self.verbose) as bdgd_file:
                layers_iterator = self.layers_of_interest
                
                if self.verbose:
                    layers_iterator = tqdm(
                        layers_iterator,
                        desc=f'Salvando camadas de {region_of_interest.bdgd_name}'
                    )
                    
                first = True
                for layer in layers_iterator:
                    layer_gdf = gpd.read_file(bdgd_file, layer=layer)
                    chunk_size = 10000
                    chunk_iterator = range(0, len(layer_gdf), chunk_size)
                    if self.verbose:
                        chunk_iterator = tqdm(
                            chunk_iterator,
                            desc=f'Salvando {layer}',
                            leave=False
                        )

                    for start in chunk_iterator:
                        end = start + chunk_size
                        chunk = layer_gdf.iloc[start:end]

                        self.interface.save_generic_gdf_to_db(chunk, layer, schema_name, first)
                        first = False

    # Leitura dos GDBs usando GeoDataFrames
    def get_all_search_layers_to_gdf(self, bdgd_row: pd.Series) -> dict[Literal['region', 'substation', 'trhv'], gpd.GeoDataFrame] | None:
        """
        Método para download, normalização e instanciação no DB das camadas necessárias para pesquisa dos BDGDs. Este método fará o download de todos os BDGDs e coletará as camadas de pesquisa de cada um (ARAT, SUB e UNTRAT).
        """
        with TemporaryDirectory(prefix='gridflow-bdgd-') as temp_folder:
            with BDGDDownloader(bdgd_row['id'], bdgd_row['title'], temp_folder, True, self.verbose) as bdgd_file:
                region_gdf = self.normalize_gdf_region(gpd.read_file(bdgd_file, layer='ARAT'), bdgd_row)
                
                substation_gdf = self.normalize_gdf_substation(gpd.read_file(bdgd_file, layer='SUB'))

                trhv_gdf = self.normalize_gdf_trhv(gpd.read_file(bdgd_file, layer='UNTRAT'))

                return {
                    'region': region_gdf,
                    'substation': substation_gdf,
                    'trhv': trhv_gdf
                }

    def normalize_gdf_region(self, raw_gdf: gpd.GeoDataFrame, bdgd_row: pd.Series) -> gpd.GeoDataFrame:
        """
        Método para normalizar o gdf da layer ARAT de um BDGD. Este gdf alimentará a tabela region do DB
        """
        region_gdf = raw_gdf.rename(columns={
            'COD_ID': 'cod_id',
            'DIST': 'dist'
        })
        region_gdf = region_gdf[['cod_id', 'dist', 'geometry']]
        region_gdf['bdgd_name'] = bdgd_row['bdgd_name']
        region_gdf['bdgd_date'] = bdgd_row['bdgd_date']

        region_gdf['bdgd_id'] = bdgd_row['id']

        return region_gdf

    def normalize_gdf_substation(self, raw_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Método para normalizar o gdf da layer SUB de um BDGD. Este gdf alimentará a tabela substation do DB
        """
        substation_gdf = raw_gdf.rename(columns={
            'COD_ID': 'cod_id',
            'DIST': 'dist',
            'NOME': 'name'
        })
        return substation_gdf[['cod_id', 'dist', 'name', 'geometry']]

    def normalize_gdf_trhv(self, raw_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Método para normalizar o gdf da layer UNTRAT de um BDGD. Este gdf alimentará a tabela trhv do DB
        Remove linhas com substation vazio para evitar violação de chave estrangeira.
        """
        trhv_gdf = raw_gdf.rename(columns={
            'COD_ID': 'cod_id',
            'SUB': 'substation',
            'DIST': 'dist',
            'POT_NOM': 'power'
        })
        # Remove linhas com substation vazio ou nulo
        trhv_gdf = trhv_gdf[trhv_gdf['substation'].notnull() & (trhv_gdf['substation'] != '')].copy()
        return trhv_gdf[['cod_id', 'substation', 'dist', 'power', 'geometry']]

if __name__ == '__main__':
    bdgd_manager = BDGDManager(verbose=True)

    poi = -49.72071732124833, -25.555419806716376
    bdgd_manager.download_and_save_entire_bdgd_by_poi(poi)
