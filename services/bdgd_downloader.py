from os import path, remove
from requests import get
from zipfile import ZipFile
import shutil
from tqdm import tqdm

class BDGDDownloader:
    def __init__(self, bdgd_id: str, bdgd_name: str, output_folder: str, extract: bool = False, verbose: bool = True):
        self.bdgd_id = bdgd_id
        self.bdgd_name = bdgd_name
        self.output_folder = output_folder
        self.extract = extract
        self.verbose = verbose
        self.zip_path = None
        self.bdgd_path = None

    def __enter__(self) -> str:
        self.zip_path = self.download()
        if self.extract:
            self.bdgd_path = self.extract_zip()
            return self.bdgd_path
        return self.zip_path
        
    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

    def download(self) -> str:
        
        with get(f"https://www.arcgis.com/sharing/rest/content/items/{self.bdgd_id}/data", stream=True) as response:
            response.raise_for_status()
            zip_path = path.join(self.output_folder, f"{self.bdgd_name}.zip")
            
            # Obtém o tamanho total do arquivo do cabeçalho Content-Length
            total_size = int(response.headers.get('content-length', 0))
            chunk_size = 8192
            with open(zip_path, "wb") as f:
                iterator = response.iter_content(chunk_size=chunk_size)
                if self.verbose:
                    iterator = tqdm(
                        iterator,
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        desc=f"Baixando {self.bdgd_name}",
                        leave=False
                    )
                for chunk in iterator:
                    if chunk:
                        f.write(chunk)
                        if self.verbose:
                            iterator.update(chunk_size) # type: ignore
            
            return zip_path
    
    def extract_zip(self) -> str:
        """Extrai o arquivo ZIP e retorna o caminho do GDB extraído"""
        if not self.zip_path or not path.exists(self.zip_path):
            raise FileNotFoundError(f"ZIP file {self.zip_path} does not exist.")
        
        with ZipFile(self.zip_path, "r") as zip_ref:
            # TODO Implementar tqdm se verboso
            files = zip_ref.infolist()
            gdb_file = files[0].filename.split('/')[0]
            extract_path = path.join(self.output_folder, gdb_file)
            zip_ref.extractall(self.output_folder)
            return extract_path
        return extract_path

    def _cleanup(self):
        """Limpa arquivos temporários"""
        if self.zip_path and path.exists(self.zip_path):
            remove(self.zip_path)
        if self.bdgd_path and path.exists(self.bdgd_path):
            if path.isdir(self.bdgd_path):
                shutil.rmtree(self.bdgd_path)
            else:
                remove(self.bdgd_path)

class BDGDListDownloader:
    def __init__(self, output_folder: str, verbose: bool = True):
        self.output_folder = output_folder
        self.verbose = verbose
        self.bdgd_list_path = None

    def __enter__(self) -> str:
        self.bdgd_list_path = self.download()
        return self.bdgd_list_path
        
    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

    def download(self) -> str:
        
        with get("https://hub.arcgis.com/api/feed/all/csv?target=dadosabertos-aneel.opendata.arcgis.com", stream=True) as response:
            response.raise_for_status()
            bdgd_list_path = path.join(self.output_folder, "bdgd_list.csv")
            
            # Obtém o tamanho total do arquivo do cabeçalho Content-Length
            total_size = int(response.headers.get('content-length', 0))
            
            with open(bdgd_list_path, "wb") as f:
                if self.verbose and total_size > 0:
                    # Cria barra de progresso com tqdm
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading bdgd_list") as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                else:
                    f.write(response.content)

            return bdgd_list_path

    def _cleanup(self):
        """Limpa arquivos temporários"""
        if self.bdgd_list_path and path.exists(self.bdgd_list_path):
            if path.isdir(self.bdgd_list_path):
                shutil.rmtree(self.bdgd_list_path)
            else:
                remove(self.bdgd_list_path)
