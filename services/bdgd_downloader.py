from os import path, remove, isatty
from requests import get
from zipfile import ZipFile
import shutil
from tqdm import tqdm
import time

IS_INTERACTIVE = isatty(0) or isatty(1) or isatty(2)
print(f'Interactive: {IS_INTERACTIVE}')
VERBOSE = True
CHUNK_SIZE = 8192

class BDGDDownloader:
    def __init__(self, bdgd_id: str, bdgd_name: str, output_folder: str, extract: bool = False, verbose: bool = VERBOSE):
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
            
            total_size = int(response.headers.get('content-length', 0))

            with open(zip_path, "wb") as f:
                iterator = response.iter_content(chunk_size=CHUNK_SIZE)
                if self.verbose and total_size > 0:
                    with tqdm(
                        iterator,
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        desc=f"Downloading {self.bdgd_name}",
                        leave=False,
                        disable=not IS_INTERACTIVE
                    ) as pbar:
                        for chunk in iterator:
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                elif self.verbose and total_size > 0 and not IS_INTERACTIVE:
                    print(f"Downloading {self.bdgd_name}")
                    for i, chunk in enumerate(iterator, 1):
                        if chunk:
                            print(f'{total_size/i*CHUNK_SIZE:.2f}% downloaded', end='\r')
                            f.write(chunk)
                else:
                    for chunk in iterator:
                        if chunk:
                            f.write(chunk)
            
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
    def __init__(self, output_folder: str, verbose: bool = VERBOSE):
        self.output_folder = output_folder
        self.bdgd_list_path = None
        self.verbose = verbose


    def __enter__(self) -> str:
        self.bdgd_list_path = self.download()
        return self.bdgd_list_path
        
    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

    def download(self) -> str:
        if self.verbose:
            print("Downloading BDGD list", end=' ')
            start_time = time.time()
        with get("https://hub.arcgis.com/api/feed/all/csv?target=dadosabertos-aneel.opendata.arcgis.com") as response:
            response.raise_for_status()
            if self.verbose:
                end_time = time.time()
                print(f"({end_time - start_time:.2f} s)") # type: ignore
            bdgd_list_path = path.join(self.output_folder, "bdgd_list.csv")
            with open(bdgd_list_path, "wb") as f:
                f.write(response.content)

            return bdgd_list_path

    def _cleanup(self):
        """Limpa arquivos temporários"""
        if self.bdgd_list_path and path.exists(self.bdgd_list_path):
            if path.isdir(self.bdgd_list_path):
                shutil.rmtree(self.bdgd_list_path)
            else:
                remove(self.bdgd_list_path)
