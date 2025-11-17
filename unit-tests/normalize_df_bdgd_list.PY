from datetime import datetime
import re
import pandas as pd
import requests

def parse_title(title: str) -> tuple[str, str, datetime]:
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
    bdgd_name = title[:title.index(dist)].strip('_').lower()
    return bdgd_name, dist, bdgd_date

def normalize_df_bdgd_list(raw_df: pd.DataFrame) -> pd.DataFrame:
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
    df['bdgd_name'], df['dist'], df['bdgd_date'] = zip(*df['title'].apply(parse_title))
    df = df[['id', 'title', 'bdgd_name', 'dist', 'bdgd_date']]
    df = df.sort_values(by='bdgd_date', ascending=False).reset_index(drop=True)
    return df

if __name__ == "__main__":
    file_path = 'test_file.csv'
    with open(file_path, 'wb') as temp_file:
        temp_file.write(requests.get('https://hub.arcgis.com/api/feed/all/csv?target=dadosabertos-aneel.opendata.arcgis.com').content)
    raw_df = pd.read_csv(file_path)
    print(normalize_df_bdgd_list(raw_df))