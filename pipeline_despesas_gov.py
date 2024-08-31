from google.oauth2 import service_account
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import io
import logging
from unidecode import unidecode


# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Listando Arquivos
def list_files(items):
    if not items:
        logging.info('No files found.')
    else:
        logging.info('Files:')
        for item in items:
            logging.info(f"{item['name']} ({item['id']})")

# Efetuando download dos arquivos em memória e concatenando-os em dataframe
def create_dataframe(items, service):
    all_data = pd.DataFrame()
    
    for item in items:
        logging.info(f"Downloading {item['name']} ({item['id']})")
        
        try:
            # Baixar o arquivo
            request = service.files().get_media(fileId=item['id'])
            file_data = io.BytesIO()
            downloader = MediaIoBaseDownload(file_data, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logging.info(f"Download {int(status.progress() * 100)}%.")
            
            file_data.seek(0)  # Voltar para o início do arquivo
            
            # Ler o arquivo para um DataFrame
            df = pd.read_csv(file_data, encoding='ISO-8859-1', delimiter=';')
            
            # Adicionar os dados ao DataFrame principal
            all_data = pd.concat([all_data, df], ignore_index=True)
            logging.info(f'Arquivo {item["name"]} concatenado!')

        except Exception as e:
            logging.error(f"Erro ao baixar ou processar o arquivo {item['name']}: {e}")

    return all_data

# Função para transformar nomes de colunas inválidos
def sanitize_column_names(df):
    df.columns = [unidecode(col).replace(' ', '_')
                                   .replace('/', '_')
                                   .replace('-', '_')
                                   .replace('(', '')
                                   .replace(')', '')
                                   .replace('$', 'S')
                                   .lower()[:300] for col in df.columns]
    
    # Exibir o DataFrame consolidado
    logging.info("Colunas sanitizadas:")
    logging.info(df.head())
    return df

# Função para mapear tipos de dados do Pandas para BigQuery
def get_bq_schema(df):
    schema = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype.startswith('int'):
            field_type = bigquery.enums.SqlTypeNames.INTEGER
        elif dtype.startswith('float'):
            field_type = bigquery.enums.SqlTypeNames.FLOAT
        elif dtype.startswith('datetime'):
            field_type = bigquery.enums.SqlTypeNames.TIMESTAMP
        elif dtype.startswith('bool'):
            field_type = bigquery.enums.SqlTypeNames.BOOLEAN
        else:
            field_type = bigquery.enums.SqlTypeNames.STRING
        schema.append(bigquery.SchemaField(col, field_type))
    return schema 
     

def ingestion_bigquery(SERVICE_ACCOUNT_FILE, all_data, schema):

    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        dataset_id = 'gold'
        table_id = 'despesas_governo_nacional'
        table_ref = f"{client.project}.{dataset_id}.{table_id}"

        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
        except:
            client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

        try:
            client.get_table(table_ref)
        except:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",  # Use "WRITE_APPEND" para adicionar dados à tabela existente
        )

        job = client.load_table_from_dataframe(
            all_data,
            table_ref,
            job_config=job_config
        )

        job.result()  # Espera o job concluir
        logging.info("DataFrame carregado com sucesso.")

    except Exception as e:
        logging.error(f"Erro na ingestão para o BigQuery: {e}")

# Criando nosso fluxo de dados
def pipeline_despesas_gov():

    SERVICE_ACCOUNT_FILE = 'keyfile.json'
    SCOPES = ['https://www.googleapis.com/auth/drive']

    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        service = build('drive', 'v3', credentials=creds)

        folder_id = '1LKVLxBuLVSLE87Ze0VxvL5ga6V8M8qJN'

        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = service.files().list(q=query, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        list_files(items)
        logging.info('Criando DataFrame')
        all_data = create_dataframe(items, service)
        logging.info('Sanitizando Colunas')
        all_data = sanitize_column_names(all_data)
        logging.info('Identificando Schemas do DataFrame para utilização na tabela do BigQuery')
        schema = get_bq_schema(all_data)
        logging.info('Efetuando a ingestão de dados no BigQuery!')
        ingestion_bigquery(SERVICE_ACCOUNT_FILE, all_data, schema)

    except Exception as e:
        logging.error(f"Erro no pipeline de despesas do governo: {e}")

if __name__ == "__main__":
    pipeline_despesas_gov()
