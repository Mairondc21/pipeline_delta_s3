import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def create_bucket(client: boto3, region: str, bucket_name: str) -> None:
    try:
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
        print(f"Bucket {bucket_name} criado com sucesso na regiao {region}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logging.error(f"error {error_code}")

def upload_files_s3(client: boto3, path_name: Path, bucket: object) -> None:
    for arquivos in os.listdir(path_name):
        arquivo_completo = os.path.join(path_name, arquivos)
        nome_arquivo = os.path.basename(arquivos).split('.')[0]
        extensao = os.path.basename(arquivos).split('.')[1]

        arquivo_bucket = f"{nome_arquivo}_landing.{extensao}" 

        client.upload_file(arquivo_completo, bucket, arquivo_bucket)
        print(f"arquivo {arquivos} enviado ao bucket {bucket}")
    
    

if __name__ == "__main__":
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("SECRET_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
    )

    # region = 'us-west-1'
    bucket_name = 'mairon-pipeline-delta-s3-landing'

    
    # create_bucket(s3,region,bucket_name)

    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = (BASE_DIR / "../../data").resolve()

    upload_files_s3(s3,DATA_DIR,bucket_name)

