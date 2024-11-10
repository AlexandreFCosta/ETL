from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json

spark = SparkSession.builder.appName("DataQuality").getOrCreate()

print("Data Quality")

# Funções de qualidade de dados
def check_nulls(df, columns):
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in columns]).collect()[0]
    nulls = {col: count for col, count in zip(columns, null_counts) if count > 0}
    if nulls:
        raise ValueError(f"Null values found: {nulls}")
    else:
        print("Sem linhas Null ✅")

def check_duplicates(df, columns):
    duplicate_count = df.groupBy(columns).count().filter("count > 1").count()
    if duplicate_count > 0:
        raise ValueError(f"Found {duplicate_count} duplicate rows.")
    else:
        print("Sem duplicatas ✅")

def count_more_than_zero(data):
    """
    Verifica se o JSON retornado contém ao menos uma entrada.
    
    :param data: Lista de dados extraídos do JSON
    :raises ValueError: Se o JSON estiver vazio.
    """
    if not data or len(data) == 0:
        raise ValueError("O JSON retornado está vazio, não contém entradas.")    
    else:
        print("Json Não Vazio ✅")

def check_json_structure(data, expected_keys):
    if isinstance(data, list):
        for entry in data:
            if not all(key in entry for key in expected_keys):
                raise ValueError(f"JSON structure mismatch: {entry}")
        print("Formato Json Válido ✅")
    else:
        raise ValueError("Invalid JSON format.")

# Função para rodar as verificações de qualidade de dados
def run_data_quality_checks(**kwargs):

    try:
        spark = SparkSession.builder.appName("DataQuality").getOrCreate()
        
        # Carregar dados da camada bronze
        bronze_path = '/opt/airflow/bronze_layer/breweries_raw.json'
        
        # Lendo o arquivo JSON bruto
        with open(bronze_path, 'r') as f:
            data = json.load(f)
        
        # Verificar estrutura do JSON
        expected_keys = [
            "id",
            "name",
            "brewery_type",
            "address_1",
            "address_2",
            "address_3",
            "city",
            "state_province",
            "postal_code",
            "country",
            "longitude",
            "latitude",
            "phone",
            "website_url",
            "state",
            "street"
            ]
        check_json_structure(data, expected_keys)
        count_more_than_zero(data)
        
        # Carregar dados no Spark DataFrame
        df = spark.read.json(bronze_path)

        check_nulls(df, ["name", "city", "state"])
        check_duplicates(df, ["id"])
    
    except Exception as e:
        print(f"Data Quality Check Failed: {e}")
        raise 
    

spark.stop()
