import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date, col
from pyspark.sql.types import DateType

# --- Inicialização do Spark Session
try:
    spark = SparkSession.builder \
        .appName("Análise de Estabelecimento de FastFood") \
        .getOrCreate()
    print("SparkSession criada com sucesso!")

except Exception as e:
    print(f"Erro ao criar SparkSession: {e}")

# --- Fim da inicialização do Spark Session

def calculate_red_violations(data_source):
    """
    Processa dados de amostras de inspecao de estabelecimentos alimenticios e 
    consulta os dados para encontrar os 10 principais estabelecimentos com mais 
    infracoes vermelhas de 2006 a 2020.
    - 10 principais estabelecimentos com mais infracoes vermelhas
    - 10 principais cidades com mais infracoes vermelhas

    :param data_source: The URI of your food establishment data CSV, such as 's3://amzn-s3-demo-bucket/food-establishment-data.csv'.
    :param output_uri: The URI where output is written, such as 's3://amzn-s3-demo-bucket/restaurant_violation_results'.
    """

    #Carregando os dados do restaurante em CSV
    if data_source is not None:
        restaurants_df = spark.read.option("header", "true").csv(data_source)

    #Renomeando colunas para nomes sem espaco
    restaurants_df = restaurants_df.withColumnRenamed("Inspection Date", "inspection_date")
    restaurants_df = restaurants_df.withColumnRenamed("City", "city")
    restaurants_df = restaurants_df.withColumnRenamed("Violation Type", "violation_type")

    #Transformar a coluna inspection_date para o tipo data e extrair o ano
    restaurants_df = restaurants_df.withColumn("inspection_year", year(to_date(col("inspection_date"), "MM/DD/YYYY")))

    #Criar uma view temporaria para query SQL
    restaurants_df.createOrReplaceTempView("restaurant_violations")

    #Query para os top 10 estabelecimentos com mais infracoes vermelhas
    top_red_violation_restaurants = spark.sql("""
        SELECT
            name,
            COUNT(*) AS total_red_violations
            FROM
                restaurant_violations
            WHERE
                violation_type = 'RED' AND
                inspection_year >= 2006 AND inspection_year <= 2025
            GROUP BY
                name
            ORDER BY
                total_red_violations DESC
            LIMIT 10
    """)

    #Query para as top 10 cidades com mais infracoes vermelhas
    top_red_violation_cities = spark.sql("""
        SELECT
            city,
            COUNT(*) AS total_red_violations
            FROM
                restaurant_violations
            WHERE
                violation_type = 'RED' AND
                inspection_year >= 2006 AND inspection_year <= 2025
            GROUP BY
                city
            ORDER BY
                total_red_violations DESC
            LIMIT 10
    """)

    return top_red_violation_restaurants, top_red_violation_cities

# Execucao no Jupyter Notebook
data_source = 's3://tarefa-emr/Food_Establishment_Inspection_Data_20250608.csv'
output_uri_restaurants = 's3://tarefa-emr/saida-exercicio/top_restaurants_violations'
output_uri_cities = 's3://tarefa-emr/saida-exercicio/top_cities_violations'

#Chamar a funcao e obter os DataFrames de resultado
top_restaurants_df, top_cities_df = calculate_red_violations(data_source)

#Exibir resultados
print("Top 10 estabelecimentos com mais infracoes vermelhas")
top_restaurants_df.show(truncate=False)

print("\nTop 10 cidades com mais infracoes vermelhas")
top_cities_df.show(truncate=False)

#Escrever os resultados para os URIs
print(f"Escrevendo resultados para {output_uri_restaurants}")
top_restaurants_df.write.option("header", "true").mode("overwrite").csv(output_uri_restaurants)

print(f"Escrevendo resultados para {output_uri_cities}")
top_cities_df.write.option("header", "true").mode("overwrite").csv(output_uri_cities)

print(f"Processamento e escrita concluidos!")


			