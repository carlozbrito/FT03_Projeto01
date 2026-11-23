import pyspark 
from pyspark.sql import SparkSession

def sen_despesas():
    import requests
    import pandas as pd
    import pyspark.sql.functions as sq
    import pyspark.sql.types as st
    from pyspark.sql import SQLContext as spark
    from datetime import datetime
    spark= SparkSession.builder.getOrCreate()
    data_atual = datetime.now().strftime("%Y-%m-%d")


    # solicita dados API 
    url = "https://adm.senado.gov.br/adm-dadosabertos/api/v1/senadores/despesas_ceaps/2026"
    #response = requests.get(url,timeout=(3,5))# timeout define tempo de espera para resposta do servidor 

    try:
       # response.raise_for_status()  # Esta método verifica se a resposta é bem-sucedida. (Bem melhor do que verificar o status_code com if)
        #data_sen_despesas = response.json()
        response = requests.get(url,timeout=(3,5))
        response.raise_for_status()
        data_sen_despesas = response.json()
    except requests.exceptions.HTTPError as erro:
        print(f"HTTP com erro: {erro}")  # Print do erro HTTP
        raise
    except requests.exceptions.RequestException as erro:
        print(f"Ocorreu um erro: {erro}")  # Print erro
        raise

    #Recebe dados da API
    data_sen_despesas = response.json()

    #Schema de dados para despesas senadores
    sen_despesas_schema = st.StructType([
        st.StructField("id", st.StringType(), True),
        st.StructField("tipoDocumento", st.StringType(), True),
        st.StructField("ano", st.StringType(), True),
        st.StructField("mes", st.StringType(), True),
        st.StructField("codSenador", st.StringType(), True),
        st.StructField("nomeSenador", st.StringType(), True),
        st.StructField("tipoDespesa", st.StringType(), True),
        st.StructField("cpfCnpj", st.StringType(), True),
        st.StructField("fornecedor", st.StringType(), True),
        st.StructField("documento", st.StringType(), True),
        st.StructField("data", st.StringType(), True),
        st.StructField("detalhamento", st.StringType(), True),
        st.StructField("valorReembolsado", st.StringType(), True)
    ])


    df_spark_sen_despesas = spark.createDataFrame(data_sen_despesas,schema=sen_despesas_schema) 



    # realiza : cast em data e valor
    #         : dropa colunas
    df_spark_sen_despesas1 = df_spark_sen_despesas.withColumn('dataDoc',sq.to_date('data'))\
    .withColumn('valorReembolso',df_spark_sen_despesas.valorReembolsado.cast('float'))\
    .drop('data_nc','data_Doc','valorReembolsado','Valor','data')
    return    df_spark_sen_despesas1

    #df_spark_sen_despesas.to_csv(f'data/Extacao_API/data_countries.csv',index=False) 
