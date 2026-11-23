import pyspark 
from pyspark.sql import SparkSession
def sen_por_uf():
    import requests
    import pandas as pd
    import pyspark.sql.functions as sq
    import pyspark.sql.types as st
    from datetime import datetime
    spark= SparkSession.builder.getOrCreate()
    data_atual = datetime.now().strftime("%Y-%m-%d")
    


    url = "https://adm.senado.gov.br/adm-dadosabertos/api/v1/senadores?formato=json"
    #response = requests.get(url,timeout=(3,5))# timeout define tempo de espera para resposta do servidor 

    try:
       # response.raise_for_status()  # Esta método verifica se a resposta é bem-sucedida. (Bem melhor do que verificar o status_code com if)
       response = requests.get(url,timeout=(3,5))
       response.raise_for_status()
       data_sen_uf = response.json()
    except requests.exceptions.HTTPError as erro:
        print(f"HTTP com erro: {erro}")  # Print do erro HTTP
        raise
    except requests.exceptions.RequestException as erro:
        print(f"Ocorreu um erro: {erro}")  # Print erro
        raise



    ##schema para dados de senadores
    senadores_uf_schema = st.StructType(fields=([st.StructField("codigoParlamentar",st.StringType(),True),
                                            st.StructField("nomeParlamentar",st.StringType(),True),
                                            st.StructField("titularSuplente",st.StringType(),True),
                                            st.StructField("uf",st.StringType(),True),
                                            st.StructField("partido",st.StringType(),True),
                                            st.StructField("endereco",st.StringType(),True),
                                            st.StructField("telefones",st.StringType(),True),
                                            st.StructField("fax",st.StringType(),True),
                                            st.StructField("dataNascimento",st.StringType(),True),
                                            st.StructField("email",st.StringType(),True),
                                            st.StructField("chefeGabinete",st.StringType(),True),
                                            st.StructField("aniversario",st.StringType(),True),
                                            st.StructField("mandato",st.StringType(),True),                                          
                                            ]))




    # cria Dataframe Spark, aplicando o schema(sen_despesas_schema ) 
    df_spark_sen_uf =  spark.createDataFrame(data_sen_uf["data"],schema=senadores_uf_schema)
    df_spark_sen_uf = df_spark_sen_uf.withColumn('dataNascimento_NC',sq.to_date('dataNascimento'))
    
    return df_spark_sen_uf
