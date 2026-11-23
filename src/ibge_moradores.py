import pyspark 
from pyspark.sql import  SparkSession

def ibge_moradores():
    
    import requests
    import pandas as pd
    import pyspark.sql.functions as sq
    import pyspark.sql.types as st
    from pyspark.sql import SQLContext as spark
    from datetime import datetime
    spark = SparkSession.builder.getOrCreate()
    data_atual = datetime.now().strftime("%Y-%m-%d")


    # solicita dados API 
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/579/periodos/-6/variaveis/246?localidades=N3[all]&classificacao=1[6795]"
    #response = requests.get(url,timeout=(3,5))# timeout define tempo de espera para resposta do servidor 

    try:
       # response.raise_for_status()  # Esta método verifica se a resposta é bem-sucedida. (Bem melhor do que verificar o status_code com if)
        #data_sen_despesas = response.json()
        response = requests.get(url,timeout=(3,5))
        response.raise_for_status()
        data_ibge_media_moradores = response.json() 
    except requests.exceptions.HTTPError as erro:
        print(f"HTTP com erro: {erro}")  # Print do erro HTTP
        raise
    except requests.exceptions.RequestException as erro:
        print(f"Ocorreu um erro: {erro}")  # Print erro
        raise

    #Recebe dados da API
    data_ibge_media_moradores = response.json() 

    ibge_media_moradores = st.StructType([
    st.StructField("id", st.StringType(), True),
    st.StructField("variavel", st.StringType(), True),
    st.StructField("unidade", st.StringType(), True),
    st.StructField("resultados", st.ArrayType(
        st.StructType([
            st.StructField("classificacoes", st.ArrayType(
                st.StructType([
                    st.StructField("id", st.StringType(), True),
                    st.StructField("nome", st.StringType(), True),
                    st.StructField("categoria", st.MapType(st.StringType(), st.StringType()), True)
                                    ])
                                ), True),
                                st.StructField("series", st.ArrayType(
                                    st.StructType([
                                        st.StructField("localidade", st.StructType([
                                            st.StructField("id", st.StringType(), True),
                                            st.StructField("nivel", st.StructType([
                                                st.StructField("id", st.StringType(), True),
                                                st.StructField("nome", st.StringType(), True)
                                            ]), True),
                                            st.StructField("nome", st.StringType(), True)
                                        ]), True),
                                        st.StructField("serie", st.MapType(st.StringType(), st.StringType()), True)
                                    ])
                                ), True)
                            ])
                        ), True)
                    ])





    # cria Dataframe Spark, aplicando o schema(sen_despesas_schema ) 
    df_spark_media_moradores_IBGE =  spark.createDataFrame(data_ibge_media_moradores,schema=ibge_media_moradores)
    # parte 1
    df_spark_media_moradores_IBGE = df_spark_media_moradores_IBGE.withColumn('resultados',sq.explode('resultados'))

    #parte 2
    df_spark_media_moradores_IBGE= (
            df_spark_media_moradores_IBGE
            .withColumn("classificacao",sq.explode("resultados.classificacoes"))  
            .withColumn("serie_item",sq.explode("resultados.series"))  
        



    )

    #parte 3
    df_spark_media_moradores_IBGE = (

    df_spark_media_moradores_IBGE
    .select(
    sq.col("id").alias('variavel_id'),
    sq.col('variavel').alias('variavel_nome'),
    sq.col('unidade'),
    sq.col('classificacao.id').alias('classificacao_id'),
    sq.col('classificacao.nome').alias('classificacao_nome') ,           
    sq.explode('classificacao.categoria').alias('categoria_id','categoria_nome'),
    sq.col('serie_item.localidade.id').alias('localidade_id'),
    sq.col("serie_item.localidade.nome").alias("localidade_nome"),
    sq.col("serie_item.localidade.nivel.id").alias("nivel_id"),
    sq.col("serie_item.localidade.nivel.nome").alias("nivel_nome"),
    sq.col("serie_item.serie").alias("serie_map")

    )

    .select("*",sq.explode("serie_map").alias("ano", "valor_str"))
    .drop("serie_map")
    .withColumn('data_agora',sq.current_date())

    )

    return df_spark_media_moradores_IBGE 

