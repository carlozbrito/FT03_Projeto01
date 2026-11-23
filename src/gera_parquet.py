def gera_parquet():
    from src.ibge_moradores import ibge_moradores
    from src.sen_despesas import sen_despesas
    from src.sen_por_uf import sen_por_uf 
    
    sen_por_uf().coalesce(1).write.mode('overwrite').partitionBy('uf').parquet('output/senadores_uf')
    sen_despesas().coalesce(1).write.mode('overwrite').partitionBy('dataDoc').parquet('output/senadores_depesas')
    ibge_moradores().coalesce(1).write.mode('overwrite').partitionBy('localidade_nome').parquet('output/ibge_media_moradores')