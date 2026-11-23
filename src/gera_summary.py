def gera_summary() :
        from src.ibge_moradores import ibge_moradores
        from src.sen_despesas import sen_despesas
        from src.sen_por_uf import sen_por_uf 

        sen_por_uf().summary().toPandas().to_csv('summary_data_quality/senadores_uf_summary.csv')
        sen_despesas().summary().toPandas().to_csv('summary_data_quality/sen_despesas_summary.csv')
        ibge_moradores().summary().toPandas().to_csv('summary_data_quality/ibge_media_moradores.csv')


        return