# %%
#cria sessão spark e app
import findspark
findspark.init()

import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FT03").getOrCreate()

cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
#print("Trabalhando com:", cores, "core(s)")
spark

# %%
from src.sen_por_uf import sen_por_uf
from src.sen_despesas import sen_despesas
from src.ibge_moradores import ibge_moradores

# %%
sen_por_uf()
sen_despesas()
ibge_moradores()

# %%
from src.gera_parquet import gera_parquet
from src.gera_summary import gera_summary

# %%
gera_parquet()
gera_summary()


