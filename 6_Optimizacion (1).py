# # **Pricing: Willingness to pay CEF**
# Tema : Optimizacion de Tasa
# Fecha última modificación: 27/02/2021
#---

# ========================================================================================
# CRUCE VARIABLES
# ========================================================================================

# ========================================================================================
# Importar librerías
# ========================================================================================

import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import *
import time
import pandas as pd
import random

# Setear ruta de inicio
os.chdir('/home/cdsw')

import json
#matriculabs = 'bs80423'
matriculabs = 'bt24946'
#matriculabs = 'bt38919'

# ========================================================================================
# Iniciar sesión en Spark
# ========================================================================================

# Configurar el appName con un nombre descriptivo acerca de lo que se realizará en la sesión. 
# Dejar por defecto el resto de parámetros.

spark = SparkSession.builder.\
  appName("Pricing_WTP_CEF").\
  config("spark.driver.memory", "16g").\
  config("spark.driver.memoryOverhead", "4g").\
  config("spark.executor.cores", "4").\
  config("spark.executor.memory", "16g").\
  config("spark.dynamicAllocation.maxExecutors", "20").\
  config("spark.executor.memoryOverhead", "8g").\
  config("spark.sql.shuffle.partitions", "100").\
  config("spark.sql.broadcastTimeout", "1200").\
  config("spark.jars", "ojdbc7.jar").\
  config("spark.driver.extraClassPath", "ojdbc7.jar").\
  config("spark.executor.extraJavaOptions", "-Doracle.net.crypto_checksum_client=REQUESTED,-Doracle.net.crypto_checksum_types_client=SHA1,-Doracle.net.encryption_client=REQUIRED,-Doracle.net.encryption_types_client=AES256, -Doracle.jdbc.timezoneAsRegion=false,-Dhadoop.security.credential.provider.path=jceks://hdfs/user/"+matriculabs+"/password.jceks").\
  config("spark.driver.extraJavaOptions", "-Doracle.net.crypto_checksum_client=REQUESTED,-Doracle.net.crypto_checksum_types_client=SHA1,-Doracle.net.encryption_client=REQUIRED,-Doracle.net.encryption_types_client=AES256, -Doracle.jdbc.timezoneAsRegion=false,-Dhadoop.security.credential.provider.path=jceks://hdfs/user/"+matriculabs+"/password.jceks").\
  config("spark.hadoop.hadoop.security.credential.provider.path","jceks://hdfs/user/"+matriculabs+"/password.jceks").\
  enableHiveSupport().\
  master("yarn").\
  getOrCreate()
  
spark.conf.set("spark.sql.shuffle.partitions", "20")



# ========================================================================================
# Parámetros
# ========================================================================================
# Nombre del modelo
model = 'Pricing_WTP_CEF'
label = 'wtp_cef'
campana_aux = 'blaze' #INV o blaze


# Obtener el usuario en Datalake 
hdfs_user_name = spark.sparkContext.sparkUser()
hdfs_user_name = hdfs_user_name.replace('b','').replace('B','')


# Definir la ruta de Hue desde donde se extraerán los datos
path_hdfs_sandbox_data_root = f"/prod/bcp/edv/pric/t24946/202008_WTP_CEF/implementation_bkp/data"

# Definir la ruta de CDSW
path_cdsw_data = f"implementation_bkp/data"


# ========================================================================================
# Meses
# ========================================================================================
codmes = 202401
indicators_reported = ['VAN']
indicator_target = 'VAN'

# ========================================================================================
## Leer
# ========================================================================================
nombre_base_muestra = f'df_seg_opt_mx_efectividad_{codmes}'
df_seg_opt_mx_efectividad = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/4_Iteraciones/{nombre_base_muestra}_{campana_aux}") #AGREGAR BLAZE

# ========================================================================================
# Caps de Iteraciones
# ========================================================================================
import pyspark.sql.functions as f
df_seg_opt= df_seg_opt_mx_efectividad

# Indicadores Estimados
for ind in indicators_reported:
  df_seg_opt = df_seg_opt.withColumn(f'{ind}_estimado',f.col(f'{ind}')*f.col('prob_adj_ef'))

# MTO estimado
df_seg_opt = df_seg_opt.withColumn('MTO_estimado',f.col('mtofinalofertadosol')*f.col('prob_adj_ef'))

## LÍMITE DE ITERACIONES
df_seg_opt_caps = df_seg_opt.where(((f.col('seg_wtp_cef')==1)&(f.col('iter')>=-8)&(f.col('iter')<=8))|\
                                   ((f.col('seg_wtp_cef')==2)&(f.col('iter')>=-7)&(f.col('iter')<=7))|\
                                   ((f.col('seg_wtp_cef')==3)&(f.col('iter')>=-6)&(f.col('iter')<=6))|\
                                   ((f.col('seg_wtp_cef')==4)&(f.col('iter')>=-5)&(f.col('iter')<=5))|\
                                   ((f.col('seg_wtp_cef')==5)&(f.col('iter')>=-4)&(f.col('iter')<=4))|\
                                   ((f.col('seg_wtp_cef')==6)&(f.col('iter')>=-3)&(f.col('iter')<=3))|\
                                   ((f.col('seg_wtp_cef')==7)&(f.col('iter')>=-2)&(f.col('iter')<=2)) \
                                  )

# EXTRAE LOS CODCLAVECIC Y FLG_SUBSEGMENTOS
nombre_base_muestra = f'TasasProducto_{codmes}'
df_subsegmento = spark.read\
                        .format('csv')\
                        .option("header",True)\
                        .load(f"{path_hdfs_sandbox_data_root}/in/2_CLVs/{nombre_base_muestra}_{campana_aux}.csv") # AGREGA BLAZE

df_seg_opt_caps = df_seg_opt_caps.join(df_subsegmento.select('llave','FLG_SUBSEGMENTO_CONSUMO'), on='llave', how='left') 
      
cond1 = (f.col('tasa_propuesta') < 0.145) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['ARMANDO','NO CLIENTE']))
cond2 = (f.col('tasa_propuesta') < 0.195) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['BEATRIZ']))
cond3 = (f.col('tasa_propuesta') < 0.235) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['CARLOS']))
cond4 = (f.col('tasa_propuesta') < 0.135) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['BEX']))
cond5 = (f.col('tasa_propuesta') < 0.125) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['ENALTA']))

cond7 = (f.col('tasa_propuesta') > 0.959) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['ARMANDO', 'BEATRIZ', 'CARLOS', 'NO CLIENTE']))
cond8 = (f.col('tasa_propuesta') > 0.429) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['BEX']))
cond9 = (f.col('tasa_propuesta') > 0.199) & (f.col('FLG_SUBSEGMENTO_CONSUMO').isin(['ENALTA']))


df_seg_opt_caps_f = df_seg_opt_caps.where(~(cond1 | cond2 | cond3 | cond4 | cond5 | cond7 | cond8 | cond9 ))

# Guardar
nombre_base_muestra = f'1f_dta_iter_opt_caps_{campana_aux}'
df_seg_opt_caps_f.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE

# ========================================================================================
# Datos Agrupados a la Tasa Lead
# ========================================================================================

df_res_opt = \
df_seg_opt.where((f.col('iter')==0))\
          .agg(f.count('*').alias('clientes'),
               f.sum('mtofinalofertadosol').alias('oferta'),
               f.avg('prob_adj_ef').alias('prob'),
               f.avg('tasa_propuesta').alias('tasa_cli'),
               (f.sum(f.col('tasa_propuesta')*f.col('mtofinalofertadosol'))/f.sum('mtofinalofertadosol')).alias('tasa_pnd'),
               f.sum('mto_estimado').alias('mto_estimado'),
               f.sum(f.when(isnan(f.col('van_estimado')),0).otherwise(f.col('van_estimado'))).alias('van_estimado')
               )

# Guardar
nombre_base_muestra = f'2f_df_res_opt'
df_res_opt.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}_{campana_aux}") #AGREGAR BLAZE

# Leer
nombre_base_muestra = f'2f_df_res_opt'
df_res_opt = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}_{campana_aux}") #AGREGAR BLAZE

df_res_opt.show()

# ========================================================================================
# Leer base Caps
# =========================================
nombre_base_muestra = f'1f_dta_iter_opt_caps_{campana_aux}'
df_seg_opt_caps = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE

# ========================================================================================
#
#df_res_pd = pd.DataFrame()
#
#emptyRDD = spark.sparkContext.emptyRDD()
#
#from pyspark.sql.types import StructType,StructField, StringType
#schema = StructType([
#  StructField('clientes', StringType(), True),
#  StructField('oferta', StringType(), True),
#  StructField('prob', StringType(), True),
#  StructField('tasa_cli', StringType(), True),
#  StructField('tasa_pnd', StringType(), True),
#  StructField('mto_estimado', StringType(), True),
#  StructField('van_estimado', StringType(), True),
#  StructField('prob_e', StringType(), True),
#  StructField('tasa_cli_e', StringType(), True),
#  StructField('tasa_pnd_e', StringType(), True),
#  StructField('mto_estimado_e', StringType(), True),
#  StructField('van_estimado_e', StringType(), True),
#  StructField('exp', StringType(), True),
#  StructField('dif_prob', StringType(), True),
#  StructField('dif_t_cli', StringType(), True),
#  StructField('dif_t_pnd', StringType(), True),
#  StructField('dif_mto_est', StringType(), True),
#  StructField('dif_van_est', StringType(), True),
#  StructField('pct_prob', StringType(), True),
#  StructField('pct_t_cli', StringType(), True),
#  StructField('pct_t_pnd', StringType(), True),
#  StructField('pct_mto_est', StringType(), True),
#  StructField('pct_van_est', StringType(), True)
#  ])
#
##Create empty DataFrame from empty RDD
#df_res_pd_prueba = spark.createDataFrame(emptyRDD,schema)
## ========================================================================================


# ========================================================================================

import numpy as np


for exp in np.arange(0.5,10,0.5):
#  exp = 0.5
  # ========================================================================================
  # Función Objetivo
  # ========================================================================================

  df_seg_opt_f = df_seg_opt_caps.withColumn('obj_funct_p_1', f.col('VAN')*pow(f.col('prob_adj_ef'),exp))
  df_seg_opt_f = df_seg_opt_f.fillna(0)

  exp_agg = [f.max(f'{ind}_estimado').alias(f'm_{ind}_estimado') for ind in indicators_reported] + \
            [f.sum(f'{ind}_estimado').alias(f's_{ind}_estimado') for ind in indicators_reported] + \
            [f.max('obj_funct_p_1').alias('m_obj_funct_p_1')] + \
            [f.sum('obj_funct_p_1').alias('s_obj_funct_p_1')]

  df_sen = df_seg_opt_f.groupBy('llave') \
                       .agg(*exp_agg)


  # JOIN: FUNCIONES OBJETIVO
  # -----------------------------------------------------------------------------------------------------
  df_seg_opt_f = df_seg_opt_f.join(df_sen, on = 'llave', how = 'left') 

  # -----------------------------------------------------------------------
  # Cruce y Selección de Tasas #codclavecic
  # -----------------------------------------------------------------------
  exp_agg = \
  [f.sum(f.when(f.col('iter')==0,f.col('mtofinalofertadosol'))).alias('OFERTA')] +\
  [f.sum(f.when(f.col('iter')==0,f.col('prob_adj_ef'))).alias('PROB_LEAD')] +\
  [f.sum(f.when(f.col('iter')==0,f.col('ITER'))).alias('ITER_LEAD')] +\
  [f.min(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col('ITER')).otherwise(f.when(f.col(f'{indicator_target}_estimado') ==f.col(f'm_{indicator_target}_estimado') ,f.col('ITER')))).alias('ITER_SR')] +\
  [f.min(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('ITER')).otherwise(f.when(f.col('obj_funct_p_1')==f.col('m_obj_funct_p_1'),f.col('ITER')))).alias('ITER_OF_1')] +\
  [f.sum(f.when(f.col('iter')==0,f.col('TASA_PROPUESTA'))).alias('TASA_LEAD')] +\
  [f.min(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col('TASA_PROPUESTA')).otherwise(f.when(f.col(f'{indicator_target}_estimado').cast('decimal(32,10)') ==f.col(f'm_{indicator_target}_estimado').cast('decimal(32,10)') ,f.col('TASA_PROPUESTA')))).alias('TASA_SR')] +\
  [f.min(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('TASA_PROPUESTA')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col('TASA_PROPUESTA')))).alias('TASA_OF_1')] +\
  [f.sum(f.when(f.col('iter')==0,f.col('MTO_estimado'))).alias('MTO_LEAD')] +\
  [f.min(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col('MTO_estimado')).otherwise(f.when(f.col(f'{indicator_target}_estimado').cast('decimal(32,10)') ==f.col(f'm_{indicator_target}_estimado').cast('decimal(32,10)') ,f.col('MTO_estimado')))).alias('MTO_SR')] +\
  [f.min(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('MTO_estimado')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col('MTO_estimado')))).alias('MTO_OF_1')] +\
  [f.sum(f.when(f.col('iter')==0,f.col(f'{ind}_estimado'))).alias(f'{ind}_LEAD') for ind in indicators_reported]  +\
  [f.min(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col(f'{ind}_estimado')).otherwise(f.when(f.col(f'{indicator_target}_estimado').cast('decimal(32,10)') ==f.col(f'm_{indicator_target}_estimado').cast('decimal(32,10)') ,f.col(f'{ind}_estimado')))).alias(f'{ind}_SR') for ind in indicators_reported]  +\
  [f.min(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col(f'{ind}_estimado')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col(f'{ind}_estimado')))).alias(f'{ind}_OF_1') for ind in indicators_reported]

  df_seg_opt_a = df_seg_opt_f.groupBy('llave')\
                             .agg(*exp_agg)

  # -----------------------------------------------------------------------------------------------------
  # -----------------------------------------------------------------------------------------------------

  # Cruce con Base Caps #codclavecic
  # ---------------------------------
  df_seg_opt_b = df_seg_opt_caps.select(['codclavecic','llave','tasa_propuesta','iter'])\
                                .join(df_seg_opt_a.select(['llave','tasa_of_1']),
                                      on = 'llave',
                                      how = 'inner')


  # NUEVA FORMA
  df_seg_opt_d = df_seg_opt_b.filter(f.col('tasa_propuesta')==f.col('tasa_of_1')).distinct()
  
  # FORMA ANTIGUA
  # Crear Campo Diferencia más cercana a la tasa con caps
  #df_seg_opt_b = df_seg_opt_b.withColumn('dif_abs',abs(f.col('tasa_propuesta')-f.col('tasa_of_1')).cast('decimal(32,10)'))
  #
  ## Crear base Codclavecic y diferencia minima
  #df_seg_opt_c = df_seg_opt_b.groupby('llave').agg(f.min('dif_abs').cast('decimal(32,10)').alias('dif_abs')) #codclavecic

  # Cruce: Nos quedamos con la Tasa Propuesta WTP
  #df_seg_opt_d = df_seg_opt_b.join(df_seg_opt_c, on = ['llave','dif_abs'],how = 'inner')

  # Base datos con la Tasa Propuesta WTP
  df_seg_opt_e = df_seg_opt_caps.join(df_seg_opt_d.select(['llave','tasa_propuesta']), #codclavecic
                                 on = ['llave','tasa_propuesta'], #codclavecic
                                 how = 'inner')


  exp_agg = \
  [f.avg('prob_adj_ef').alias('prob_e')] +\
  [f.avg('tasa_propuesta').alias('tasa_cli_e'),] +\
  [(f.sum(f.col('tasa_propuesta')*f.col('mtofinalofertadosol'))/f.sum('mtofinalofertadosol')).alias('tasa_pnd_e')] +\
  [f.sum('mto_estimado').alias('mto_estimado_e')] +\
  [f.sum(f.when(isnan(f.col(f'{ind}_estimado')),0).otherwise(f.col(f'{ind}_estimado'))).alias(f'{ind}_estimado_e') for ind in indicators_reported]


  df_res_exp1 = df_seg_opt_e.agg(*exp_agg)

  # NUEVA FORMA
  df_res_exp1 = df_res_exp1.withColumn('join_column', lit(1))
  df_res_opt_0 = df_res_opt.withColumn('join_column', lit(1))

  df_res_exp2 = df_res_opt_0.crossJoin(df_res_exp1)
  df_res_exp2 = df_res_exp2.drop('join_column')

  # FORMA ANTIGUA
  #df_res_exp1 = \
  #df_seg_opt_e.agg(f.count('*').alias('clientes'),
  #                 f.sum('mtofinalofertadosol').alias('oferta'),
  #                 f.avg('prob_adj_ef').alias('prob_e'),
  #                 f.avg('tasa_propuesta').alias('tasa_cli_e'),
  #                 (f.sum(f.col('tasa_propuesta')*f.col('mtofinalofertadosol'))/f.sum('mtofinalofertadosol')).alias('tasa_pnd_e'),
  #                 f.sum('mto_estimado').alias('mto_estimado_e'),
  #                 f.sum(f.when(isnan(f.col('van_estimado')),1).otherwise(f.col('van_estimado'))).alias('van_estimado_e')
  #                )


  # PARCHE DE FREBRERO PORQUE NO CORRIA. POSIBLES DUPLICADOS
  #df_res_exp1 = df_res_exp1.withColumn('clientes',lit(584002))
  #df_res_exp1 = df_res_exp1.withColumn('oferta',lit(35080521837.0000))

  #df_res_exp2 = df_res_opt.join(df_res_exp1,
  #                              on = ['clientes','oferta'],
  #                              how = 'left')


  df_res_exp2 = df_res_exp2.withColumn('exp',lit(exp))
  df_res_exp2 = df_res_exp2.withColumn('dif_prob',   f.col('prob_e')-f.col('prob'))
  df_res_exp2 = df_res_exp2.withColumn('dif_t_cli',  f.col('tasa_cli_e')-f.col('tasa_cli'))
  df_res_exp2 = df_res_exp2.withColumn('dif_t_pnd',  f.col('tasa_pnd_e')-f.col('tasa_pnd'))
  df_res_exp2 = df_res_exp2.withColumn('dif_mto_est',f.col('mto_estimado_e')-f.col('mto_estimado'))
  for ind in indicators_reported:
    df_res_exp2 = df_res_exp2.withColumn(f'dif_{ind}_est',f.col(f'{ind}_estimado_e')-f.col(f'{ind}_estimado'))

  df_res_exp2 = df_res_exp2.withColumn('pct_prob',   (f.col('prob_e')-f.col('prob'))/f.col('prob'))
  df_res_exp2 = df_res_exp2.withColumn('pct_t_cli',  (f.col('tasa_cli_e')-f.col('tasa_cli'))/f.col('tasa_cli'))
  df_res_exp2 = df_res_exp2.withColumn('pct_t_pnd',  (f.col('tasa_pnd_e')-f.col('tasa_pnd'))/f.col('tasa_pnd'))
  df_res_exp2 = df_res_exp2.withColumn('pct_mto_est',(f.col('mto_estimado_e')-f.col('mto_estimado'))/f.col('mto_estimado'))

  for ind in indicators_reported:
    df_res_exp2 = df_res_exp2.withColumn(f'pct_{ind}_est',(f.col(f'{ind}_estimado_e')-f.col(f'{ind}_estimado'))/f.col(f'{ind}_estimado'))


  if exp == 0.5:
    df_res_pd_0 = df_res_exp2
  else:
    df_res_pd_0 = df_res_pd_0.union(df_res_exp2)
  #df_res_pd_prueba = df_res_pd_prueba.union(df_res_exp2)
  print(f"Terminó: Exponente - {exp}")


# Guardar
nombre_base_muestra = f'2_df_ResultadosOptimizaciónParquet_f'
df_res_pd_0.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}")

## Leer
nombre_base_muestra = f'2_df_ResultadosOptimizaciónParquet_f'
df_res_pd_0 = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") 

# A PANDAS   
df_res_pd = df_res_pd_0.toPandas()

# Guardar
df_res_pd.to_csv(f"{path_cdsw_data}/out/5_Optimizacion/1_Resultados_TradeOff_efect_{codmes}_{campana_aux}.csv") #AGREGAR BLAZE
#df_res_pd.to_csv(f"{path_cdsw_data}/out/5_Optimizacion/1_Resultados_TradeOff_efect_{codmes}_{campana_aux}_base.csv") #AGREGAR BLAZE
#df_res_pd.to_csv(f"{path_cdsw_data}/out/5_Optimizacion/1_Resultados_TradeOff_efect_{codmes}_{campana_aux}_share.csv") #AGREGAR BLAZE


# ========================================================================================
# Función Objetivo
# ========================================================================================

# OCT 22 : 2.5
# NOV 22 : 1.5
# DIC 22 : 1.5
# ENE 23 : 3
# FEB 23 : 5.5
# MAR 23 : 4.5
# ABR 23 : 4.5
# MAY 23 : 6
# JUN 23 : 5
# JUL 23 : 4.5
# AGO 23 : 5
# SEP 23 : 5
# OCT 23 : 5.5
# NOV 23 : 5
# DIC 23 : 5
# ENE 24 : 2.5

# INV NOV 22 : 1
# INV DIC 22 : 1.5
# INV ENE 23 : 6.5
# INV FEB 23 : 4.5
# INV MAR 23 : 4.0
# INV ABR 23 : 4.5
# INV MAY 23 : 5.5
# INV JUN 23 : 4.5
# INV AGO 23 : 4.5
# INV set 23 : 5.5
# INV oct 23 : 6
# INV NOV 23 : 5
# INV dic 23 : 5
# INV ENE 24 : 2.5

# ADD ENE 22 : 3.5
# ADD ABR 22 : 4

# ADD2 ABR 22 : 4.5


df_seg_opt_f = df_seg_opt_caps.withColumn('obj_funct_p_1', f.col('VAN')*pow(f.col('prob_adj_ef'),2.5))
df_seg_opt_f = df_seg_opt_f.fillna(0)


#codclavecic
df_sen = df_seg_opt_f.groupby('llave')\
                     .agg(f.max('van_estimado').alias('m_van_estimado'),
                          f.max('obj_funct_p_1').alias('m_obj_funct_p_1'),
                          f.sum('van_estimado').alias('s_van_estimado'),
                          f.sum('obj_funct_p_1').alias('s_obj_funct_p_1')
                         )

# JOIN: FUNCIONES OBJETIVO
# -----------------------------------------------------------------------------------------------------
df_seg_opt_f = df_seg_opt_f.join(df_sen, on = 'llave', how = 'left') #codclavecic


# -----------------------------------------------------------------------
# Cruce y Selección de Tasas
# -----------------------------------------------------------------------
#codclavecic
df_seg_opt_a = df_seg_opt_f.groupby('llave')\
                           .agg(
                                f.sum(f.when(f.col('iter')==0,f.col('mtofinalofertadosol'))).alias('OFERTA'),

                                f.sum(f.when(f.col('iter')==0,f.col('prob_adj_ef'))).alias('PROB_LEAD'),

                                f.sum(f.when(f.col('iter')==0,f.col('ITER'))).alias('ITER_LEAD'),
                                f.sum(f.when((f.col('s_van_estimado') ==0)&(f.col('iter')==0),f.col('ITER')).otherwise(f.when(f.col('van_estimado') ==f.col('m_van_estimado') ,f.col('ITER')))).alias('ITER_SR'),
                                f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('ITER')).otherwise(f.when(f.col('obj_funct_p_1')==f.col('m_obj_funct_p_1'),f.col('ITER')))).alias('ITER_OF_1'),

                                f.sum(f.when(f.col('iter')==0,f.col('TASA_PROPUESTA'))).alias('TASA_LEAD'),
                                f.sum(f.when((f.col('s_van_estimado') ==0)&(f.col('iter')==0),f.col('TASA_PROPUESTA')).otherwise(f.when(f.col('van_estimado').cast('decimal(32,10)') ==f.col('m_van_estimado').cast('decimal(32,10)') ,f.col('TASA_PROPUESTA')))).alias('TASA_SR'),
                                f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('TASA_PROPUESTA')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col('TASA_PROPUESTA')))).alias('TASA_OF_1'),

                                f.sum(f.when(f.col('iter')==0,f.col('VAN_estimado'))).alias('VAN_LEAD'),
                                f.sum(f.when((f.col('s_van_estimado') ==0)&(f.col('iter')==0),f.col('VAN_estimado')).otherwise(f.when(f.col('van_estimado').cast('decimal(32,10)') ==f.col('m_van_estimado').cast('decimal(32,10)') ,f.col('VAN_estimado')))).alias('VAN_SR'),
                                f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('VAN_estimado')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col('VAN_estimado')))).alias('VAN_OF_1'),

                                f.sum(f.when(f.col('iter')==0,f.col('MTO_estimado'))).alias('MTO_LEAD'),
                                f.sum(f.when((f.col('s_van_estimado') ==0)&(f.col('iter')==0),f.col('MTO_estimado')).otherwise(f.when(f.col('van_estimado').cast('decimal(32,10)') ==f.col('m_van_estimado').cast('decimal(32,10)') ,f.col('MTO_estimado')))).alias('MTO_SR'),
                                f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('MTO_estimado')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col('MTO_estimado')))).alias('MTO_OF_1')
                               )

# ---------------------------------
df_seg_opt_b = df_seg_opt_caps.select(['codclavecic','llave','tasa_propuesta','iter'])\
                              .join(df_seg_opt_a.select(['llave','tasa_of_1']),
                                    on = 'llave',
                                    how = 'inner')


# NUEVA FORMA
df_seg_opt_d = df_seg_opt_b.filter(f.col('tasa_propuesta')==f.col('tasa_of_1')).distinct()
  
  
# Base datos con la Tasa Propuesta WTP
df_seg_opt_e = df_seg_opt_caps.join(df_seg_opt_d.select(['llave','tasa_propuesta']), #codclavecic
                               on = ['llave','tasa_propuesta'], #codclavecic
                               how = 'inner')


# Guardar
nombre_base_muestra = f'4_df_tasa_wtp_cef_{codmes}'
#nombre_base_muestra = f'4_df_tasa_wtp_cef_{codmes}_SHARE'
df_seg_opt_e.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}_{campana_aux}") #AGREGAR BLAZE

## Leer
nombre_base_muestra = f'4_df_tasa_wtp_cef_{codmes}'
df_seg_opt_exp = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}_{campana_aux}") #AGREGAR BLAZE

## Diferentes agrupaciones: Tasa base y Tasa final (en orden)
df_seg_opt =df_seg_opt.withColumn('RNG_MTO_V2',f.when(f.col('mtofinalofertadosol')<  5000,'1.[-;5k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')<  8000,'2.[5k;8k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 12000,'3.[8k;12k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 18000,'4.[12k;18k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 30000,'5.[18k;30k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 50000,'6.[30k;50k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 80000,'7.[50k;80k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')<120000,'8.[80k;120k>').otherwise('9.[120k;+>'))))))))
                                          )

df_seg_opt.where((f.col('iter')==0)&(f.col('tasa_propuesta')<=0.959))\
                    .groupby('RNG_MTO_V2')\
                     .agg(f.count('*').alias('clientes'),
                          f.sum('mtofinalofertadosol').alias('oferta'),
                          f.avg('tasa_propuesta').alias('tasa_cli'),
                          (f.sum(f.col('tasa_propuesta')*f.col('mtofinalofertadosol'))/f.sum('mtofinalofertadosol')).alias('tasa_pnd'),
                         f.sum('mto_estimado').alias('mto_estimado'),
                         f.sum(f.when(isnan(f.col('van_estimado')),0).otherwise(f.col('van_estimado'))).alias('van_estimado'),
                        f.avg('prob_adj_ef').alias('prob'),
                          #f.avg('EFECT1').alias('EFECT1')
                         ).orderBy(f.col('RNG_MTO_V2')).toPandas()
    
df_seg_opt_e = df_seg_opt_exp
df_seg_opt_e = df_seg_opt_e.withColumn('RNG_MTO_V2',f.when(f.col('mtofinalofertadosol')<  5000,'1.[-;5k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')<  8000,'2.[5k;8k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 12000,'3.[8k;12k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 18000,'4.[12k;18k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 30000,'5.[18k;30k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 50000,'6.[30k;50k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')< 80000,'7.[50k;80k>').otherwise(
                                                        f.when(f.col('mtofinalofertadosol')<120000,'8.[80k;120k>').otherwise('9.[120k;+>'))))))))
                                          )

df_seg_opt_e.groupby('RNG_MTO_V2')\
                     .agg(f.count('*').alias('clientes'),
                          f.sum('mtofinalofertadosol').alias('oferta'),
                          f.avg('tasa_propuesta').alias('tasa_cli_e'),
                          (f.sum(f.col('tasa_propuesta')*f.col('mtofinalofertadosol'))/f.sum('mtofinalofertadosol')).alias('tasa_pnd_e'),
                         f.sum('mto_estimado').alias('mto_estimado_e'),
                         f.sum(f.when(isnan(f.col('van_estimado')),0).otherwise(f.col('van_estimado'))).alias('van_estimado_e'),
                        f.avg('prob_adj_ef').alias('prob_e'),
                         ).orderBy(f.col('RNG_MTO_V2')).toPandas()
  

###gráfico
df_seg_opt_iter0 = df_seg_opt.select('llave','tasa_propuesta').where(f.col('iter')==0).withColumnRenamed('tasa_propuesta', 'tasa_base') #codclavecic

df_seg_opt_final = df_seg_opt_e.join(df_seg_opt_iter0.select(['llave','tasa_base']), #codclavecic
                               on = ['llave'], #codclavecic
                               how = 'inner')                               
                               
df_seg_opt_final = df_seg_opt_final.withColumn('dif_tasa', f.col('tasa_propuesta')-f.col('tasa_base'))                            
df_tasas_graph= df_seg_opt_final.toPandas()  

df_tasas_graph = df_tasas_graph.astype({"dif_tasa": float})
import matplotlib.pyplot as plt   
plt.hist(df_tasas_graph.dif_tasa, bins=60, alpha=1, edgecolor = 'black',  linewidth=1)
plt.show()

spark.stop()