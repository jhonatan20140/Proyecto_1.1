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
campana_aux_save = campana_aux + '_piloto_BA'

# Obtener el usuario en Datalake 
hdfs_user_name = spark.sparkContext.sparkUser()
hdfs_user_name = hdfs_user_name.replace('b','').replace('B','')


# Definir la ruta de Hue desde donde se extraerán los datos
path_hdfs_sandbox_data_root = f"/prod/bcp/edv/pric/t24946/202008_WTP_CEF/implementation_bkp/data"

# Definir la ruta de CDSW
path_cdsw_data = f"implementation_bkp/data"

#******************************************************************************************
#CARGA DE CREDENCIALES ORACLE
#******************************************************************************************          

# CARGA DE CREDENCIALES ORACLE
with open('credentials_pedro.json', 'r') as f:
    credentials = json.load(f)

x = spark.sparkContext._jsc.hadoopConfiguration()
a = x.getPassword(credentials['pass_file'])

password = ""
for i in range(a.__len__()):
   password = password + str(a.__getitem__(i))
    

# ========================================================================================
# Meses
# ========================================================================================
indicators_reported = ['ROA','AVG_BALANCE','ROA_SALDO']
indicator_target = 'ROA_SALDO'
codmes = 202401

# =================================
# Extrayento las tasa leads (FUE PARA EL EJERCICIO) 
#
#sqlQuery=\
#"(SELECT CODCLAVECIC, PCTTASAOFERTADA AS TASA_LEAD_NOV \
#FROM ( \
#     SELECT A.CODCLAVECIC, A.PCTTASAOFERTADA, A.MTOFINALOFERTADOSOL, \
#             ROW_NUMBER() OVER(PARTITION BY A.CODCLAVECIC ORDER BY A.MTOFINALOFERTADOSOL DESC) ORD \
#      FROM USRDB_CRM_SOLTEC.ME_OFERTAGC_VIEW A \
#      WHERE A.CODLOTEOFERTA = 202311 \
#            AND A.TIPVENTA=6 AND A.TIPOFERTA=3 ) T1 \
#WHERE ORD = 1)"
#
#df_tasa_leads = spark.read \
#          .format("jdbc") \
#          .option("url", 'jdbc:oracle:thin:@130.1.22.91:1521:BCPDW3') \
#          .option("dbtable", sqlQuery) \
#          .option("user", credentials["username"]) \
#          .option("password", password) \
#          .load()
            

# ========================================================================================
## Leer
# ========================================================================================
#nombre_base_muestra = f'df_seg_opt_mx_efectividad_{codmes}'
#nombre_base_muestra = f'df_seg_opt_mx_base_{codmes}'
nombre_base_muestra = f'df_seg_opt_mx_share_{codmes}_{campana_aux_save}'
df_seg_opt_mx_efectividad = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/4_Iteraciones/{nombre_base_muestra}") #AGREGAR BLAZE

# ========================================================================================
# Caps de Iteraciones
# ========================================================================================
import pyspark.sql.functions as f

df_seg_opt= df_seg_opt_mx_efectividad
df_seg_opt= df_seg_opt.filter(f.col('numscoreriesgo')>=360) 

# Indicadores Estimados
for ind in indicators_reported:
  df_seg_opt = df_seg_opt.withColumn(f'{ind}_estimado',f.col(f'{ind}')*f.col('prob_adj_ef'))

# MTO estimado
df_seg_opt = df_seg_opt.withColumn('MTO_estimado',f.col('mtofinalofertadosol')*f.col('prob_adj_ef'))

nombre_base_muestra = f'TasasProducto_{codmes}_{campana_aux}_iniciativa'
df_tasaRoaMin = spark.read\
                     .format('csv')\
                     .option("header",True)\
                     .load(f"{path_hdfs_sandbox_data_root}/in/2_CLVs/{nombre_base_muestra}.csv")

df_tasaRoaMin = df_tasaRoaMin.select('llave','TasaPropuestaMin')

# Quitando las Tasas con ROA < 0.5%
df_seg_opt_0 = df_seg_opt.join(df_tasaRoaMin, on ='llave', how='left')
df_seg_opt_0 = df_seg_opt_0.filter(f.col('tasa_propuesta')>=f.col('TasaPropuestaMin'))
      
# Guardar
nombre_base_muestra = f'1_dta_iter_opt_caps_{campana_aux_save}'
df_seg_opt_0.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE

# ========================================================================================
# Datos Agrupados a la Tasa Lead
# ========================================================================================
df_seg_opt = df_seg_opt_0

df_res_opt = \
df_seg_opt.where((f.col('iter')==0)&(f.col('tasa_propuesta')<=0.959))\
          .agg(f.count('*').alias('clientes'),
               f.sum('mtofinalofertadosol').alias('oferta'),
               f.avg('prob_adj_ef').alias('prob'),
               f.avg('tasa_propuesta').alias('tasa_cli'),
               (f.sum(f.col('tasa_propuesta')*f.col('mtofinalofertadosol'))/f.sum('mtofinalofertadosol')).alias('tasa_pnd'),
               f.sum('mto_estimado').alias('mto_estimado')
#               ,f.sum(f.when(isnan(f.col('van_estimado')),0).otherwise(f.col('van_estimado'))).alias('van_estimado')
               ,f.sum(f.when(isnan(f.col('ROA_estimado')),0).otherwise(f.col('ROA_estimado'))).alias('ROA_estimado')
               ,f.sum(f.when(isnan(f.col('AVG_BALANCE_estimado')),0).otherwise(f.col('AVG_BALANCE_estimado'))).alias('AVG_BALANCE_estimado')
               ,f.sum(f.when(isnan(f.col('ROA_SALDO_estimado')),0).otherwise(f.col('ROA_SALDO_estimado'))).alias('ROA_SALDO_estimado')
#               ,f.sum(f.when(isnan(f.col('SPREAD_NET_estimado')),0).otherwise(f.col('SPREAD_NET_estimado'))).alias('SPREAD_NET_estimado')
#               ,f.sum(f.when(isnan(f.col('UN_estimado')),0).otherwise(f.col('UN_estimado'))).alias('UN_estimado')
               )


# Guardar
nombre_base_muestra = f'2_df_res_opt_{campana_aux_save}'
df_res_opt.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE

# Leer
nombre_base_muestra = f'2_df_res_opt_{campana_aux_save}'
df_res_opt = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE



# ========================================================================================
# Leer base Caps
# =========================================
nombre_base_muestra = f'1_dta_iter_opt_caps_{campana_aux_save}'
df_seg_opt_caps = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE


# ========================================================================================
#exp = 0.5
import numpy as np
  
for exp in np.arange(0.5,10,0.5):

  # ========================================================================================
  # Función Objetivo
  # ========================================================================================

  df_seg_opt_f = df_seg_opt_caps.withColumn('obj_funct_p_1', f.col(indicator_target)*pow(f.col('prob_adj_ef'),exp))
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


  # Cruce con Base Caps #codclavecic
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

  print(f"Terminó: Exponente - {exp}")


# Guardar
nombre_base_muestra = f'2_df_ResultadosOptimizaciónParquet'
df_res_pd_0.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}")

# Leer
nombre_base_muestra = f'2_df_ResultadosOptimizaciónParquet'
df_res_pd_0 = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") 

# A PANDAS   
df_res_pd = df_res_pd_0.toPandas()

# Guardar
df_res_pd.to_csv(f"{path_cdsw_data}/out/5_Optimizacion/1_Resultados_TradeOff_share_{codmes}_{campana_aux}.csv") #AGREGAR BLAZE


# ========================================================================================
# Función Objetivo
# ========================================================================================

# DIC 23: 2.5
# ENE 24: 2

df_seg_opt_f = df_seg_opt_caps.withColumn('obj_funct_p_1', f.col(f'{indicator_target}')*pow(f.col('prob_adj_ef'),2))
df_seg_opt_f = df_seg_opt_f.fillna(0)


exp_agg = [f.max(f'{ind}_estimado').alias(f'm_{ind}_estimado') for ind in indicators_reported] + \
        [f.sum(f'{ind}_estimado').alias(f's_{ind}_estimado') for ind in indicators_reported] + \
        [f.max('obj_funct_p_1').alias('m_obj_funct_p_1')] + \
        [f.sum('obj_funct_p_1').alias('s_obj_funct_p_1')]

df_sen = df_seg_opt_f.groupBy('llave') \
                     .agg(*exp_agg)

# JOIN: FUNCIONES OBJETIVO
# -----------------------------------------------------------------------------------------------------
df_seg_opt_f = df_seg_opt_f.join(df_sen, on = 'llave', how = 'left') #codclavecic


# -----------------------------------------------------------------------
# Cruce y Selección de Tasas
# -----------------------------------------------------------------------

exp_agg = \
[f.sum(f.when(f.col('iter')==0,f.col('mtofinalofertadosol'))).alias('OFERTA')] +\
[f.sum(f.when(f.col('iter')==0,f.col('prob_adj_ef'))).alias('PROB_LEAD')] +\
[f.sum(f.when(f.col('iter')==0,f.col('ITER'))).alias('ITER_LEAD')] +\
[f.sum(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col('ITER')).otherwise(f.when(f.col(f'{indicator_target}_estimado') ==f.col(f'm_{indicator_target}_estimado') ,f.col('ITER')))).alias('ITER_SR')] +\
[f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('ITER')).otherwise(f.when(f.col('obj_funct_p_1')==f.col('m_obj_funct_p_1'),f.col('ITER')))).alias('ITER_OF_1')] +\
[f.sum(f.when(f.col('iter')==0,f.col('TASA_PROPUESTA'))).alias('TASA_LEAD')] +\
[f.sum(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col('TASA_PROPUESTA')).otherwise(f.when(f.col(f'{indicator_target}_estimado').cast('decimal(32,10)') ==f.col(f'm_{indicator_target}_estimado').cast('decimal(32,10)') ,f.col('TASA_PROPUESTA')))).alias('TASA_SR')] +\
[f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('TASA_PROPUESTA')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col('TASA_PROPUESTA')))).alias('TASA_OF_1')] +\
[f.sum(f.when(f.col('iter')==0,f.col('MTO_estimado'))).alias('MTO_LEAD')] +\
[f.sum(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col('MTO_estimado')).otherwise(f.when(f.col(f'{indicator_target}_estimado').cast('decimal(32,10)') ==f.col(f'm_{indicator_target}_estimado').cast('decimal(32,10)') ,f.col('MTO_estimado')))).alias('MTO_SR')] +\
[f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col('MTO_estimado')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col('MTO_estimado')))).alias('MTO_OF_1')] +\
[f.sum(f.when(f.col('iter')==0,f.col(f'{ind}_estimado'))).alias(f'{ind}_LEAD') for ind in indicators_reported]  +\
[f.sum(f.when((f.col(f's_{indicator_target}_estimado') ==0)&(f.col('iter')==0),f.col(f'{ind}_estimado')).otherwise(f.when(f.col(f'{indicator_target}_estimado').cast('decimal(32,10)') ==f.col(f'm_{indicator_target}_estimado').cast('decimal(32,10)') ,f.col(f'{ind}_estimado')))).alias(f'{ind}_SR') for ind in indicators_reported]  +\
[f.sum(f.when((f.col('s_obj_funct_p_1')==0)&(f.col('iter')==0),f.col(f'{ind}_estimado')).otherwise(f.when(f.col('obj_funct_p_1').cast('decimal(32,10)')==f.col('m_obj_funct_p_1').cast('decimal(32,10)'),f.col(f'{ind}_estimado')))).alias(f'{ind}_OF_1') for ind in indicators_reported]

df_seg_opt_a = df_seg_opt_f.groupBy('llave')\
                           .agg(*exp_agg)


# -----------------------------------------------------------------------------------------------------
# Cruce con Base Caps
# -----------------------------------------------------------------------------------------------------
df_seg_opt_b = df_seg_opt_caps.select(['codclavecic','llave','tasa_propuesta'])\
                              .join(df_seg_opt_a.select(['llave','tasa_of_1']),
                                    on = 'llave',
                                    how = 'inner')

# Crear Campo Diferencia más cercana a la tasa con caps
df_seg_opt_b = df_seg_opt_b.withColumn('dif_abs',abs(f.col('tasa_propuesta')-f.col('tasa_of_1')).cast('decimal(32,10)'))

# NUEVA FORMA
df_seg_opt_d = df_seg_opt_b.filter(f.col('tasa_propuesta')==f.col('tasa_of_1')).distinct()
  
  
# Base datos con la Tasa Propuesta WTP
df_seg_opt_e = df_seg_opt_caps.join(df_seg_opt_d.select(['llave','tasa_propuesta']), #codclavecic
                               on = ['llave','tasa_propuesta'], #codclavecic
                               how = 'inner')



        

# Guardar
nombre_base_muestra = f'4_df_tasa_wtp_cef_{codmes}_{campana_aux_save}'
df_seg_opt_e.write.format('parquet').mode('overwrite').save(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE

## Leer
nombre_base_muestra = f'4_df_tasa_wtp_cef_{codmes}_{campana_aux_save}'
df_seg_opt_e = spark.read.format('parquet').load(f"{path_hdfs_sandbox_data_root}/out/5_Optimizacion/{nombre_base_muestra}") #AGREGAR BLAZE


spark.stop()

