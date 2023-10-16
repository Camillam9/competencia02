# para correr el Google Cloud
#   8 vCPU
#  64 GB memoria RAM


# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

require("data.table")
require("lightgbm")


# defino los parametros de la corrida, en una lista, la variable global  PARAM
#  muy pronto esto se leera desde un archivo formato .yaml
PARAM <- list()
PARAM$experimento <- "KA8241"

PARAM$input$dataset <- "./datasets/competencia_02_julia.csv.gz"

# meses donde se entrena el modelo
PARAM$input$training <- c(201905, 201906, 201907, 201908, 201909, 201910, 201911, 201912, 202011, 202012, 202101, 202102, 202103, 202104, 202105)
PARAM$input$future <- c(202107) # meses donde se aplica el modelo

PARAM$finalmodel$semilla <- 100103

# hiperparametros intencionalmente NO optimos
PARAM$finalmodel$optim$num_iterations <- 730
PARAM$finalmodel$optim$learning_rate <- 0.0323601846272594
PARAM$finalmodel$optim$feature_fraction <- 0.909773795582897
PARAM$finalmodel$optim$min_data_in_leaf <- 4637
PARAM$finalmodel$optim$num_leaves <- 667


# Hiperparametros FIJOS de  lightgbm
PARAM$finalmodel$lgb_basicos <- list(
  boosting = "gbdt", # puede ir  dart  , ni pruebe random_forest
  objective = "binary",
  metric = "custom",
  first_metric_only = TRUE,
  boost_from_average = TRUE,
  feature_pre_filter = FALSE,
  force_row_wise = TRUE, # para reducir warnings
  verbosity = -100,
  max_depth = -1L, # -1 significa no limitar,  por ahora lo dejo fijo
  min_gain_to_split = 0.0, # min_gain_to_split >= 0.0
  min_sum_hessian_in_leaf = 0.001, #  min_sum_hessian_in_leaf >= 0.0
  lambda_l1 = 0.0, # lambda_l1 >= 0.0
  lambda_l2 = 0.0, # lambda_l2 >= 0.0
  max_bin = 31L, # lo debo dejar fijo, no participa de la BO
  
  bagging_fraction = 1.0, # 0.0 < bagging_fraction <= 1.0
  pos_bagging_fraction = 1.0, # 0.0 < pos_bagging_fraction <= 1.0
  neg_bagging_fraction = 1.0, # 0.0 < neg_bagging_fraction <= 1.0
  is_unbalance = FALSE, #
  scale_pos_weight = 1.0, # scale_pos_weight > 0.0
  
  drop_rate = 0.1, # 0.0 < neg_bagging_fraction <= 1.0
  max_drop = 50, # <=0 means no limit
  skip_drop = 0.5, # 0.0 <= skip_drop <= 1.0
  
  extra_trees = TRUE, # Magic Sauce
  
  seed = PARAM$finalmodel$semilla
)


#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Aqui empieza el programa
setwd("~/buckets/b1")

# cargo el dataset donde voy a entrenar
dataset <- fread(PARAM$input$dataset, stringsAsFactors = TRUE)
#-------------------------------------------------------------------------------
#Mrentabilidad 201905 201920 
dataset[foto_mes %in% c(201905, 201910), mrentabilidad := (shift(mrentabilidad, type = "lag") + shift(mrentabilidad, type = "lead")) / 2, by = numero_de_cliente]
#Mrentabilidad_annual 201905	201910 
dataset[foto_mes %in% c(201905, 201910), mrentabilidad_annual := (shift(mrentabilidad_annual, type = "lag") + shift(mrentabilidad_annual, type = "lead")) / 2, by = numero_de_cliente]
#mcomisiones 201905	201910 
dataset[foto_mes %in% c(201905, 201910), mcomisiones := (shift(mcomisiones, type = "lag") + shift(mcomisiones, type = "lead")) / 2, by = numero_de_cliente]
#mactivos_margen 201905	201910 
dataset[foto_mes %in% c(201905, 201910), mactivos_margen := (shift(mactivos_margen, type = "lag") + shift(mactivos_margen, type = "lead")) / 2, by = numero_de_cliente]
#mpasivos_margen 201905	201910 
dataset[foto_mes %in% c(201905, 201910), mpasivos_margen := (shift(mpasivos_margen, type = "lag") + shift(mpasivos_margen, type = "lead")) / 2, by = numero_de_cliente]
#cpayroll2_trx 201901 
dataset[foto_mes %in% c(201901), cpayroll2_trx := (shift(cpayroll2_trx, type = "lag") + shift(cpayroll2_trx, type = "lead")) / 2, by = numero_de_cliente]
#ctarjeta_visa_debitos_automaticos 201904
dataset[foto_mes %in% c(201904), ctarjeta_visa_debitos_automaticos := (shift(ctarjeta_visa_debitos_automaticos, type = "lag") + shift(ctarjeta_visa_debitos_automaticos, type = "lead")) / 2, by = numero_de_cliente]
#mtarjeta_visa_debitos_automaticos 201904
dataset[foto_mes %in% c(201904), mttarjeta_visa_debitos_automaticos := (shift(mttarjeta_visa_debitos_automaticos, type = "lag") + shift(mttarjeta_visa_debitos_automaticos, type = "lead")) / 2, by = numero_de_cliente]
#ccajero_propios_descuentos 201910 202002  202009 202010 202102
dataset[foto_mes %in% c(201910, 202002,  202009, 202010, 202102), ccajeros_propios_descuentos := (shift(ccajeros_propios_descuentos, type = "lag") + shift(ccajeros_propios_descuentos, type = "lead")) / 2, by = numero_de_cliente]
#mcajero_propios_descuentos 201910 202002 202009 202010 202102
dataset[foto_mes %in% c(201910, 202002, 202009, 202010, 202102), mcajeros_propios_descuentos := (shift(mcajeros_propios_descuentos, type = "lag") + shift(mcajeros_propios_descuentos, type = "lead")) / 2, by = numero_de_cliente]
#ctarjeta_visa_descuentos 201910 202002 202009 202010 202102
dataset[foto_mes %in% c(201910, 202002, 202009, 202010, 202102), ctarjeta_visa_descuentos := (shift(ctarjeta_visa_descuentos, type = "lag") + shift(ctarjeta_visa_descuentos, type = "lead")) / 2, by = numero_de_cliente]
#mtarjeta_visa_descuentos 201910 202002  202009 202010 202102
dataset[foto_mes %in% c(201910, 202002, 202009, 202010, 202102), mtarjeta_visa_descuentos := (shift(mtarjeta_visa_descuentos, type = "lag") + shift(mtarjeta_visa_descuentos, type = "lead")) / 2, by = numero_de_cliente]
#ctarjeta_master_descuentos 201910 202002  202009 202010 202102
dataset[foto_mes %in% c(201910, 202002, 202009, 202010, 202102), ctarjeta_master_descuentos := (shift(ctarjeta_master_descuentos, type = "lag") + shift(ctarjeta_master_descuentos, type = "lead")) / 2, by = numero_de_cliente]
#mtarjeta_master_descuentos 201910 202002  202009 202010 202102
dataset[foto_mes %in% c(201910, 202002, 202009, 202010, 202102), mtarjeta_master_descuentos := (shift(mtarjeta_master_descuentos, type = "lag") + shift(mtarjeta_master_descuentos, type = "lead")) / 2, by = numero_de_cliente]
#ccomisiones_otras 201905 201910
dataset[foto_mes %in% c(201905, 201910), ccomisiones_otras := (shift(ccomisiones_otras, type = "lag") + shift(ccomisiones_otras, type = "lead")) / 2, by = numero_de_cliente]
#mcomisiones_otras 201905 201910 
dataset[foto_mes %in% c(201905, 201910), mcomisiones_otras := (shift(mcomisiones_otras, type = "lag") + shift(mcomisiones_otras, type = "lead")) / 2, by = numero_de_cliente]
#ctransferencias_recibidas (primeros 4 meses) 201901 201902 201903 201904
dataset[foto_mes %in% c(201901, 201902, 201903, 201904), ctransferencias_recibidas := ctransferencias_recibidas[foto_mes == 201905], by = numero_de_cliente]
#mtransferencias_recibidas (primeros 4 meses) 201901 201902 201903 201904
dataset[foto_mes %in% c(201901, 201902, 201903, 201904), mtransferencias_recibidas := mtransferencias_recibidas[foto_mes == 201905], by = numero_de_cliente]
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
library(dplyr)

lag_cols <- colnames(dataset)[!(colnames(dataset) %in% c("numero_de_cliente", "foto_mes", "clase_ternaria"))]

dataset <- dataset %>%
  arrange(numero_de_cliente, foto_mes) %>%
  group_by(numero_de_cliente) %>%
  mutate(across(all_of(lag_cols), list(Lag1 = ~lag(.x, 1), Lag2 = ~lag(.x, 2), Lag3 = ~lag(.x, 3)), Lag4 = ~lag(.x, 4), Lag5 = ~lag(.x, 5), Lag6 = ~lag(.x, 6) .names="lagged_{.col}_Lag{.fn}"))


dataset <- as.data.table(dataset)
#--------------------------------------

# Catastrophe Analysis  -------------------------------------------------------
# deben ir cosas de este estilo
#   dataset[foto_mes == 202006, active_quarter := NA]

# Data Drifting
# por ahora, no hago nada


# Feature Engineering Historico  ----------------------------------------------
#   aqui deben calcularse los  lags y  lag_delta
#   Sin lags no hay paraiso ! corta la bocha
#   https://rdrr.io/cran/data.table/man/shift.html


#--------------------------------------

# paso la clase a binaria que tome valores {0,1}  enteros
# set trabaja con la clase  POS = { BAJA+1, BAJA+2 }
# esta estrategia es MUY importante
dataset[, clase01 := ifelse(clase_ternaria %in% c("BAJA+2", "BAJA+1"), 1L, 0L)]

#--------------------------------------

# los campos que se van a utilizar
campos_buenos <- setdiff(colnames(dataset), c("clase_ternaria", "clase01"))

#--------------------------------------


# establezco donde entreno
dataset[, train := 0L]
dataset[foto_mes %in% PARAM$input$training, train := 1L]

#--------------------------------------
# creo las carpetas donde van los resultados
# creo la carpeta donde va el experimento
dir.create("./exp/", showWarnings = FALSE)
dir.create(paste0("./exp/", PARAM$experimento, "/"), showWarnings = FALSE)

# Establezco el Working Directory DEL EXPERIMENTO
setwd(paste0("./exp/", PARAM$experimento, "/"))



# dejo los datos en el formato que necesita LightGBM
dtrain <- lgb.Dataset(
  data = data.matrix(dataset[train == 1L, campos_buenos, with = FALSE]),
  label = dataset[train == 1L, clase01]
)


# genero el modelo
param_completo <- c(PARAM$finalmodel$lgb_basicos,
                    PARAM$finalmodel$optim)

modelo <- lgb.train(
  data = dtrain,
  param = param_completo,
)

#--------------------------------------
# ahora imprimo la importancia de variables
tb_importancia <- as.data.table(lgb.importance(modelo))
archivo_importancia <- "impo.txt"

fwrite(tb_importancia,
       file = archivo_importancia,
       sep = "\t"
)

#--------------------------------------


# aplico el modelo a los datos sin clase
dapply <- dataset[foto_mes == PARAM$input$future]

# aplico el modelo a los datos nuevos
prediccion <- predict(
  modelo,
  data.matrix(dapply[, campos_buenos, with = FALSE])
)

# genero la tabla de entrega
tb_entrega <- dapply[, list(numero_de_cliente, foto_mes)]
tb_entrega[, prob := prediccion]

# grabo las probabilidad del modelo
fwrite(tb_entrega,
       file = "prediccion.txt",
       sep = "\t"
)

# ordeno por probabilidad descendente
setorder(tb_entrega, -prob)


# genero archivos con los  "envios" mejores
# deben subirse "inteligentemente" a Kaggle para no malgastar submits
# si la palabra inteligentemente no le significa nada aun
# suba TODOS los archivos a Kaggle
# espera a la siguiente clase sincronica en donde el tema sera explicado

cortes <- seq(8000, 15000, by = 500)
for (envios in cortes) {
  tb_entrega[, Predicted := 0L]
  tb_entrega[1:envios, Predicted := 1L]
  
  fwrite(tb_entrega[, list(numero_de_cliente, Predicted)],
         file = paste0(PARAM$experimento, "_", envios, ".csv"),
         sep = ","
  )
}

cat("\n\nLa generacion de los archivos para Kaggle ha terminado\n")
