library(tidyverse)
library(prophet)

# 1. CARGA
df <- read_rds("data/processed/dataset_unificado.rds")
combinaciones <- df %>% distinct(provincia_nombre, enfermedad)
final_predictions <- data.frame()

print(paste("Entrenando modelos para", nrow(combinaciones), "series temporales..."))

# 2. BUCLE DE ENTRENAMIENTO
for(i in 1:nrow(combinaciones)) {
  prov <- combinaciones$provincia_nombre[i]
  enf <- combinaciones$enfermedad[i]
  
  # Filtro de datos específicos
  df_subset <- df |> filter(provincia_nombre == prov, enfermedad == enf)
  
  # Quality Gate: Si hay pocos datos, saltamos para no romper el modelo
  if(nrow(df_subset) < 10) next 
  
  # Prophet Core
  m <- prophet(df_subset, yearly.seasonality = TRUE, weekly.seasonality = FALSE, daily.seasonality = FALSE)
  future <- make_future_dataframe(m, periods = 52, freq = "week")
  forecast <- predict(m, future)
  
  # Estandarización de resultados
  forecast_clean <- forecast %>%
    select(ds, yhat, yhat_lower, yhat_upper) %>%
    # Unimos por fecha para que los históricos tengan 'y' y las proyecciones tengan NA
    left_join(df_subset %>% select(ds, y), by = "ds") %>% 
    mutate(
      yhat = pmax(yhat, 0),
      yhat_lower = pmax(yhat_lower, 0),
      yhat_upper = pmax(yhat_upper, 0),
      provincia_nombre = prov,
      enfermedad = enf,
      type = ifelse(ds > max(df_subset$ds, na.rm = TRUE), "Proyección", "Histórico")
    )
  
  final_predictions <- bind_rows(final_predictions, forecast_clean)
}

# 3. GUARDADO FINAL
write_rds(final_predictions, "data/processed/pronostico_final.rds")
print("✅ Modelos generados exitosamente.")

