library(tidyverse)
library(janitor)
library(ISOweek)

# --- CONFIGURACIÓN ---
input_dir <- "data/raw"
output_file <- "data/processed/dataset_unificado.rds"

# 1. LECTURA DE ARCHIVOS
files <- list.files(input_dir, pattern = "\\.csv$", full.names = TRUE)
if(length(files) == 0) stop("❌ Error: No hay CSVs en data/raw")

process_csv <- function(filepath) {
  # Usamos read_delim para tener control total
  read_delim(
    filepath, 
    delim = ";",                    # Forzamos el punto y coma
    locale = locale(encoding = "UTF-8"), # Manejo de tildes y eñes
    col_types = cols(.default = "c"), 
    show_col_types = FALSE
  ) %>%
    clean_names() %>%
    rename(anio = any_of(c("anio", "ano"))) %>% 
    select(any_of(c("provincia_nombre", "anio", "semanas_epidemiologicas", "evento_nombre", "cantidad_casos")))
}

print("🔄 Iniciando ETL masivo...")

# 2. TRANSFORMACIÓN
df_final <- map_df(files, process_csv) |>
  mutate(
    anio = as.numeric(anio),
    semanas_epidemiologicas = as.numeric(semanas_epidemiologicas),
    cantidad_casos = as.numeric(cantidad_casos)
  ) %>%
  filter(!is.na(cantidad_casos), cantidad_casos > 0) |>
  
  # Categorización inteligente (Regex)
  mutate(enfermedad = case_when(
    str_detect(evento_nombre, "(?i)bronquiolitis") ~ "Bronquiolitis",
    str_detect(evento_nombre, "(?i)influenza") ~ "Influenza (Gripe)",
    str_detect(evento_nombre, "(?i)neumon") ~ "Neumonía",
    TRUE ~ "Otros"
  )) %>%
  filter(enfermedad != "Otros") |>
  
  # Conversión ISO-8601 (Semana -> Fecha)
  mutate(
    iso_week_str = paste0(anio, "-W", sprintf("%02d", semanas_epidemiologicas), "-1"),
    ds = ISOweek2date(iso_week_str)
  ) |>
  
  # Agregación Final
  group_by(provincia_nombre, enfermedad, ds) |>
  summarise(y = sum(cantidad_casos), .groups = "drop") |>
  arrange(provincia_nombre, enfermedad, ds)

# 3. CARGA
write_rds(df_final, output_file)

print(paste("✅ Éxito: ETL completado."))
print(paste("Dataset procesado guardado en", output_file))
print(paste("Provincias detectadas:", length(unique(df_final$provincia_nombre))))
print(paste("Dimensiones:", nrow(df_final), "registros generados."))
