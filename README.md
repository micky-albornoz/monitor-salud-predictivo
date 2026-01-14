# 🏥 Monitor Federal de Salud: Predicción de Demanda con IA
### Autor: [Miguel Albornoz](https://github.com/micky-fearnot)

> **Sistema de inteligencia epidemiológica para la optimización de recursos sanitarios en Argentina.**

[![Status](https://img.shields.io/badge/Status-Active-success)]()
[![Tech](https://img.shields.io/badge/Stack-R%20%7C%20Prophet%20%7C%20Quarto-blue)]()

### 🎯 Resumen Ejecutivo
Este proyecto aborda la problemática de la **saturación estacional** en hospitales públicos. Utilizando datos abiertos del Ministerio de Salud, desarrollé un pipeline de Machine Learning capaz de predecir picos de **Bronquiolitis, Influenza y Neumonía** con 52 semanas de antelación.

El objetivo es permitir a la gestión sanitaria pasar de un modelo reactivo a uno **prospectivo**, planificando la contratación de personal y stock de insumos antes de que llegue el invierno.

---

### 🛠️ Stack Tecnológico

El proyecto implementa una arquitectura moderna de Data Science:

* **Ingeniería de Datos (ETL):**
    * **Lenguaje:** R (`tidyverse`, `janitor`).
    * **Procesamiento:** Normalización de *Semanas Epidemiológicas* (ISO-8601) y unificación de reportes provinciales masivos.
    * **Big Data:** Gestión de datasets voluminosos mediante **Git LFS**.
* **Machine Learning & Forecasting:**
    * **Modelo:** **Facebook Prophet** (Modelos Aditivos Generalizados).
    * **Estrategia:** Entrenamiento iterativo de múltiples modelos independientes (uno por cada combinación *Provincia-Patología*).
    * **Features:** Detección automática de estacionalidad anual y puntos de cambio de tendencia.
* **Visualización & Despliegue:**
    * **Framework:** **Quarto** (Next-gen RMarkdown).
    * **Interactividad:** **Observable JS (OJS)** para filtrado de datos client-side (sin servidor).
    * **Deploy:** GitHub Pages (Serverless architecture).

---

### 📊 Estructura del Proyecto

```text
├── data/
│   ├── raw/             # Datos crudos (Vigilancia Epidemiológica) - Gestionado con LFS
│   └── processed/       # Datos limpios y pronósticos (.rds)
├── scripts/
│   ├── 01_etl.R         # Pipeline de limpieza y transformación multidimensional
│   └── 02_forecast.R    # Generación masiva de modelos Prophet
├── dashboard.qmd        # Código fuente del Dashboard Interactivo
└── _quarto.yml          # Configuración de publicación
```
---
## 📊 Vistas previas de los dashboards

**Dashboard 1: Tablero de Comando**
![Dashboard 1 Tablero de Comando](https://github.com/micky-fearnot/monitor-salud-predictivo/blob/main/images/imagen-tablero-comando.png)

**Dashboard 2: Análisis Comparativo**
![Dashboard 2 Análisis Comparativo](https://github.com/micky-fearnot/monitor-salud-predictivo/blob/main/images/imagen-analisis-comparativo.png)

---

### 🚀 Cómo reproducir este análisis

1. Clonar el repositorio:

```bash
git clone [https://github.com/TU_USUARIO/monitor-salud-arg.git](https://github.com/TU_USUARIO/monitor-salud-arg.git)
```
2. Restaurar librerías: Abrir monitor-salud-arg.Rproj en RStudio e instalar dependencias: tidyverse, prophet, quarto.

3. Ejecutar Pipeline: Correr los scripts en orden numérico (01 -> 02).

4. Visualizar: Renderizar dashboard.qmd.

> **Datos Abiertos: Ministerio de Salud de la Nación Argentina.**
