# Analizador NLP de Propuestas - Reforma Universitaria (MECUN)

Con el fin de realizar un proceso de reforma integral a los estatutos de la Universidad Nacional de Colombia, se pidió a la comunidad universitaria que se reúniera, deliberara y finalmente enviara propuestas respecto a 5 ejes estratégicos que componen el gobierno universitario. 

De estos lugares deliberativos fueron enviadas cientos de propuestas, por lo cual era indispensable crear una herramienta que permitiera a los encargados de crear dicha propuesta de reforma, acceder de manera organizada y ágil a cada una de las cientos de propuestas que fueron enviadas, reduciendo así el tiempo de procesamiento de datos de meses a una semana.

## Ficha Técnica

* **Lenguaje:** Python 3.x
* **Modelos de Lenguaje (LLMs):** Google Gemini 2.5 Flash (Clasificación iterativa) y Gemini 2.5 Pro (Razonamiento cualitativo).
* **Método Estadístico:** Muestreo probabilístico mediante Fórmula de Slovin (Margen de error: 10%).
* **Técnicas NLP:** Zero-shot classification, Clustering semántico, Extracción de información cualitativa.
* **Librerías Core:** `pandas`, `google-generativeai`, `python-docx`.
* **Entrada de datos:** Archivos tabulares (`.csv`, `.xlsx`) con datos en texto libre.

## Metodología y Arquitectura del Pipeline

El script ejecuta un pipeline secuencial de procesamiento de lenguaje natural estructurado en cuatro fases:

### 1. Preprocesamiento y Muestreo Dinámico
El sistema realiza la ingesta de datos, filtra valores nulos y segmenta la base de datos por el eje temático seleccionado. Para evitar sesgos en la categorización, se calcula un tamaño de muestra representativo ($n$) aplicando la fórmula de Slovin. Sobre esta muestra, un LLM realiza una lectura exploratoria para identificar categorías emergentes que no estaban contempladas en el diccionario base (Zero-shot classification), aplicando reglas estrictas de control de duplicidad.

### 2. Clasificación Estricta y Medición de Confianza
La totalidad de las propuestas del eje poblacional son sometidas a un proceso de clasificación individual. El modelo asigna cada registro a una única categoría (predefinida o emergente) y retorna un valor numérico (0-100) que representa el nivel de confianza de la clasificación. Los valores por debajo del umbral (<70%) son etiquetados para revisión manual.

### 3. Agrupamiento Semántico (Clustering)
Dentro de cada categoría, el algoritmo agrupa las propuestas que comparten mecanismos o ideas centrales idénticas, reduciendo la redundancia de lectura. El resultado es la síntesis de una viñeta conceptual respaldada por los IDs de las propuestas ciudadanas originales que la sustentan.

### 4. Análisis de Consensos y Disensos
Fase final de evaluación cualitativa. Un modelo de alto razonamiento (`gemini-pro`) audita las propuestas agrupadas para identificar patrones de acuerdo político (consensos mayoritarios), variables de negociación (zonas grises) y conflictos irreconciliables (disensos).

## Estructura de Salida (Entregables)

La ejecución del código exporta automáticamente tres documentos en el directorio raíz:

1. `Reporte_Paginado_[Eje].docx`: Documento paginado y estructurado por categorías y subgrupos semánticos, con casillas de verificación de viabilidad listas para trabajo de campo.
2. `Reporte_Tabular_[Eje].csv`: Base de datos procesada con variables categóricas asignadas, métricas de confianza y síntesis de ideas, delimitado por `;` para importación segura.
3. `Mapa_Semaforo_[Eje].docx`: Informe analítico de tensiones normativas extraídas directamente de los textos originales.
