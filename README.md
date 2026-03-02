# JMAexcepciones

Extractor automatizado de dictámenes que autorizan **excepciones al Plan Regulador** en las minutas publicadas por la Junta Municipal de Asunción.

El programa:

1. Descarga el índice de sesiones desde el sitio oficial.
2. Detecta enlaces a minutas en PDF (Google Drive).
3. Descarga cada PDF.
4. Extrae el texto.
5. Identifica dictámenes que autorizan excepciones edilicias.
6. Exporta los resultados a CSV y JSON.

---

## Requisitos

* Python 3.9+
* Dependencias:

```bash
pip install requests beautifulsoup4 pdfplumber
```

> Si `pdfplumber` no está disponible, el script intenta usar `pypdf` como alternativa.

---

## Uso básico

```bash
python jma_excepciones.py
```

Este comando ejecuta el pipeline completo:

* Descarga las minutas publicadas.
* Extrae excepciones edilicias.
* Genera los archivos:

  * `excepciones_plan_regulador.csv`
  * `excepciones_plan_regulador.json`

---

## Opciones disponibles

El programa acepta los siguientes flags:

### `--debug-links`

Imprime todos los enlaces detectados en el índice antes de procesarlos.

```bash
python jma_excepciones.py --debug-links
```

Útil para:

* Verificar qué PDFs fueron encontrados.
* Diagnosticar cambios en la estructura del sitio.

---

### `--html FILE`

Usa un archivo HTML local en lugar de hacer la request HTTP al sitio oficial.

```bash
python jma_excepciones.py --html sesiones.html
```

Útil para:

* Trabajar offline.
* Evitar múltiples requests al servidor.
* Depurar cambios en el parser sin depender del sitio en vivo.

---

### `--dump-text`

Guarda el texto extraído de cada PDF como archivo `.txt`.

```bash
python jma_excepciones.py --dump-text
```

Los textos se guardan en:

```
minutas_pdf/<id>.txt
```

Útil para:

* Depurar errores de extracción.
* Ajustar patrones regex.
* Auditar manualmente los dictámenes detectados.

---

## Flujo interno (pipeline)

1. **Obtención del índice**
   Scraping del calendario de sesiones.

2. **Descarga de PDFs**

   * Maneja redirecciones de Google Drive.
   * Reintentos automáticos ante errores HTTP.

3. **Extracción de texto**

   * `pdfplumber` (preferido)
   * `pypdf` (fallback)

4. **Detección de excepciones**
   Se buscan bloques que:

   * Contengan palabras clave como “régimen de excepcionalidad”.
   * Incluyan autorización de anteproyecto o proyecto de construcción.

5. **Extracción estructurada de campos**

   * Fecha de sesión
   * Expediente
   * Solicitante
   * Niveles
   * Destino
   * Ubicación
   * Cuentas corrientes catastrales
   * Ordenanza de referencia
   * URL fuente

6. **Exportación**

   * CSV plano (análisis en Excel / R / Pandas)
   * JSON estructurado (uso programático)

---

## Archivos generados

### `excepciones_plan_regulador.csv`

Una fila por excepción detectada.

### `excepciones_plan_regulador.json`

Lista de objetos con todos los campos estructurados.

---

## Combinación de opciones

Las opciones pueden combinarse:

```bash
python jma_excepciones.py --html sesiones.html --dump-text --debug-links
```

---

## Notas técnicas

* El script incluye reintentos automáticos ante errores HTTP (429, 500, 502, 503, 504).
* Introduce pausas entre descargas para evitar sobrecargar el servidor.
* Evita descargar el mismo PDF más de una vez.
* Filtra falsos positivos (por ejemplo, minutas no relacionadas con construcción).

