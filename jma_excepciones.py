#!/usr/bin/env python3
"""
jma_excepciones.py
==================
Descarga todas las minutas publicadas por la Junta Municipal de Asunción (JMA)
y extrae los dictámenes que autorizan excepciones al Plan Regulador.

Requisitos:
    pip install requests beautifulsoup4 pdfplumber

Uso:
    python jma_excepciones.py

Salida:
    excepciones_plan_regulador.csv  – una fila por excepción encontrada
    excepciones_plan_regulador.json – mismos datos en JSON
"""

import csv
import json
import re
import time
import os
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ── Intenta importar pdfplumber; si falla, usa pypdf como fallback ──────────
try:
    import pdfplumber
    PDF_BACKEND = "pdfplumber"
except ImportError:
    try:
        from pypdf import PdfReader
        PDF_BACKEND = "pypdf"
    except ImportError:
        print("ERROR: Instalá pdfplumber o pypdf:")
        print("  pip install pdfplumber")
        sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────
BASE_URL        = "https://jma.gov.py/calendario-de-sesiones/"
DOWNLOAD_DIR    = Path("minutas_pdf")
OUTPUT_CSV      = "excepciones_plan_regulador.csv"
OUTPUT_JSON     = "excepciones_plan_regulador.json"
REQUEST_TIMEOUT = 30          # segundos
SLEEP_BETWEEN   = 1.5         # pausa entre descargas (ser amable con el servidor)

# Headers que imitan un navegador real (algunos servidores rechazan requests sin esto)
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PY,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def make_session() -> requests.Session:
    """Crea una sesión HTTP con reintentos automáticos y headers de navegador."""
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# Sesión global reutilizable
HTTP = make_session()

# ─────────────────────────────────────────────────────────────────────────────
# Estructura de datos
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Excepcion:
    fecha_sesion:     str  = ""   # ej. "17/12/2025"
    expediente:       str  = ""   # ej. "12.541/2025"
    solicitante:      str  = ""   # nombre del peticionario
    niveles:          str  = ""   # cantidad de pisos, ej. "30"
    destino:          str  = ""   # vivienda multifamiliar, comercio, etc.
    ubicacion:        str  = ""   # calles / dirección
    ctas_ctes:        str  = ""   # cuentas corrientes catastrales
    ordenanza_ref:    str  = ""   # ordenanza de referencia (ej. "106/23")
    texto_completo:   str  = ""   # fragmento completo del dictamen
    fuente_url:       str  = ""   # URL del PDF original

# ─────────────────────────────────────────────────────────────────────────────
# Helpers: Google Drive → URL de descarga directa
# ─────────────────────────────────────────────────────────────────────────────
def gdrive_to_direct(url: str) -> Optional[str]:
    """Convierte una URL de vista de Google Drive a URL de descarga."""
    # Formato: https://drive.google.com/file/d/{FILE_ID}/view?...
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Paso 1: Obtener todos los links de minutas publicados
# ─────────────────────────────────────────────────────────────────────────────
def obtener_links_minutas(html_local: Optional[str] = None) -> list[dict]:
    if html_local and Path(html_local).exists():
        print(f"📂 Leyendo HTML local: {html_local}")
        html = Path(html_local).read_text(encoding="utf-8", errors="ignore")
    else:
        print(f"📡 Descargando índice de sesiones: {BASE_URL}")
        resp = HTTP.get(BASE_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        html = resp.text

    soup = BeautifulSoup(html, "html.parser")

    links = []
    current_year: Optional[int] = None
    current_month: Optional[str] = None

    MESES = {
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "setiembre", "septiembre", "octubre",
        "noviembre", "diciembre",
    }

    for el in soup.descendants:
        if el.name and re.match(r"^h[1-6]$", el.name):
            texto_h = el.get_text(strip=True)
            m = re.search(r"\b(20\d\d)\b", texto_h)
            if m:
                current_year = int(m.group(1))
            m2 = re.search(r"\b(" + "|".join(MESES) + r")\b", texto_h, re.IGNORECASE)
            if m2:
                current_month = m2.group(1).capitalize()
            continue

        if el.name == "a":
            href = el.get("href", "")
            if "drive.google.com" not in href:
                continue

            direct = gdrive_to_direct(href)
            if not direct:
                continue

            fecha_ctx = ""
            if current_month and current_year:
                fecha_ctx = f"{current_month} {current_year}"
            texto_a = el.get_text(strip=True)
            m3 = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](20\d\d)", texto_a)
            if m3:
                fecha_ctx = f"{m3.group(1)}/{m3.group(2)}/{m3.group(3)}"

            links.append({
                "texto":     texto_a,
                "year":      current_year,
                "url":       href,
                "direct":    direct,
                "fecha_ctx": fecha_ctx,
            })

    print(f"   → {len(links)} PDFs encontrados")
    return links


# ─────────────────────────────────────────────────────────────────────────────
# Paso 2: Descargar PDFs
# ─────────────────────────────────────────────────────────────────────────────
def descargar_pdf(url_directa: str, destino: Path) -> bool:
    """Descarga un PDF. Maneja la redirección de confirmación de Google Drive."""
    session = HTTP
    try:
        resp = session.get(url_directa, timeout=REQUEST_TIMEOUT, stream=True)
        resp.raise_for_status()

        # Google Drive a veces devuelve una página de confirmación de virus scan
        content_type = resp.headers.get("Content-Type", "")
        if "text/html" in content_type:
            # Extraer token de confirmación
            html = resp.text
            m = re.search(r'name="confirm"\s+value="([^"]+)"', html)
            if m:
                confirm = m.group(1)
                # También buscar uuid
                uuid_m = re.search(r'name="uuid"\s+value="([^"]+)"', html)
                params = {"confirm": confirm}
                if uuid_m:
                    params["uuid"] = uuid_m.group(1)
                    
                file_id_m = re.search(r"id=([a-zA-Z0-9_-]+)", url_directa)
                if file_id_m:
                    params["id"] = file_id_m.group(1)
                
                resp2 = session.get(
                    "https://drive.usercontent.google.com/download",
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                    stream=True,
                )
                resp2.raise_for_status()
                resp = resp2
            else:
                # Podría ser un PDF pequeño servido como HTML – descartamos
                return False

        destino.parent.mkdir(parents=True, exist_ok=True)
        with open(destino, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True

    except Exception as e:
        print(f"   ⚠️  Error descargando {url_directa}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Paso 3: Extraer texto del PDF
# ─────────────────────────────────────────────────────────────────────────────
def extraer_texto_pdf(path: Path) -> str:
    """Extrae todo el texto de un PDF usando el backend disponible."""
    try:
        if PDF_BACKEND == "pdfplumber":
            with pdfplumber.open(path) as pdf:
                paginas = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        paginas.append(t)
                return "\n".join(paginas)
        else:
            reader = PdfReader(str(path))
            return "\n".join(
                page.extract_text() or "" for page in reader.pages
            )
    except Exception as e:
        print(f"   ⚠️  Error extrayendo texto de {path.name}: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Paso 4: Parsear excepciones del texto
# ─────────────────────────────────────────────────────────────────────────────

# Fragmentos clave que identifican una excepción al plan regulador
KEYWORD_PATTERNS = [
    r"régimen de excepcionalidad",
    r"regimen de excepcionalidad",
    r"excepci[oó]n al plan regulador",
    r"AUTORIZAR la aplicaci[oó]n del r[eé]gimen",
]

def encontrar_bloques_excepcion(texto: str) -> list[str]:
    """
    Divide el texto en bloques por dictamen y filtra los que contienen excepciones.

    Las minutas reales tienen esta estructura:
        * ASUNTOS DICTAMINADOS:
        1. Dictamen de la Comisión de Hacienda...
        2. Dictamen de la Comisión de Planificación...
        5. Dictamen de la Comisión de Planificación... (excepción)

    Dividimos por líneas que empiecen con un número seguido de punto y "Dictamen",
    o directamente por "Dictamen de la Comisión" para PDFs sin numeración.
    """
    # Intentar split por ítem numerado primero (formato más común en minutas)
    partes = re.split(
        r"(?=\n\s*\d+\.\s+Dictamen\b)",
        texto, flags=re.IGNORECASE
    )
    # Si no funcionó (solo 1 parte), intentar split directo por "Dictamen de la Comisión"
    if len(partes) <= 1:
        partes = re.split(
            r"(?=Dictamen de la Comisi[oó]n)",
            texto, flags=re.IGNORECASE
        )

    bloques = []
    for parte in partes:
        for pat in KEYWORD_PATTERNS:
            if re.search(pat, parte, re.IGNORECASE):
                bloques.append(parte)
                break
    return bloques


def extraer_campos(bloque: str, url_fuente: str = "") -> Excepcion:
    """Extrae campos estructurados de un bloque de texto de excepción."""
    exc = Excepcion(fuente_url=url_fuente)
    exc.texto_completo = bloque.strip()[:2000]

    # ── Expediente ───────────────────────────────────────────────────────────
    # Acepta N° 12.541/2025, Nº 4.237/24, N° 00723/24
    m = re.search(
        r"Expediente\s+N[°oº]?\s*([\d\.]+/\d{2,4})",
        bloque, re.IGNORECASE
    )
    if m:
        exc.expediente = m.group(1).strip()

    # ── Solicitante ──────────────────────────────────────────────────────────
    solicitante = None
    for pat in [
        r'presentado por(?:\s+la\s+firma\s+)?"?([^"\n]{3,80}?)"?\.\s*(?:\d|2°|Art\.|$)',
        r'caratulado a nombre de(?:\s+la\s+firma\s+)?"?([^"\n]{3,80}?)"?\.',
        r'caratulado:\s+(?:Firma\s+)?"([^"\n]{3,60})"',
        r'en representaci[oó]n de la\s+[Ff]irma\s+"?([^",\n]{3,60})"?(?:,|\.)',
        r'(?:del|de la\s+[Ff]irma)\s+"?([A-ZÁÉÍÓÚÑÜ][^",\n]{2,60}?)"?(?:,\s*(?:sobre|referente|en el))',
        r'propiedad de la\s+[Ff]irma\s+"?([^",\n]{3,60})"?(?:,|\.)',
        r'interpuesto por\s+(.+?),\s+a trav[eé]s',
        r'a nombre de(?:\s+la\s+firma)?\s+"?(.+?),\s+sobre',
        r'presentado por(?:\s+la\s+firma)?\s+(.+?),\s+sobre',
        r'a nombre de(?:\s+la\s+firma)?\s+"?([^"\n]{3,80}?)"?\.',
    ]:
        m = re.search(pat, bloque, re.IGNORECASE | re.DOTALL)
        if m:
            candidato = " ".join(m.group(1).split()).strip(' "«»,')
            if 2 < len(candidato) < 80 and "a través" not in candidato.lower() and "*Recomendación" not in candidato:
                solicitante = candidato
                break
    if solicitante:
        exc.solicitante = solicitante

    # ── Niveles ──────────────────────────────────────────────────────────────
    m = re.search(r"torre de\s+(\d+)\s*\([^)]+\)\s*niveles?", bloque, re.IGNORECASE)
    if not m:
        m = re.search(r"\b(\d+)\s*\([^)]+\)\s*niveles?\b", bloque, re.IGNORECASE)
    if not m:
        m = re.search(r"\b(\d+)\s+niveles?\b", bloque, re.IGNORECASE)
    if m:
        exc.niveles = m.group(1)

    # ── Destino ──────────────────────────────────────────────────────────────
    m = re.search(
        r"destinad[ao]\s+a\s+(.+?)"
        r"(?:,\s*(?:sito|ubicado|individualizado|implantado|en el inmueble|del Distrito|propiedad)"
        r"|(?:\n|\.\s*)(?:\*Recomendaci[oó]n|\d|2°|Art\.|$))",
        bloque, re.IGNORECASE | re.DOTALL
    )
    if not m:
        m = re.search(
            r"relaci[oó]n al proyecto de construcci[oó]n de\s+(.+?)"
            r"(?:,\s*(?:propiedad|sito|ubicado|individualizado)|\.\s)",
            bloque, re.IGNORECASE | re.DOTALL
        )
    if m:
        dest = " ".join(m.group(1).split()).strip().rstrip(",")
        if len(dest) > 120:
            dest = dest.split(",")[0].strip()
        exc.destino = dest

    # ── Ubicación ────────────────────────────────────────────────────────────
    # Intentar múltiples patrones en orden de especificidad
    ub_patrones = [
        # "sito en [la esquina de] [las calles] X, individualizado/implantado/identificado"
        (r'sito en\s+(?:la\s+esquina\s+de\s+)?'
         r'(?:las?\s+(?:calles?|[Aa]venidas?)\s+|(?:la\s+)?[Aa]venida\s+|calles?\s+|'
         r'el\s+(?:predio\s+)?ubicado\s+en\s+(?:la[s]?\s+)?(?:calles?|[Aa]venidas?)?\s*)?'
         r'(.+?)'
         r'(?:,\s*(?:individualizado|implantado|identificado|en el inmueble|del Distrito|de acuerdo|en consonancia|propiedad))'),
        # "localizado en [las avenidas] X, propiedad"
        (r'localizado en\s+(?:las?\s+[Aa]venidas?\s+|calles?\s+)?'
         r'(.+?)(?:,\s*(?:propiedad|individualizado|identificado))'),
        # "ubicado en las calles X, con una superficie / identificado"
        (r'ubicad[ao]s?\s+en\s+(?:las?\s+(?:calles?|[Aa]venidas?)\s+|(?:la\s+)?[Aa]venida\s+|calles?\s+)?'
         r'(.+?)(?:,\s*(?:con una superficie|identificado|individualizado|implantado|del Distrito))'),
    ]
    for pat in ub_patrones:
        m = re.search(pat, bloque, re.IGNORECASE | re.DOTALL)
        if m:
            exc.ubicacion = " ".join(m.group(1).split()).strip().rstrip(",")
            break

    # ── Cuentas corrientes catastrales ───────────────────────────────────────
    # Patrón unificado: acepta Ctas/Cta, separadores coma/punto y coma/y, con o sin "N°"
    ctas_pat = (
        r'[Cc](?:uenta[s]?\s+[Cc]orrientes?\s+[Cc]atastrales?'
        r'|tas?\.?\s*[Cc]tes?\.?\s*[Cc]trales?\.?'
        r'|ta\.?\s*[Cc]te\.?\s*[Cc]tral\.?)'
        r'\s*N[°oºros\.\s]*'
        r'([\d\-/]+(?:[\s;,y]+[\d\-/]+)*)'
    )
    matches_ctas = list(re.finditer(ctas_pat, bloque, re.IGNORECASE | re.DOTALL))
    if matches_ctas:
        mejor = max(matches_ctas, key=lambda x: len(x.group(1)))
        raw = mejor.group(1).strip().rstrip(",;").strip()
        exc.ctas_ctes = re.sub(r'\s*[;,]\s*', ' / ', " ".join(raw.split()))

    # ── Ordenanza de referencia ──────────────────────────────────────────────
    # Prioriza la ordenanza de pago del tributo (la más relevante para el caso)
    for pat in [
        r"Ordenanza\s+N[°oº]?\s*(106/\d+)",
        r"Ordenanza\s+N[°oº]?\s*(163/\d+)",
        r"Ordenanza\s+N[°oº]?\s*(142/\d+)",
        r"Ordenanza\s+N[°oº]?\s*(\d+/\d+)",
    ]:
        m = re.search(pat, bloque, re.IGNORECASE)
        if m:
            exc.ordenanza_ref = m.group(1)
            break

    return exc


def extraer_fecha_del_texto(texto: str) -> str:
    """Extrae la fecha de la sesión del encabezado del documento."""
    # "Sesión Ordinaria del día 17 de diciembre de 2025"
    m = re.search(
        r"Sesi[oó]n\s+Ordinaria\s+del\s+d[ií]a\s+(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})",
        texto, re.IGNORECASE
    )
    if m:
        return m.group(1).replace("\n", " ").strip()
    # "Miércoles, 4 de setiembre de 2024" en el encabezado
    m = re.search(
        r"(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo),?\s+"
        r"(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})",
        texto, re.IGNORECASE
    )
    if m:
        return m.group(1).strip()
    return ""


# Términos que DEBEN aparecer para que sea una excepción edilicia real
# (filtra falsos positivos como minutas sobre dengue, estacionamiento, etc.)
# Un bloque debe tener AUTORIZAR + (anteproyecto O proyecto de construcción) + construcción
def es_excepcion_edilicia(bloque: str) -> bool:
    tiene_autorizar = bool(re.search(r"AUTORIZAR", bloque, re.IGNORECASE))
    tiene_proyecto  = bool(re.search(r"anteproyecto|proyecto de construcci[oó]n", bloque, re.IGNORECASE))
    tiene_construccion = bool(re.search(r"construcci[oó]n", bloque, re.IGNORECASE))
    return tiene_autorizar and tiene_proyecto and tiene_construccion


# ─────────────────────────────────────────────────────────────────────────────
# Paso 5: Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────
def procesar_minuta(info: dict, dump_text: bool = False) -> list[Excepcion]:
    """Descarga y procesa una minuta. Devuelve lista de excepciones."""
    url     = info["direct"]
    año     = info.get("year") or "????"
    texto_link = info.get("texto", "")

    # Nombre de archivo: derivado del ID de Drive (sin subdirectorio de año,
    # para evitar descargar el mismo PDF dos veces si aparece en años distintos)
    file_id = re.search(r"id=([a-zA-Z0-9_-]+)", url)
    fname   = f"{file_id.group(1) if file_id else 'unknown'}.pdf"
    destino = DOWNLOAD_DIR / fname

    print(f"   ⬇  [{año}] {texto_link or fname} … ", end=" ", flush=True)
    if destino.exists() and destino.stat().st_size > 1000:
        print("ya descargado")
        ok = True
    else:
        ok = descargar_pdf(url, destino)
        if not ok:
            print("FALLO")
            return []
        print("OK")

    texto = extraer_texto_pdf(destino)
    if not texto:
        return []

    if dump_text:
        txt_path = destino.with_suffix(".txt")
        txt_path.write_text(texto, encoding="utf-8")
        print(f"      📝 Texto guardado en {txt_path}")

    # La fecha exacta viene del texto del PDF (encabezado de la sesión).
    # El contexto HTML (mes/año del heading) es solo fallback cuando el PDF no la tiene.
    fecha_sesion = extraer_fecha_del_texto(texto) or info.get("fecha_ctx", "")
    bloques = encontrar_bloques_excepcion(texto)

    if not bloques:
        print(f"      — sin dictámenes de excepción")
        return []

    resultados = []
    for bloque in bloques:
        if not es_excepcion_edilicia(bloque):
            continue
        exc = extraer_campos(bloque, url_fuente=info["url"])
        if not exc.fecha_sesion:
            exc.fecha_sesion = fecha_sesion
        resultados.append(exc)

    print(f"      ✅ {len(resultados)} excepción(es) guardada(s) (de {len(bloques)} bloques)")
    return resultados


def guardar_resultados(excepciones: list[Excepcion]):
    """Exporta los resultados a CSV y JSON."""
    campos = list(Excepcion.__dataclass_fields__.keys())  # type: ignore

    # CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        for exc in excepciones:
            writer.writerow(asdict(exc))
    print(f"📄 CSV guardado: {OUTPUT_CSV}")

    # JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump([asdict(e) for e in excepciones], f, ensure_ascii=False, indent=2)
    print(f"📄 JSON guardado: {OUTPUT_JSON}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Extrae excepciones al Plan Regulador de minutas JMA")
    parser.add_argument("--debug-links", action="store_true",
                        help="Imprime todos los links de Drive encontrados antes de filtrar por año")
    parser.add_argument("--html", metavar="FILE", default=None,
                        help="Usar HTML local guardado del navegador (ej: sesiones.html) en lugar de hacer request HTTP")
    parser.add_argument("--dump-text", action="store_true",
                        help="Guarda el texto extraído de cada PDF en minutas_pdf/<año>/<id>.txt para depuración")
    args = parser.parse_args()

    print("=" * 60)
    print("  JMA – Extractor de Excepciones al Plan Regulador")
    print("=" * 60)

    DOWNLOAD_DIR.mkdir(exist_ok=True)

    # 1. Obtener índice de minutas
    links = obtener_links_minutas(html_local=args.html)

    if args.debug_links:
        print("\n── DEBUG: links encontrados ──────────────────────────────")
        for lnk in links:
            print(f"  año={lnk['year']}  texto={lnk['texto']!r:20s}  url={lnk['url'][:60]}")
        print()

    if not links:
        print("No se encontraron links. Verificar que el sitio esté accesible.")
        sys.exit(1)

    # 2. Procesar cada minuta
    todas_las_excepciones: list[Excepcion] = []
    total = len(links)
    for i, info in enumerate(links, 1):
        print(f"\n[{i}/{total}] {info.get('url','')[:80]}")
        try:
            excepciones = procesar_minuta(info, dump_text=args.dump_text)
            todas_las_excepciones.extend(excepciones)
        except Exception as e:
            print(f"   ❌ Error inesperado: {e}")
        time.sleep(SLEEP_BETWEEN)

    # 3. Guardar
    print(f"\n{'='*60}")
    print(f"Total de excepciones extraídas: {len(todas_las_excepciones)}")
    guardar_resultados(todas_las_excepciones)

    # 4. Resumen en pantalla
    if todas_las_excepciones:
        print("\n── MUESTRA DE RESULTADOS ──────────────────────────────────")
        for exc in todas_las_excepciones[:5]:
            print(f"  Exp. {exc.expediente} | {exc.niveles} niveles | {exc.destino}")
            print(f"  Ubicación: {exc.ubicacion}")
            print(f"  Solicitante: {exc.solicitante}")
            print()


if __name__ == "__main__":
    main()
