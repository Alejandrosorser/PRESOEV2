# -*- coding: utf-8 -*-
"""
PRESOE · robot de titulares.
Lee Google News RSS (es) para las causas del panel y reescribe el bloque
"ticker" de datos.js con los titulares más recientes, citando el medio.
Solo toca el ticker y la fecha 'actualizado_ticker': nunca los datos judiciales.
Sin dependencias externas (solo biblioteca estándar).
"""
import json, re, sys, urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

RUTA_DATOS = "datos.js"
DIAS_MAX = 12          # ignora titulares más viejos que esto
MAX_ITEMS = 13
MESES = ["ENE","FEB","MAR","ABR","MAY","JUN","JUL","AGO","SEP","OCT","NOV","DIC"]

CONSULTAS = [
    "caso Koldo Ábalos",
    "Santos Cerdán Audiencia Nacional",
    "Begoña Gómez juicio jurado",
    "caso Plus Ultra Zapatero",
    "caso Mediador Tito Berni",
    "Leire Díez Audiencia Nacional",
    "García Ortiz fiscal general",
    "caso hidrocarburos Aldama",
]

def lee_datos():
    t = open(RUTA_DATOS, encoding="utf-8").read()
    cuerpo = t.replace("window.PRESOE_DATOS =", "", 1).rstrip().rstrip(";")
    return json.loads(cuerpo)

def escribe_datos(d):
    with open(RUTA_DATOS, "w", encoding="utf-8") as f:
        f.write("window.PRESOE_DATOS =\n")
        f.write(json.dumps(d, ensure_ascii=False, indent=2))
        f.write(";\n")

def descarga_rss(consulta):
    url = ("https://news.google.com/rss/search?q=" + urllib.parse.quote(consulta)
           + "&hl=es&gl=ES&ceid=ES:es")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (PRESOE-bot)"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read().decode("utf-8", "replace")

def parsea_rss(xml_text):
    """Devuelve [(fecha_datetime, titular, fuente), ...]"""
    salida = []
    raiz = ET.fromstring(xml_text)
    for item in raiz.iter("item"):
        titulo = (item.findtext("title") or "").strip()
        fecha_txt = (item.findtext("pubDate") or "").strip()
        fuente_el = item.find("source")
        fuente = fuente_el.text.strip() if fuente_el is not None and fuente_el.text else ""
        if not titulo:
            continue
        # Google News suele terminar el título en " - Fuente"
        if not fuente and " - " in titulo:
            titulo, fuente = titulo.rsplit(" - ", 1)
        elif fuente and titulo.endswith(" - " + fuente):
            titulo = titulo[: -(len(fuente) + 3)]
        try:
            fecha = parsedate_to_datetime(fecha_txt)
            if fecha.tzinfo is None:
                fecha = fecha.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        titulo = re.sub(r"\s+", " ", titulo).strip()
        if len(titulo) > 160:
            titulo = titulo[:157].rstrip() + "…"
        salida.append((fecha, titulo, fuente.strip()))
    return salida

def normaliza(t):
    return re.sub(r"[^a-z0-9áéíóúüñ]+", " ", t.lower()).strip()

def main():
    datos = lee_datos()
    limite = datetime.now(timezone.utc) - timedelta(days=DIAS_MAX)
    candidatos, vistos = [], set()
    fallos = 0
    for c in CONSULTAS:
        try:
            for fecha, titulo, fuente in parsea_rss(descarga_rss(c)):
                clave = normaliza(titulo)[:80]
                if fecha < limite or clave in vistos:
                    continue
                vistos.add(clave)
                candidatos.append((fecha, titulo, fuente))
        except Exception as e:
            fallos += 1
            print(f"  aviso: fallo con la consulta «{c}»: {e}", file=sys.stderr)
    if not candidatos:
        print("Sin titulares nuevos utilizables; datos.js queda intacto.")
        return
    candidatos.sort(key=lambda x: x[0], reverse=True)
    ticker = []
    for fecha, titulo, fuente in candidatos[:MAX_ITEMS]:
        etiqueta = f"{fecha.day:02d} {MESES[fecha.month-1]}"
        texto = f"{titulo} — {fuente}" if fuente else titulo
        ticker.append([etiqueta, texto])
    if ticker == datos.get("ticker"):
        print("El ticker ya estaba al día.")
        return
    datos["ticker"] = ticker
    datos["actualizado_ticker"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    escribe_datos(datos)
    print(f"Ticker actualizado: {len(ticker)} titulares ({fallos} consultas fallidas).")

if __name__ == "__main__":
    main()
