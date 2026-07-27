# -*- coding: utf-8 -*-
"""
PRESOE · propuesta diaria con IA (revisión humana obligatoria) — v3 con streaming.

Recoge los titulares del día, se los pasa a la API de Anthropic junto con el
datos.js actual y pide una versión actualizada del JSON. El resultado NO se
publica: se valida aquí y, si hay cambios, el workflow abre un Pull Request
para que lo revises y lo fusiones tú. Los cambios de estado procesal o de
fallos se marcan como SENSIBLES en la descripción del PR.

Novedades v3: la respuesta de la IA llega por goteo (streaming), de modo que no
hay límite práctico de duración; techo de salida ampliado; y se mantienen los
mensajes de error claros, los reintentos y la detección de truncado de la v2.

Requiere el secreto ANTHROPIC_API_KEY en el repositorio.
"""
import json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone

sys.stdout.reconfigure(line_buffering=True)

# reutilizamos la descarga de titulares del robot del ticker
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from actualiza_ticker import CONSULTAS, descarga_rss, parsea_rss, lee_datos, escribe_datos

MODELO = "claude-sonnet-4-6"
MAX_SALIDA = 32000
CLAVES = {"actualizado_datos","actualizado_ticker","hub","casos","gente",
          "relaciones","ticker","relojes","fotos_wiki"}
ESTADOS = {"condenado","juicio","investigado","contexto"}

SISTEMA = """Mantienes datos.js, la base de datos de PRESOE, un panel público que sigue \
causas judiciales vinculadas al PSOE en España. Reglas innegociables:
1. Tu ÚNICA evidencia son los titulares que te paso. No uses conocimiento propio para \
afirmar hechos nuevos: si un titular no lo dice, no existe.
2. NUNCA cambies el campo "estado" de una persona ni añadas o modifiques un "fallo" \
salvo que un titular informe expresamente de esa resolución judicial (condena, \
absolución, procesamiento, archivo). Si lo haces, incluye el medio entre paréntesis \
en el texto del hito correspondiente.
3. Mantén siempre el lenguaje de presunción: «presunto», «según el instructor», \
«la defensa niega». Las condenas recurribles no son firmes: dilo.
4. Puedes: añadir hitos con fecha a las cronologías, matizar resúmenes, añadir un \
reloj tipo "countdown" con "objetivo" en ISO si un titular da fecha exacta de un \
señalamiento, y añadir personas o causas nuevas SOLO con evidencia sólida en varios \
titulares (elige coordenadas x/y libres, a más de 120 px de los nodos existentes, \
dentro de 60-1650 × 60-1100, y añade su hub si es una causa).
5. Si cambias algo sustantivo, pon "actualizado_datos" en la fecha de hoy.
6. Si ningún titular aporta nada fiable, devuelve el JSON EXACTAMENTE igual.
Responde ÚNICAMENTE con el JSON completo actualizado, sin comentarios ni marcado, \
en formato compacto (una sola línea, sin sangrías ni saltos de línea)."""

def llama_api(clave, sistema, usuario):
    cuerpo = json.dumps({
        "model": MODELO,
        "max_tokens": MAX_SALIDA,
        "stream": True,
        "system": sistema,
        "messages": [{"role": "user", "content": usuario}],
    }).encode("utf-8")
    intentos = 0
    while True:
        intentos += 1
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=cuerpo,
            headers={
                "content-type": "application/json",
                "x-api-key": clave,
                "anthropic-version": "2023-06-01",
                "accept": "text/event-stream",
            },
        )
        try:
            trozos, stop, eventos = [], None, 0
            with urllib.request.urlopen(req, timeout=120) as r:
                for linea_b in r:
                    linea = linea_b.decode("utf-8", "replace").strip()
                    if not linea.startswith("data:"):
                        continue
                    dato = linea[5:].strip()
                    if dato == "[DONE]":
                        break
                    try:
                        ev = json.loads(dato)
                    except Exception:
                        continue
                    tipo = ev.get("type")
                    if tipo == "content_block_delta":
                        delta = ev.get("delta", {})
                        if delta.get("type") == "text_delta":
                            trozos.append(delta.get("text", ""))
                            eventos += 1
                            if eventos % 500 == 0:
                                print(f"  …generando ({sum(len(t) for t in trozos)} caracteres)")
                    elif tipo == "message_delta":
                        stop = ev.get("delta", {}).get("stop_reason") or stop
                    elif tipo == "error":
                        raise RuntimeError(ev.get("error", {}).get("message", "error de la API en el stream"))
            texto = "".join(trozos)
            if not texto:
                raise RuntimeError("la API devolvió un stream vacío")
            break
        except urllib.error.HTTPError as e:
            detalle = ""
            try:
                detalle = e.read().decode("utf-8", "replace")[:600]
            except Exception:
                pass
            if e.code in (429, 529) and intentos < 3:
                print(f"La API está ocupada (HTTP {e.code}); reintento en 30 s…")
                time.sleep(30)
                continue
            if e.code == 401:
                print("ERROR: la API rechazó la clave (HTTP 401). El secreto "
                      "ANTHROPIC_API_KEY está mal pegado o la clave fue revocada: "
                      "crea una nueva en console.anthropic.com → API Keys y "
                      "actualiza el secreto.", file=sys.stderr)
            elif e.code == 400 and "credit" in detalle.lower():
                print("ERROR: no hay saldo en la cuenta de Anthropic "
                      "('credit balance is too low'). Carga crédito en "
                      "console.anthropic.com → Billing y vuelve a lanzar.", file=sys.stderr)
            else:
                print(f"ERROR de la API (HTTP {e.code}): {detalle}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            if intentos < 2:
                print(f"Fallo de red durante la llamada a la API ({e}); reintento en 30 s…")
                time.sleep(30)
                continue
            print(f"ERROR: no se pudo completar la llamada a la API: {e}", file=sys.stderr)
            sys.exit(1)
    if stop == "max_tokens":
        print("ERROR: la respuesta de la IA quedó truncada por el límite de salida. "
              "Sube MAX_SALIDA en scripts/propuesta_ia.py (p. ej. a 48000) y relanza.",
              file=sys.stderr)
        sys.exit(1)
    return texto

def valida(d, referencia):
    if set(d) != CLAVES:
        return "claves de nivel superior alteradas"
    ids_gente = {p.get("id") for p in d["gente"]}
    ids_casos = {c.get("id") for c in d["casos"]}
    for p in d["gente"]:
        for campo in ("id","nombre","ini","estado","x","y","casos","rol","resumen"):
            if campo not in p:
                return f"persona sin campo «{campo}»"
        if p["estado"] not in ESTADOS:
            return f"estado inválido: {p['estado']}"
        if not set(p["casos"]) <= ids_casos:
            return f"persona {p['id']} con causa inexistente"
    for c in d["casos"]:
        if not set(c.get("gente", [])) <= ids_gente:
            return f"causa {c.get('id')} con persona inexistente"
        if c.get("id") not in d["hub"]:
            return f"causa {c.get('id')} sin coordenadas de hub"
    for r in d["relojes"]:
        if r.get("tipo") not in {"countdown","countup","standby"}:
            return "reloj con tipo inválido"
    return None

def cambios_sensibles(antes, despues):
    est_antes = {p["id"]: p.get("estado") for p in antes["gente"]}
    fal_antes = {p["id"]: p.get("fallo") for p in antes["gente"]}
    avisos = []
    for p in despues["gente"]:
        pid = p["id"]
        if pid not in est_antes:
            avisos.append(f"- ⚠ Persona NUEVA: **{p.get('nombre')}** (estado: {p.get('estado')})")
            continue
        if p.get("estado") != est_antes[pid]:
            avisos.append(f"- ⚠ Cambio de estado de **{p.get('nombre')}**: "
                          f"`{est_antes[pid]}` → `{p.get('estado')}`")
        if p.get("fallo") != fal_antes.get(pid):
            avisos.append(f"- ⚠ Fallo modificado o añadido para **{p.get('nombre')}**")
    nuevos_casos = {c["id"] for c in despues["casos"]} - {c["id"] for c in antes["casos"]}
    for cid in nuevos_casos:
        avisos.append(f"- ⚠ Causa NUEVA: `{cid}`")
    return avisos

def main():
    clave = os.environ.get("ANTHROPIC_API_KEY")
    if not clave:
        print("ERROR: falta el secreto ANTHROPIC_API_KEY en el repositorio "
              "(Settings → Secrets and variables → Actions → Repository secrets).",
              file=sys.stderr)
        sys.exit(1)

    datos = lee_datos()
    titulares = []
    for consulta in CONSULTAS:
        try:
            for fecha, titulo, fuente in parsea_rss(descarga_rss(consulta))[:8]:
                titulares.append(f"[{fecha:%d-%m-%Y}] {titulo} ({fuente})")
        except Exception:
            pass
    titulares = list(dict.fromkeys(titulares))[:60]
    print(f"Titulares recogidos: {len(titulares)}")
    if not titulares:
        open("cambios_pr.md","w",encoding="utf-8").write("Sin titulares hoy: sin propuesta.")
        print("Sin titulares; no se llama a la API.")
        return

    hoy = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    usuario = (f"Fecha de hoy: {hoy}.\n\nTITULARES RECOGIDOS HOY:\n" +
               "\n".join(titulares) +
               "\n\nJSON ACTUAL DE datos.js:\n" +
               json.dumps(datos, ensure_ascii=False))

    print("Llamando a la API (puede tardar 2-4 minutos)…")
    texto = llama_api(clave, SISTEMA, usuario)
    print(f"Respuesta recibida: {len(texto)} caracteres.")

    inicio, fin = texto.find("{"), texto.rfind("}")
    if inicio < 0 or fin < 0:
        print("ERROR: la respuesta de la IA no contiene JSON.", file=sys.stderr)
        sys.exit(1)
    try:
        propuesta = json.loads(texto[inicio:fin+1])
    except Exception as e:
        print(f"ERROR: el JSON de la respuesta no es válido ({e}).", file=sys.stderr)
        sys.exit(1)

    error = valida(propuesta, datos)
    if error:
        print(f"ERROR: propuesta rechazada por validación: {error}.", file=sys.stderr)
        sys.exit(1)

    if propuesta == datos:
        open("cambios_pr.md","w",encoding="utf-8").write("La IA no propone cambios hoy.")
        print("Sin cambios propuestos.")
        return

    avisos = cambios_sensibles(datos, propuesta)
    lineas = ["## Propuesta automática de actualización de PRESOE",
              f"Generada el {hoy} a partir de {len(titulares)} titulares.", "",
              "**Revisa antes de fusionar.** En especial:"]
    lineas += avisos if avisos else ["- (sin cambios de estado, fallos ni altas: solo cronologías/resúmenes/relojes)"]
    lineas += ["", "Titulares usados como evidencia:", ""]
    lineas += [f"- {t}" for t in titulares[:25]]
    open("cambios_pr.md","w",encoding="utf-8").write("\n".join(lineas))

    escribe_datos(propuesta)
    print(f"Propuesta escrita en datos.js ({len(avisos)} cambios sensibles).")

if __name__ == "__main__":
    main()
