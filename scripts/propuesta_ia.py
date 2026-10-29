# -*- coding: utf-8 -*-
"""
PRESOE · actualización diaria con IA — v4 (publicación automática + revisión de lo sensible).

v4: los cambios NO sensibles (hitos nuevos en las cronologías, resúmenes, relojes,
titulares) se publican solos cada día en datos.js, ordenando las cronologías por
fecha. Los cambios SENSIBLES (estado procesal, prisión, fallos, fase de una causa,
personas o causas nuevas) nunca se publican solos: van a un Pull Request aparte.

Texto original de la v3:

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
import copy, json, os, re, sys, time, urllib.error, urllib.request
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

SENSIBLES_PERSONA = ("estado", "prision", "fallo")
SENSIBLES_CAUSA = ("fase", "fallo")

def cambios_sensibles(antes, despues):
    g_antes = {p["id"]: p for p in antes["gente"]}
    c_antes = {c["id"]: c for c in antes["casos"]}
    avisos = []
    for p in despues["gente"]:
        o = g_antes.get(p["id"])
        if o is None:
            avisos.append(f"- ⚠ Persona NUEVA: **{p.get('nombre')}** (estado: {p.get('estado')})")
            continue
        if p.get("estado") != o.get("estado"):
            avisos.append(f"- ⚠ Cambio de estado de **{p.get('nombre')}**: "
                          f"`{o.get('estado')}` → `{p.get('estado')}`")
        if p.get("prision") != o.get("prision"):
            avisos.append(f"- ⚠ Cambio de situación de prisión de **{p.get('nombre')}**")
        if p.get("fallo") != o.get("fallo"):
            avisos.append(f"- ⚠ Fallo modificado o añadido para **{p.get('nombre')}**")
    for c in despues["casos"]:
        o = c_antes.get(c["id"])
        if o is None:
            avisos.append(f"- ⚠ Causa NUEVA: `{c['id']}` ({c.get('nombre')})")
            continue
        if c.get("fase") != o.get("fase"):
            avisos.append(f"- ⚠ Cambio de fase de la causa **{c.get('nombre')}**: "
                          f"`{o.get('fase')}` → `{c.get('fase')}`")
        if c.get("fallo") != o.get("fallo"):
            avisos.append(f"- ⚠ Fallo de la causa **{c.get('nombre')}** modificado o añadido")
    return avisos

def _restaura(destino, origen, campos):
    for campo in campos:
        if campo in origen:
            destino[campo] = copy.deepcopy(origen[campo])
        else:
            destino.pop(campo, None)

def version_segura(antes, propuesta):
    """La propuesta de la IA sin ningún cambio sensible: conserva hitos, resúmenes,
    relojes y ticker nuevos, pero deja estado/prisión/fallos/fase como estaban y
    descarta personas y causas nuevas (esas esperan a la revisión humana)."""
    s = copy.deepcopy(propuesta)
    g_antes = {p["id"]: p for p in antes["gente"]}
    c_antes = {c["id"]: c for c in antes["casos"]}
    s["gente"] = [p for p in s["gente"] if p["id"] in g_antes]
    s["casos"] = [c for c in s["casos"] if c["id"] in c_antes]
    for p in s["gente"]:
        _restaura(p, g_antes[p["id"]], SENSIBLES_PERSONA)
        p["casos"] = [cid for cid in p.get("casos", []) if cid in c_antes]
    for c in s["casos"]:
        _restaura(c, c_antes[c["id"]], SENSIBLES_CAUSA)
        c["gente"] = [pid for pid in c.get("gente", []) if pid in g_antes]
    ids = {p["id"] for p in s["gente"]}
    s["hub"] = {k: v for k, v in s["hub"].items() if k in antes["hub"] or k in c_antes}
    s["fotos_wiki"] = {k: v for k, v in s["fotos_wiki"].items()
                       if k in antes["fotos_wiki"] or k in ids}
    rel_antes = [json.dumps(r, sort_keys=True) for r in antes["relaciones"]]
    s["relaciones"] = [r for r in s["relaciones"]
                       if json.dumps(r, sort_keys=True) in rel_antes
                       or (r.get("a") in ids and r.get("b") in ids)]
    return s

_MESES = {"ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,"jul":7,"ago":8,
          "sep":9,"set":9,"oct":10,"nov":11,"dic":12}

def _clave_fecha(f):
    f = (f or "").strip().lower()
    m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})", f)
    if m:
        return (int(m[3]), int(m[2]), int(m[1]))
    m = re.match(r"^([a-zé]{3})[a-zé]*\.?\s+(?:de\s+)?(\d{4})", f)
    if m and m[1][:3] in _MESES:
        return (int(m[2]), _MESES[m[1][:3]], 0)
    m = re.match(r"^(\d{4})$", f)
    if m:
        return (int(m[1]), 0, 0)
    return None

def ordena_hitos(d):
    """Ordena cada cronología por fecha (la IA añade los hitos al final)."""
    for c in d["casos"]:
        hitos, ultima, claves = c.get("hitos", []), (0, 0, 0), []
        for h in hitos:
            k = _clave_fecha(h.get("f"))
            ultima = k if k else ultima      # sin fecha legible: se queda tras el anterior
            claves.append(ultima)
        orden = sorted(range(len(hitos)), key=lambda i: claves[i])
        c["hitos"] = [hitos[i] for i in orden]

def escribe_en(ruta, d):
    with open(ruta, "w", encoding="utf-8") as f:
        f.write("window.PRESOE_DATOS =\n")
        f.write(json.dumps(d, ensure_ascii=False, indent=2))
        f.write(";\n")

def salida_gh(**kv):
    ruta = os.environ.get("GITHUB_OUTPUT")
    if ruta:
        with open(ruta, "a", encoding="utf-8") as f:
            for k, v in kv.items():
                f.write(f"{k}={'true' if v else 'false'}\n")

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

    ordena_hitos(propuesta)
    if propuesta == datos:
        open("cambios_pr.md","w",encoding="utf-8").write("La IA no propone cambios hoy.")
        print("Sin cambios propuestos.")
        return

    # 1) Parte segura: se publica sola en datos.js
    avisos = cambios_sensibles(datos, propuesta)
    segura = version_segura(datos, propuesta) if avisos else propuesta
    publicar = segura != datos
    if publicar:
        error = valida(segura, datos)
        if error:
            print(f"Aviso: la parte segura no pasa la validación ({error}); "
                  "todo queda para revisión.")
            publicar = False
    if publicar:
        escribe_datos(segura)
        print("Cambios no sensibles escritos en datos.js para publicarse ya.")

    # 2) Parte sensible: se guarda aparte y el workflow abre un Pull Request
    if avisos:
        escribe_en("datos_propuesta.js", propuesta)
        lineas = ["## Cambios SENSIBLES pendientes de tu revisión",
                  f"Generados el {hoy} a partir de {len(titulares)} titulares.", "",
                  ("Las novedades no sensibles (cronologías, resúmenes, relojes) "
                   "ya se han publicado solas. " if publicar else "") +
                  "Este PR solo contiene lo que NO se publica sin tu visto bueno:", ""]
        lineas += avisos
        lineas += ["", "Si es correcto, pulsa **Merge**. Si no, ciérralo sin más.", "",
                   "Titulares usados como evidencia:", ""]
        lineas += [f"- {t}" for t in titulares[:25]]
        open("cambios_pr.md","w",encoding="utf-8").write("\n".join(lineas))
        print(f"{len(avisos)} cambios sensibles apartados para revisión.")

    salida_gh(publicar=publicar, revisar=bool(avisos))

if __name__ == "__main__":
    main()
