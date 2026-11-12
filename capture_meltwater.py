"""
Meltwater Auto-Capture · Monitor Oniria\\TBWA · Paraguay (NSGroup)
==================================================================
Abre el shareable report de Meltwater con Playwright (sesión de navegador
real: Meltwater bloquea IPs de datacenter en el endpoint de snapshot), espera
a que se genere el `.json.gz`, lo valida (señal > 0) y lo entrega al worker.

Dos modos de entrega, en este orden de preferencia:

  1. DIRECTO (recomendado, estándar NSG nov-2026):
     secrets WORKER_INGEST_URL + SYNC_TOKEN
     -> POST del JSON completo a https://<worker>/ingest
     Los fallos del worker son sincrónicos y visibles en el run.

  2. RELAY POR MAKE (compatibilidad):
     secret MAKE_WEBHOOK_URL
     -> POST {"url": <gz>, "sync": true} al webhook; Make reenvía a /update-url.

GOTCHA Cloudflare: el POST directo con urllib y User-Agent por defecto
("Python-urllib/...") devuelve 403 code 1010 (Browser Integrity Check).
Por eso post_to_worker manda User-Agent de navegador + Accept.
"""

import asyncio
import gzip
import json
import os
import random
import sys
import time
import traceback
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright


def _req(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        sys.exit(f"FALTA el secret/env requerido: {name}")
    return val


MELTWATER_URL = _req("MELTWATER_URL")
MELTWATER_PASSWORD = os.environ.get("MELTWATER_PASSWORD", "")

WORKER_INGEST_URL = os.environ.get("WORKER_INGEST_URL", "").strip()
SYNC_TOKEN = os.environ.get("SYNC_TOKEN", "").strip()
MAKE_WEBHOOK = os.environ.get("MAKE_WEBHOOK_URL", "").strip()
if not WORKER_INGEST_URL and not MAKE_WEBHOOK:
    sys.exit("Configurá WORKER_INGEST_URL (+SYNC_TOKEN) o MAKE_WEBHOOK_URL")

MAX_WAIT_SECONDS = int(os.environ.get("MAX_WAIT_SECONDS", "300"))
POLL_INTERVAL_MS = int(os.environ.get("POLL_INTERVAL_MS", "5000"))
STALL_RELOAD_SECONDS = int(os.environ.get("STALL_RELOAD_SECONDS", "120"))
JITTER_SECONDS = int(os.environ.get("JITTER_SECONDS", "0"))
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"
PROXY_URL = os.environ.get("PROXY_URL", "").strip()

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

DEBUG_DIR = Path("debug_output")
DEBUG_DIR.mkdir(exist_ok=True)


def compute_signal(data: dict) -> int:
    """Suma de 'señal' recorriendo tabs/rows/cards/fragments. 0 = snapshot vacío."""
    ip = (data or {}).get("insightPage") or {}
    signal = 0
    for t in ip.get("tabs") or []:
        for row in t.get("rows") or []:
            for card in row.get("cards") or []:
                for frag in card.get("fragments") or []:
                    d = frag.get("data") or {}
                    hits = d.get("hits")
                    if isinstance(hits, list):
                        signal += len(hits)
                    tot = d.get("total")
                    if isinstance(tot, (int, float)):
                        signal += tot
                    date = (d.get("aggs") or {}).get("date") or {}
                    for v in date.get("values") or []:
                        signal += (v.get("counts") or {}).get("doc") or 0
    return signal


async def fetch_gz(page, url: str):
    """Baja el .gz en la misma sesión, descomprime y devuelve (signal, data, size)."""
    resp = await page.request.get(url, timeout=45_000)
    if not resp.ok:
        print(f"  GET gz status {resp.status}", flush=True)
        return -1, None, 0
    body = await resp.body()
    size = len(body)
    if len(body) >= 2 and body[0] == 0x1F and body[1] == 0x8B:
        try:
            body = gzip.decompress(body)
        except Exception as e:
            print(f"  gzip.decompress falló: {e}", flush=True)
            return -1, None, size
    try:
        data = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as e:
        print(f"  json.loads falló: {e}", flush=True)
        return -1, None, size
    return compute_signal(data), data, size


def _proxy_kwargs():
    if not PROXY_URL:
        return {}
    p = urlparse(PROXY_URL)
    server = f"{p.scheme}://{p.hostname}" + (f":{p.port}" if p.port else "")
    proxy = {"server": server}
    if p.username:
        proxy["username"] = p.username
    if p.password:
        proxy["password"] = p.password
    print(f"→ Usando proxy: {server}", flush=True)
    return {"proxy": proxy}


async def capture() -> tuple:
    gz = {"url": None}
    snapshot_statuses, all_responses = [], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                  "--disable-dev-shm-usage", "--disable-features=IsolateOrigins,site-per-process"],
        )
        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="es-ES",
            user_agent=BROWSER_UA,
            extra_http_headers={"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"},
            **_proxy_kwargs(),
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        def on_response(response):
            url, st = response.url, response.status
            all_responses.append(f"{st} {url[:160]}")
            if "snapshot" in url:
                snapshot_statuses.append(f"{st} {url[:120]}")
            if "insightPageSnapshot.json.gz" in url and st == 200:
                gz["url"] = url
                print("✓ URL .gz detectada", flush=True)

        page.on("response", on_response)

        print("→ Navegando a Meltwater...", flush=True)
        await page.goto(MELTWATER_URL, wait_until="domcontentloaded", timeout=60_000)
        print(f"  page.title = {await page.title()}", flush=True)

        # Password gate (Web Component flux-textfield#passcode)
        try:
            await page.wait_for_selector("#passcode", timeout=20_000, state="attached")
            print("→ Password gate detectado, enviando passcode...", flush=True)
            await page.wait_for_function(
                "() => document.getElementById('passcode') && document.getElementById('submit') "
                "&& typeof submitPasscode === 'function'", timeout=15_000)
            await page.evaluate(
                """(pwd) => { const i = document.getElementById('passcode');
                    i.value = pwd; i.dispatchEvent(new Event('input', {bubbles:true})); submitPasscode(); }""",
                MELTWATER_PASSWORD)
            await page.wait_for_load_state("domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(2_000)
        except Exception:
            print("→ Sin password gate (¿ya autenticado o reporte abierto?)", flush=True)

        try:
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass

        print(f"→ Esperando .gz (hasta {MAX_WAIT_SECONDS}s, sin recargas agresivas)...", flush=True)
        deadline = time.time() + MAX_WAIT_SECONDS
        last_reload = time.time()
        payload, gz_url = None, None

        while time.time() < deadline:
            if gz["url"]:
                signal, data, size = await fetch_gz(page, gz["url"])
                print(f"  señal={signal}  size={size}B", flush=True)
                if signal > 0:
                    payload, gz_url = data, gz["url"]
                    print(f"✓ Snapshot CON datos (señal={signal}).", flush=True)
                    break
                print("⚠ Snapshot vacío (regenerando). Sigo esperando...", flush=True)
                gz["url"] = None

            if (time.time() - last_reload) > STALL_RELOAD_SECONDS:
                print(f"→ Stall > {STALL_RELOAD_SECONDS}s. Reload suave único.", flush=True)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=45_000)
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                except Exception:
                    pass
                last_reload = time.time()

            await page.wait_for_timeout(POLL_INTERVAL_MS)

        try:
            await page.screenshot(path=str(DEBUG_DIR / "final.png"))
        except Exception:
            pass
        (DEBUG_DIR / "responses.txt").write_text("\n".join(all_responses))
        (DEBUG_DIR / "snapshot_statuses.txt").write_text("\n".join(snapshot_statuses))
        await browser.close()

    if not payload:
        diag = "\n".join(snapshot_statuses[-10:]) or "(sin requests a /snapshot)"
        raise RuntimeError(
            f"No se obtuvo un .gz con datos en {MAX_WAIT_SECONDS}s.\n"
            f"Últimos estados del endpoint snapshot:\n{diag}\n"
            f"502 persistente = Meltwater bloquea la IP de salida.")
    return payload, gz_url


def post_to_worker(payload: dict) -> int:
    print(f"→ POST directo al worker: {WORKER_INGEST_URL}", flush=True)
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        WORKER_INGEST_URL, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "x-sync-token": SYNC_TOKEN,
                 "User-Agent": BROWSER_UA,          # evita 403 Cloudflare code 1010
                 "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        out = resp.read().decode("utf-8", errors="replace")[:400]
        print(f"✓ Worker respondió {resp.status}: {out}", flush=True)
        return resp.status


def post_to_make(gz_url: str) -> int:
    print("→ POST al webhook de Make...", flush=True)
    body = json.dumps({"url": gz_url, "sync": True}).encode("utf-8")
    req = urllib.request.Request(
        MAKE_WEBHOOK, data=body, method="POST",
        headers={"Content-Type": "application/json", "User-Agent": BROWSER_UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        out = resp.read().decode("utf-8", errors="replace")[:200]
        print(f"✓ Make respondió {resp.status}: {out}", flush=True)
        return resp.status


async def main():
    try:
        if JITTER_SECONDS > 0:
            d = random.randint(0, JITTER_SECONDS)
            print(f"→ Jitter: durmiendo {d}s...", flush=True)
            await asyncio.sleep(d)

        payload, gz_url = await capture()

        if WORKER_INGEST_URL:
            status = post_to_worker(payload)
        else:
            status = post_to_make(gz_url)

        if status not in (200, 202):
            sys.exit(f"Status inesperado en la entrega: {status}")
        print("✓ Pipeline completo OK.", flush=True)
    except SystemExit:
        raise
    except Exception as e:
        print(f"\n✗ ERROR: {e}", flush=True)
        traceback.print_exc()
        (DEBUG_DIR / "error.txt").write_text(f"{e}\n\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
