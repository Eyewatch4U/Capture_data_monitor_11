# ONIRIA-PY-capture-data

Captura automática del reporte de Meltwater del **Monitor Oniria\TBWA · Paraguay**
(NSGroup / Eyewatch Intelligence Unit) y entrega al Cloudflare Worker.

```
cron-job.org (cada 30 min)
  → workflow_dispatch de este repo
  → capture_meltwater.py (Playwright baja y valida el .json.gz)
  → POST /ingest del worker  (o relay por Make → /update-url)
  → KV ONIRIAMONITOR · key latest_oniria_py
  → tablero https://monitoroniria.reports-dca.workers.dev/monitor-oniria
```

Búsqueda de Meltwater: **NBZ_PY_Oniria TBWA** (savedSearch `27098612`).

## Secrets del repositorio

| Secret | Para qué |
|---|---|
| `MELTWATER_URL` | URL pública del snapshot del reporte. Forma `.../report/public/<id>/snapshot`. Si la 1ª corrida vuelve vacía o 502, usar la tokenizada `.../public/<id>/insight-pages/snapshot?snapshot_token=...` |
| `MELTWATER_PASSWORD` | Passcode del share (vacío si no tiene gate) |
| `WORKER_INGEST_URL` | `https://monitoroniria.reports-dca.workers.dev/ingest` — entrega directa |
| `SYNC_TOKEN` | Debe coincidir con el secret del worker (`MAKE2026PY`) |
| `MAKE_WEBHOOK_URL` | *Opcional.* Solo si se usa el relay por Make en vez del directo |

Si `WORKER_INGEST_URL` está cargado, el script **ignora Make** y postea el JSON
completo al worker. Es el modo recomendado: los errores del worker se ven en el
run en vez de quedar ocultos detrás del ACK inmediato de Make.

## Disparo

cron-job.org, cada 30 minutos:

```
POST https://api.github.com/repos/Eyewatch4U/ONIRIA-PY-capture-data/actions/workflows/meltwater.yml/dispatches
Authorization: Bearer <PAT con Actions RW>
Accept: application/vnd.github+json
Content-Type: application/json

{"ref":"main"}
```

## Diagnóstico

- Run fallido → artifact `debug-<run_id>` (`responses.txt`, `snapshot_statuses.txt`, `final.png`).
- `502` repetidos en `/snapshot` = Meltwater está bloqueando la IP de salida del runner.
- `403` del worker con `code 1010` = Cloudflare rechazó el User-Agent (ya mitigado: el POST manda UA de navegador).
- Worker: `GET /health` y `GET /whoami`.
