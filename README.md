# MQTT Event Viewer (C Edition)

Applicazione C standalone che si sottoscrive a un topic MQTT ed espone una dashboard HTTP per ispezionare in tempo reale gli eventi pubblicati.

- **[Mongoose](https://github.com/cesanta/mongoose)** fornisce sia il server HTTP sia il client MQTT.
- **[cJSON](https://github.com/DaveGamble/cJSON)** si occupa del parsing JSON.
- **SQLite** (opzionale) persiste gli eventi su disco.

Mongoose e cJSON sono inclusi nel repository (`mongoose.c/.h`, `cJSON.c/.h`): l'unica dipendenza esterna della build è SQLite. Il binario serve i file statici leggendo a runtime la cartella `web/`, che deve quindi trovarsi accanto all'eseguibile.

La cartella `web/` contiene una **interfaccia d'esempio minimale** e la **documentazione delle API** ([`web/docs.html`](web/docs.html)), raggiungibile anche dal server su `http://localhost:5000/docs.html`. Puoi sostituirla con la tua interfaccia.

## Funzionalità

- Eseguibile nativo (Windows/MinGW, Linux, ARM cross-compilato); nessun runtime Python richiesto.
- Connessione a qualsiasi broker/topic MQTT (wildcard supportate), configurabile da variabili d'ambiente o `.env`.
- Keepalive MQTT con PING espliciti e riconnessione automatica.
- Parsing dell'evento JSON strutturato: src/type/id/desc, blocchi rule/anc/meas, timestamp.
- API REST (`/api/events`, `/api/events/all`, `/api/status`, `/api/info`) e stream SSE (`/stream`) consumati dal frontend in `web/`.
- Buffer circolare in memoria degli eventi recenti.
- Persistenza SQLite opzionale con soglia massima e purge automatico dei record più vecchi.
- Loop HTTP e loop MQTT su due thread separati, coordinati da mutex: le richieste lunghe (history, replay SSE) non bloccano la ricezione dal broker.

## Build

### Prerequisiti

- GCC (MinGW-w64 su Windows) o una toolchain cross (es. `arm-linux-gnueabihf-gcc`).
- Autotools: `autoconf`, `automake`, `libtool`.
- Header e libreria SQLite (`sqlite3.h`, `-lsqlite3`), se si vuole la persistenza.

### Compilazione

```bash
./autogen.sh        # solo la prima volta, da un checkout git
./configure         # SQLite abilitato di default
make
```

Il risultato è `viewer` / `viewer.exe` nella root del repository.

Opzioni di `configure`:

- `--disable-sqlite` — build senza persistenza.
- `--without-ssl` — non linka OpenSSL anche se presente.
- `CPPFLAGS` / `LDFLAGS` — percorsi aggiuntivi per header e librerie.

Esempi di cross-compilazione:

```bash
# Windows da Linux
./configure --host=x86_64-w64-mingw32 CPPFLAGS="-I/path/sysroot/include" LDFLAGS="-L/path/sysroot/lib"

# ARM
./configure --host=arm-linux-gnueabihf CPPFLAGS="-I/path/arm-sysroot/include" LDFLAGS="-L/path/arm-sysroot/lib"
```

## Avvio

```powershell
.\viewer.exe
```

Il server ascolta su `http://localhost:5000` (modificabile con `HTTP_HOST` e `PORT`; usa `HTTP_HOST=0.0.0.0` per accettare connessioni da altre macchine).

Aprendo `http://localhost:5000`, la pagina d'esempio mostra versione, broker e stato MQTT (`/api/info`, `/api/status`). Carica poi gli ultimi 20 eventi da `/api/events` e aggiunge quelli nuovi in tempo reale via SSE (`/stream`).

Per scrivere un'interfaccia propria vedi [`web/docs.html`](web/docs.html). I vincoli principali: il server applica una Content-Security-Policy che vieta script e stili inline, e gli header CORS sono disabilitati, quindi la UI deve essere servita dallo stesso server, o dietro lo stesso reverse proxy.

Per la configurazione parti da [`.env.example`](.env.example): copialo in `.env` e modificalo.

## Configurazione

Variabili d'ambiente, oppure voci nel file `.env` (caricato automaticamente all'avvio):

| Variabile | Descrizione | Default |
| --- | --- | --- |
| `MQTT_BROKER_HOST` | Hostname del broker MQTT | `broker.hivemq.com` |
| `MQTT_BROKER_PORT` | Porta MQTT | `1883` |
| `MQTT_TOPIC` | Topic filter (wildcard supportate) | `public/test/topic` |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | Credenziali (opzionali) | vuoto |
| `MQTT_CLIENT_ID` | Client id MQTT | generato automaticamente |
| `MQTT_KEEPALIVE` | Keepalive in secondi | `60` |
| `EVENT_BUFFER_SIZE` | Numero massimo di eventi nel buffer in memoria | `8192` |
| `HTTP_HOST` | Indirizzo su cui ascolta il server HTTP | `localhost` |
| `PORT` | Porta HTTP | `5000` |
| `EVENT_DB_ENABLED` | Abilita (`1`) o disabilita (`0`) la persistenza SQLite | `1` |
| `EVENT_DB_PATH` | Percorso del database SQLite | `viewer-events.db` |
| `EVENT_DB_MAX_EVENTS` | Numero massimo di eventi persistiti (`0` = illimitato) | `5000` |
| `EVENT_DB_PURGE_BATCH` | Righe più vecchie eliminate quando si supera la soglia | `250` |
| `SSE_CACHE_LIMIT` | Eventi inviati subito a un nuovo client SSE (`0` = nessun limite) | `200` |
| `LOG_LEVEL` | Verbosità log: `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG` | `INFO` |

In produzione conviene `LOG_LEVEL=WARN` o `ERROR`; `DEBUG` solo per il troubleshooting.

## Endpoint HTTP

| Endpoint | Descrizione |
| --- | --- |
| `GET /` | Dashboard statica (file serviti da `web/`). |
| `GET /api/info` | Versione applicativa, broker/topic, dimensione buffer. |
| `GET /api/events` | Cronologia paginata (vedi sotto). |
| `GET /api/events/all` | Intera cronologia, dalla più recente: `{"events":[...],"total":N}`. Pensato per tool esterni. |
| `GET /api/status` | Stato MQTT: `connected`, `last_connected`, `last_error`. |
| `GET /stream` | Stream SSE: un messaggio per ogni nuovo evento. |

### Paginazione di `/api/events`

`per_page` vale `20` di default, massimo `50`.

**Con SQLite attivo** (paginazione keyset):

- Parametri: `before_id`, `after_id`, `from_ts`, `to_ts`.
- Risposta:
  ```json
  {"events":[...],"per_page":20,"has_newer":false,"has_older":true,
   "first_id":123,"last_id":104,"prev_after_id":0,"next_before_id":104,"total":999}
  ```
  Per la pagina successiva (più vecchia) passa `before_id=next_before_id`; per quella precedente `after_id=prev_after_id`.

**Senza SQLite** (paginazione a pagine sul buffer in memoria):

- Parametro: `page` (da 1).
- Risposta: `{"events":[...],"page":1,"per_page":20,"total":N}`.

## Persistenza SQLite

Con `EVENT_DB_ENABLED=1` il viewer apre (o crea) il database indicato da `EVENT_DB_PATH` e accoda ogni messaggio MQTT nella tabella `events`:

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    event_json TEXT NOT NULL
);
```

- Quando il numero di righe supera `EVENT_DB_MAX_EVENTS`, vengono eliminate in blocco le `EVENT_DB_PURGE_BATCH` più vecchie. Imposta `EVENT_DB_MAX_EVENTS=0` per una cronologia illimitata.
- All'avvio vengono precaricati nel buffer in memoria fino a `EVENT_BUFFER_SIZE` eventi recenti, così la dashboard mostra subito lo storico.
- `/api/events` e `/api/events/all` leggono dal database; senza persistenza usano il buffer in memoria (che si azzera al riavvio).
- Lo stream SSE gestisce solo gli aggiornamenti live. Per evitare blocchi alla riconnessione del browser, a un nuovo client vengono inviati al massimo `SSE_CACHE_LIMIT` eventi.

## Versioning

- La versione corrente è definita in [`version.h`](version.h) (`EVENT_VIEWER_VERSION`) ed è esposta nel campo `version` di `GET /api/info` e nel badge della dashboard.
- Ogni rilascio aggiorna `version.h` (SemVer) e [`CHANGELOG.md`](CHANGELOG.md).

## Utility

- `sample-events.jsonl` — 6 eventi d'esempio (un ciclo di test completo: `LSTART`, `LMREQ`, tre `FMEAS` con blocchi di misura, `LEND`), uno per riga.
- `publish_samples.py` — ripubblica i campioni su un broker (default `broker.hivemq.com`, topic `public/test/topic`) con ritmo configurabile (`--delay 4`) e ripartenza da un ID specifico (`--start-id 1911110500`).
- `publish_single_event.py` — pubblica un singolo evento di test.

## Licenza

Copyright (C) 2025-2026 Olix.

Il progetto ha due licenze distinte:

| Parte | Licenza |
| --- | --- |
| Server (sorgenti C, script, build) | **GNU General Public License v2.0** (`GPL-2.0-only`), vedi [`LICENSE`](LICENSE). La licenza è imposta da Mongoose, disponibile solo con GPL-2.0 o con licenza commerciale. |
| Interfaccia web d'esempio e documentazione (`web/`) | **MIT**, vedi [`web/LICENSE`](web/LICENSE). |

Server e interfaccia web sono programmi separati che comunicano solo via HTTP/JSON. Puoi quindi sostituire `web/` con un'interfaccia tua, anche con una licenza diversa o proprietaria; l'esempio è MIT proprio per poterlo usare come punto di partenza.

Le licenze dei componenti di terze parti (Mongoose, cJSON, SQLite) sono riportate in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

Lo zip di release deve contenere:
- `viewer.exe`;
- la cartella `web/`, che il server legge a runtime;
- `LICENSE`, `web/LICENSE` e `THIRD_PARTY_NOTICES.md`.

Per la parte GPL va inoltre reso disponibile il sorgente corrispondente al binario, per esempio con il link al repository e al tag della release. Non distribuire build linkate con OpenSSL 3 (`./configure --with-ssl`): la sua licenza non è compatibile con la GPL-2.0.
