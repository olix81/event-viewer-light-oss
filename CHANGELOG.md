# Changelog

Tutte le modifiche rilevanti a questo progetto sono tracciate in questo file.

Il formato segue [Keep a Changelog](https://keepachangelog.com/it-IT/1.1.0/) e il versionamento usa [Semantic Versioning](https://semver.org/lang/it/).

## [Unreleased]
### Added
- Licenza GPL-2.0 (`LICENSE`) per il server e `THIRD_PARTY_NOTICES.md` con le licenze di Mongoose, cJSON e SQLite.
- Interfaccia web d'esempio minimale (`web/index.html`, licenza MIT) e documentazione API in HTML (`web/docs.html`).
- `.env.example` con la configurazione di default.
- Parametro `replay` su `GET /stream` (`?replay=0` salta il reinvio degli eventi in cache, `?replay=N` lo limita a N).
- Intestazioni `SPDX-License-Identifier` nei sorgenti del progetto.

### Changed
- `configure` non linka più OpenSSL per default (`--with-ssl` per abilitarlo): OpenSSL 3 non è compatibile con la GPL-2.0 e il viewer non usa TLS.
- Versione in `configure.ac` allineata a 0.2.0; segnalazione bug sulle issue GitHub.
- README riscritto e allineato all'uso di Mongoose per HTTP e MQTT.
- SQLite in modalità WAL con `synchronous=NORMAL`: niente fsync a ogni insert e letture HTTP non bloccate dalle scritture MQTT. Rimosso l'indice inutilizzato `idx_events_received_at`.
- Gli eventi SSE vengono inoltrati subito (`mg_wakeup`) invece di attendere fino a 100 ms il timeout del loop HTTP.
- Coda SSE limitata a 1024 eventi/8 MiB e output in attesa per client a 4 MiB; in caso di sovraccarico gli stream vengono chiusi per riconnessione e replay. Il loop HTTP attende 1 ms con arretrato per evitare busy-spin.
- La UI d'esempio usa il replay SSE predefinito per recuperare gli eventi recenti dopo una riconnessione, deduplicando quelli già caricati dalla cronologia.
- La cartella dei file statici è configurabile con `WEB_ROOT`; il default resta `web/`.

### Fixed
- `GET /api/events?after_id=N` restituiva gli eventi più recenti in assoluto invece della pagina adiacente più nuova di `N`. Inoltre `has_newer`/`prev_after_id` erano sempre `false`/`0` navigando con `before_id`, e `has_older` era errato con `after_id`.
- La UI d'esempio mostrava gli eventi del replay SSE sopra quelli più recenti e, al superamento di 200 righe, scartava proprio gli ultimi eventi.
- `publish_samples.py` poteva terminare prima che il loop di rete MQTT avesse completato l'invio; ora verifica ogni pubblicazione e segnala eventuali errori o timeout.

## [0.2.0] - 2026-03-15
### Added
- Versione applicativa esplicita in `version.h` (`EVENT_VIEWER_VERSION`).
- Campo `version` nell'endpoint `GET /api/info`.
- Badge "Versione" nella dashboard web.
- Changelog ufficiale per tracciare le release.

### Changed
- Ottimizzazioni CPU lato frontend con throttling del refresh vista, cache timestamp eventi e riduzione dei ricalcoli filtro/tipi.
- Ottimizzazioni lato backend su query SQLite (evita `json_extract` quando non serve) e rate-limit del warning SSE senza client.

## [0.1.0] - 2026-02-02
### Added
- Prima release stabile del viewer C con MQTT, API HTTP, dashboard web e stream SSE.
- Supporto persistenza SQLite e paginazione cronologia.
