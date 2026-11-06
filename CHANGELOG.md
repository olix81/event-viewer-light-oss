# Changelog

Tutte le modifiche rilevanti a questo progetto sono tracciate in questo file.

Il formato segue [Keep a Changelog](https://keepachangelog.com/it-IT/1.1.0/) e il versionamento usa [Semantic Versioning](https://semver.org/lang/it/).

## [Unreleased]
### Added
- Licenza GPL-2.0 (`LICENSE`) per il server e `THIRD_PARTY_NOTICES.md` con le licenze di Mongoose, cJSON e SQLite.
- Interfaccia web d'esempio minimale (`web/index.html`, licenza MIT) e documentazione API in HTML (`web/docs.html`).
- `.env.example` con la configurazione di default.
- Intestazioni `SPDX-License-Identifier` nei sorgenti del progetto.

### Changed
- `configure` non linka più OpenSSL per default (`--with-ssl` per abilitarlo): OpenSSL 3 non è compatibile con la GPL-2.0 e il viewer non usa TLS.
- Versione in `configure.ac` allineata a 0.2.0; segnalazione bug sulle issue GitHub.
- README riscritto e allineato all'uso di Mongoose per HTTP e MQTT.

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
