/* SPDX-License-Identifier: MIT
   Copyright (C) 2025-2026 Olix */
/* Esempio minimale di client per MQTT Event Viewer.
   - /api/info    metadati (versione, broker, topic)
   - /api/status  stato della connessione MQTT
   - /api/events  ultimi eventi (cronologia)
   - /stream      Server-Sent Events con gli eventi live */
(function () {
  "use strict";

  const MAX_ROWS = 200;      // righe massime mostrate in tabella
  const HISTORY_SIZE = 20;   // eventi caricati all'avvio
  const STATUS_POLL_MS = 5000;

  const tbody = document.getElementById("events");
  const countLabel = document.getElementById("event-count");
  const seen = new Set();    // chiavi degli eventi gia' mostrati
  let rows = 0;

  /* All'apertura, /stream reinvia (dal piu' vecchio al piu' recente) fino a
     SSE_CACHE_LIMIT eventi gia' in memoria: possono coincidere con quelli della
     cronologia, quindi li riconosciamo con una chiave e li scartiamo. */
  function eventKey(evt) {
    const d = evt.event_data || {};
    return [evt.timestamp, d.src, d.type, d.id, d.timestamp].join("|");
  }

  function isNew(evt) {
    const key = eventKey(evt);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }

  function setText(id, text) {
    document.getElementById(id).textContent = text;
  }

  function setBadge(id, text, kind) {
    const el = document.getElementById(id);
    el.textContent = text;
    el.className = "badge badge-" + kind;
  }

  async function getJson(path) {
    const res = await fetch(path, { cache: "no-store" });
    if (!res.ok) throw new Error(path + ": HTTP " + res.status);
    return res.json();
  }

  /* Un evento ha la forma {"timestamp": "<ricezione ISO>", "event_data": {...}}.
     event_data manca se il payload MQTT non era JSON valido. */
  function eventRow(evt) {
    const data = evt.event_data || {};
    const tr = document.createElement("tr");
    const cells = [
      data.timestamp_iso || evt.timestamp || "",
      data.src || "",
      data.type || "",
      data.id !== undefined ? String(data.id) : "",
      data.desc || ""
    ];
    for (const value of cells) {
      const td = document.createElement("td");
      td.textContent = value; // textContent: nessun rischio di HTML injection
      tr.appendChild(td);
    }
    if (data.type) tr.dataset.type = data.type;
    return tr;
  }

  function updateCount() {
    countLabel.textContent = rows + (rows === 1 ? " evento" : " eventi");
  }

  function clearPlaceholder() {
    const empty = tbody.querySelector("tr.empty");
    if (empty) empty.remove();
  }

  function appendEvent(evt) {          // in fondo (cronologia, gia' ordinata)
    if (!isNew(evt)) return;
    clearPlaceholder();
    tbody.appendChild(eventRow(evt));
    rows++;
    updateCount();
  }

  function prependEvent(evt) {         // in cima (evento live)
    if (!isNew(evt)) return;
    clearPlaceholder();
    const tr = eventRow(evt);
    tr.classList.add("fresh");
    tbody.insertBefore(tr, tbody.firstChild);
    rows++;
    while (rows > MAX_ROWS && tbody.lastChild) {
      tbody.removeChild(tbody.lastChild);
      rows--;
    }
    updateCount();
  }

  async function loadInfo() {
    try {
      const info = await getJson("/api/info");
      setText("info-version", info.version || "—");
      if (info.mqtt) {
        setText("info-broker", info.mqtt.host + ":" + info.mqtt.port + " — " + info.mqtt.topic);
      }
    } catch (err) {
      console.error(err);
    }
  }

  async function loadStatus() {
    try {
      const st = await getJson("/api/status");
      if (st.connected) setBadge("mqtt-status", "connesso", "ok");
      else setBadge("mqtt-status", st.last_error || "disconnesso", "error");
    } catch (err) {
      setBadge("mqtt-status", "server non raggiungibile", "error");
    }
  }

  async function loadHistory() {
    try {
      const page = await getJson("/api/events?per_page=" + HISTORY_SIZE);
      (page.events || []).forEach(appendEvent); // gia' dal piu' recente
    } catch (err) {
      console.error(err);
    }
  }

  function openStream() {
    const source = new EventSource("/stream");
    source.onopen = function () { setBadge("sse-status", "attivo", "ok"); };
    source.onerror = function () { setBadge("sse-status", "riconnessione...", "warn"); };
    source.onmessage = function (msg) {
      try {
        prependEvent(JSON.parse(msg.data));
      } catch (err) {
        console.warn("Evento SSE non valido", err);
      }
    };
  }

  loadInfo();
  loadStatus();
  setInterval(loadStatus, STATUS_POLL_MS);
  loadHistory().then(openStream);
})();
