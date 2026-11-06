## Project snapshot

This is a compact C application built with Mongoose and cJSON that bridges an MQTT broker to an in-memory circular event buffer and exposes a web UI + SSE stream for live events.

Key files
- `main.c` — entire application (MQTT client, event store, HTTP/SSE server, event JSON serialization).
- `mongoose.c` / `mongoose.h` — embedded networking library (HTTP, MQTT, WebSocket).
- `cJSON.c` / `cJSON.h` — embedded JSON library for event parsing and serialization.
- `.env` — configuration file for MQTT broker, topic, buffer size, HTTP port.

Quick context for an AI coding assistant
- Purpose: connect to an MQTT broker (supports `mqtt://`, `mqtts://`, `tls://`, `ws://`), collect recent messages in a fixed-size circular buffer, and present them via HTTP (index.html, /api/events, /api/status, /api/info, /stream SSE endpoint).
- Runtime: single-process C binary; compiles with `mingw32-make` (or gcc) and runs directly with no external dependencies.

How to run (developer / CI)
- Build from source using the Makefile:
  - mingw32-make (or make on Linux)
  - Output: `viewer.exe` (Windows) or `viewer` (Unix-like).
- Configure via `.env` file or environment variables, then run:
  - Windows PowerShell: `.\viewer.exe`
  - Unix: `./viewer`

Important environment variables (loaded from `.env` or shell)
- `MQTT_BROKER_HOST` (default: `broker.hivemq.com`; accepts `mqtt://`, `tls://`, `ws://` schemes)
- `MQTT_BROKER_PORT` (default: `1883`)
- `MQTT_TOPIC` (default: `public/test/topic`)
- `MQTT_USERNAME`, `MQTT_PASSWORD` (optional; blank by default)
- `MQTT_CLIENT_ID` (auto-generated if blank)
- `MQTT_KEEPALIVE` (default: `60` seconds)
- `EVENT_BUFFER_SIZE` (default: `200`; circular deque max events)
- `PORT` (HTTP listen port; default `5000`)

Architecture & patterns to preserve
- Single-file codebase: `main.c` contains all business logic (event store, MQTT handler, HTTP routes, SSE broadcast). Minimize external dependencies to keep the binary small and portable.
- Circular event buffer: `struct event_store` uses a fixed-size array with head index to efficiently recycle memory. Do not convert to dynamic allocation without profiling — circular buffers are designed for low-latency event systems.
- MQTT URL handling: the program now detects schemes in `MQTT_BROKER_HOST` (e.g., `tls://broker.hivemq.com`) and avoids double-prefixing. Keep this logic intact when modifying broker configuration.
- SSE broadcast: `broadcast_sse()` walks the client linked-list and sends formatted JSON to each active connection. The HTTP handler adds clients on `/stream` request and removes them on MG_EV_CLOSE; preserve this lifecycle for real-time updates.
- Event JSON schema: always include `timestamp` (ISO 8601), `topic`, `qos`, `retain`, `payload` (text or hex), and `payload_summary` (parsed JSON or text marker). Preserve these keys for UI and API compatibility.

Testing & debugging notes
- No unit tests in the repo. Manual verification:
  - Use a public MQTT broker (broker.hivemq.com) or run local mosquitto: `mosquitto -p 1883`.
  - Publish test messages: `mosquitto_pub -h localhost -t public/test/topic -m '{"src":"test","id":1,"type":"alert","desc":"Test event"}'`.
  - Open browser to `http://localhost:5050/` (or configured PORT).
  - Monitor `/stream` in a separate tab or curl: `curl http://localhost:5050/stream`.
- To debug: compile with debug symbols (add `-g` to Makefile CFLAGS) and use gdb: `gdb ./viewer` (Windows: use gdb from MSYS2/MinGW or VS debugger).
- Enable verbose logging: pass `-v` flag to Mongoose (if supported in your build) or add `printf` statements in `mqtt_handler()` and `http_handler()` callbacks.

Integration and extension points
- Replace circular buffer with persistent storage: modify `struct event_store` and `event_store_add()` to write to SQLite or file, but keep the `snapshot()` contract so HTTP `/api/events` continues to work.
- Add authentication: modify `http_handler()` to validate Authorization headers before serving non-public routes.
- Extend event parsing: edit `create_event_json()` to extract domain-specific fields from MQTT payloads (similar to how `src`, `id`, `type`, `desc` are already parsed).

Project-specific conventions
- Memory management: use `malloc/free` consistently. Mongoose and cJSON manage their own internal memory; do not mix manual and library-managed pointers.
- Configuration: read `.env` once at startup via `load_env_file()`. Avoid runtime reconfiguration.
- Logging: use `printf()` for informational output (visible in console). Mongoose uses internal logging; control via Mongoose MG_LL_* levels if needed.
- String buffers: use fixed-size char arrays (snprintf) for safety; e.g., `char mqtt_url[512]` prevents buffer overflow.

Files to inspect when making changes
- `main.c` — all application logic (handlers, event store, configuration).
- `mongoose.h` — for event types (MG_EV_*) and API (mg_http_listen, mg_mqtt_connect, etc.).
- `cJSON.h` — for JSON creation/parsing macros (cJSON_CreateObject, cJSON_Parse, etc.).
- `.env` — runtime configuration.
- `Makefile` — build rules and compiler flags.

If you make functional changes
- Add a brief comment in `main.c` near the changed function explaining the modification and rationale (helps future agents).
- Test the HTTP routes via `curl` or browser and the `/stream` endpoint via `curl -N` (disable buffering) to verify SSE delivery.
- Preserve backward-compatible event JSON schema: always emit `timestamp`, `topic`, `qos`, `retain`, `payload`, `payload_summary`.

When unsure, ask the user for:
- exact MQTT broker connection details (host, port, scheme, credentials, topic filter).
- whether adding persistence (file, DB) or high-throughput optimizations is desired.
- target deployment environment (Windows binary, Linux, embedded, container).
- whether a C library refactor or splitting into separate .c files is acceptable (impacts build complexity).

Recent changes
- Fixed MQTT URL construction to handle broker hosts with schemes (e.g., `tls://broker.hivemq.com`). The program now detects existing schemes and avoids double-prefixing.
- Added `printf("Connecting to MQTT URL: %s\n", mqtt_url);` for runtime diagnostics.

End — please report any issues with the app's startup, connectivity, or HTTP routes, or ask for additional enhancements.
