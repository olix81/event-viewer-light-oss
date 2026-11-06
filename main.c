/* SPDX-License-Identifier: GPL-2.0-only
   Copyright (C) 2025-2026 Olix */
/* main.c -- MQTT event viewer exposing HTTP APIs and SSE streaming.  This
   file wires together configuration, persistence, MQTT ingestion, and the
   embedded HTTP server that serves the dashboard.  */
#define _CRT_SECURE_NO_WARNINGS

#include "mongoose.h"
#include "cJSON.h"
#include "version.h"

#if defined(__has_include)
#if __has_include("sqlite3.h")
#define SQLITE3_HEADER_AVAILABLE 1 
#else /* no sqlite3.h header */
#define SQLITE3_HEADER_AVAILABLE 0
#endif /* __has_include("sqlite3.h") */
#else /* !defined(__has_include) */
#define SQLITE3_HEADER_AVAILABLE 1
#endif /* defined(__has_include) */

#if defined(USE_SQLITE) && USE_SQLITE && SQLITE3_HEADER_AVAILABLE
#define HAVE_SQLITE3 1
#else /* SQLite disabled or missing headers */
#define HAVE_SQLITE3 0
#endif /* defined(USE_SQLITE) && USE_SQLITE && SQLITE3_HEADER_AVAILABLE */

#if HAVE_SQLITE3
#include "sqlite3.h"
#ifndef SQLITE_TRANSIENT
#define SQLITE_TRANSIENT ((sqlite3_destructor_type)-1)
#endif /* !defined(SQLITE_TRANSIENT) */
#endif /* HAVE_SQLITE3 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#ifdef _WIN32
#include <process.h>
#include <windows.h>
#else /* !_WIN32 */
#include <pthread.h>
#include <unistd.h>
#endif /* !_WIN32 */

#ifndef APP_VERSION
#ifdef EVENT_VIEWER_VERSION
#define APP_VERSION EVENT_VIEWER_VERSION
#elif defined(PACKAGE_VERSION)
#define APP_VERSION PACKAGE_VERSION
#else
#define APP_VERSION "dev"
#endif
#endif /* !APP_VERSION */

#define STARTUP_STEP_COUNT 4

enum log_level {
  LOG_LEVEL_OFF = -1,
  LOG_LEVEL_ERROR = 0,
  LOG_LEVEL_WARN = 1,
  LOG_LEVEL_INFO = 2,
  LOG_LEVEL_DEBUG = 3,
};

/* Global minimum log level.  Messages below this threshold are skipped by app_log.  */
static enum log_level g_log_level = LOG_LEVEL_INFO;
static void app_log(enum log_level level, const char *tag, const char *fmt, ...);

/* Compare the strings a and b ignoring case.  Either pointer may be NULL; the
   return value is nonzero only when both are non-NULL and equal ignoring case.  */
static int str_case_equal(const char *a, const char *b) {
  if (!a || !b) return 0;
  while (*a && *b) {
    if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return 0;
    a++;
    b++;
  }
  return *a == '\0' && *b == '\0';
}

/* Produce the printable mnemonic for level.  The string is one of the expected
   tokens (ERROR, WARN, INFO, DEBUG) and defaults to "OFF" for unknown levels.  */
static const char *log_level_name(enum log_level level) {
  switch (level) {
    case LOG_LEVEL_ERROR:
      return "ERROR";
    case LOG_LEVEL_WARN:
      return "WARN";
    case LOG_LEVEL_INFO:
      return "INFO";
    case LOG_LEVEL_DEBUG:
      return "DEBUG";
    default:
      return "OFF";
  }
}

/* Parse the textual LOG_LEVEL setting held in value.  Leading whitespace and
   synonyms are accepted, and the return value is clamped to the enum range.  */
static enum log_level parse_log_level(const char *value) {
  if (!value || !*value) return LOG_LEVEL_INFO;
  while (*value == ' ' || *value == '\t') value++;
  if (*value == '\0') return LOG_LEVEL_INFO;
  if (str_case_equal(value, "OFF") || str_case_equal(value, "NONE")) return LOG_LEVEL_OFF;
  if (str_case_equal(value, "ERROR")) return LOG_LEVEL_ERROR;
  if (str_case_equal(value, "WARN") || str_case_equal(value, "WARNING")) return LOG_LEVEL_WARN;
  if (str_case_equal(value, "INFO")) return LOG_LEVEL_INFO;
  if (str_case_equal(value, "DEBUG") || str_case_equal(value, "TRACE")) return LOG_LEVEL_DEBUG;
  if (isdigit((unsigned char)value[0]) || value[0] == '-') {
    int parsed = atoi(value);
    if (parsed < (int)LOG_LEVEL_OFF) return LOG_LEVEL_OFF;
    if (parsed > (int)LOG_LEVEL_DEBUG) return LOG_LEVEL_DEBUG;
    return (enum log_level)parsed;
  }
  return LOG_LEVEL_INFO;
}

#ifdef _WIN32
#include <direct.h>
#define SETENV(k, v) _putenv_s((k), (v))
#else /* !_WIN32 */
#define SETENV(k, v) setenv((k), (v), 1)
#endif /* !_WIN32 */

#define DEFAULT_BUFFER_SIZE 512
#define DEFAULT_HTTP_PORT 5000
#define EVENTS_DEFAULT_PER_PAGE 20
#define EVENTS_MAX_PER_PAGE 50
#define DEFAULT_SSE_CACHE_LIMIT 200

struct config {
  char broker_host[256];
  int broker_port;
  char topic[256];
  char username[128];
  char password[128];
  char client_id[128];
  int keepalive;
  int event_buffer;
  int http_port;
  char http_host[64];
  char db_path[260];
  int db_enabled;
  int db_max_events;
  int db_purge_batch;
  int sse_cache_limit;
  enum log_level log_level;
};

struct mqtt_status {
  int connected;
  char last_connected[64];
  char last_error[256];
};

struct event_record {
  char *json;
};

struct event_store {
  struct event_record *items;
  size_t max;
  size_t count;
  size_t head;
};

#if HAVE_SQLITE3
struct persistence {
  sqlite3 *db;
  sqlite3_stmt *insert_stmt;
  int enabled;
  char path[260];
  size_t max_events;
  size_t purge_batch;
};
#if HAVE_SQLITE3
struct events_page {
  char **rows;
  sqlite3_int64 *ids;
  sqlite3_int64 *timestamps;
  size_t count;
  int trimmed_extra;
};
/* Release heap memory tracked by page for fetched SQLite rows.  Argument page
   may be NULL; on return the pointed struct is reset to zeros.  */
static void events_page_free(struct events_page *page) {
  if (!page) return;
  if (page->rows) {
    for (size_t i = 0; i < page->count; ++i) free(page->rows[i]);
    free(page->rows);
  }
  free(page->ids);
  free(page->timestamps);
  memset(page, 0, sizeof(*page));
}
#endif /* HAVE_SQLITE3 */
#else /* !HAVE_SQLITE3 */
struct persistence {
  int enabled;
  char path[260];
  size_t max_events;
  size_t purge_batch;
};
#endif /* !HAVE_SQLITE3 */

struct sse_client {
  struct mg_connection *conn;
  struct sse_client *next;
};

struct sse_pending {
  char *json;
  struct sse_pending *next;
};

struct app_mutex {
#ifdef _WIN32
  CRITICAL_SECTION cs;
#else /* !_WIN32 */
  pthread_mutex_t handle;
#endif /* !_WIN32 */
};

/* Initialize the cross-platform mutex stored in m.  Passing NULL does nothing
   so callers may invoke this even when the mutex is optional.  */
static void app_mutex_init(struct app_mutex *m) {
  if (!m) return;
#ifdef _WIN32
  InitializeCriticalSection(&m->cs);
#else /* !_WIN32 */
  pthread_mutex_init(&m->handle, NULL);
#endif /* !_WIN32 */
}

/* Take the mutex stored in m if it was initialized.  This function blocks
   until the lock is acquired and returns immediately otherwise.  */
static void app_mutex_lock(struct app_mutex *m) {
  if (!m) return;
#ifdef _WIN32
  EnterCriticalSection(&m->cs);
#else /* !_WIN32 */
  pthread_mutex_lock(&m->handle);
#endif /* !_WIN32 */
}

/* Release the mutex stored in m if it was initialized.  Calling it with NULL
   is safe and simply results in a no-op.  */
static void app_mutex_unlock(struct app_mutex *m) {
  if (!m) return;
#ifdef _WIN32
  LeaveCriticalSection(&m->cs);
#else /* !_WIN32 */
  pthread_mutex_unlock(&m->handle);
#endif /* !_WIN32 */
}

/* Destroy the mutex stored in m and free any OS resources.  NULL arguments are
   ignored so teardown paths can be simplified.  */
static void app_mutex_free(struct app_mutex *m) {
  if (!m) return;
#ifdef _WIN32
  DeleteCriticalSection(&m->cs);
#else /* !_WIN32 */
  pthread_mutex_destroy(&m->handle);
#endif /* !_WIN32 */
}

static char *duplicate_json_string(const char *text);


struct app_state {
  struct config cfg;
  struct event_store store;
  struct mqtt_status status;
  struct sse_client *sse_clients;
  struct sse_pending *sse_head;
  struct sse_pending *sse_tail;
  struct app_mutex data_lock;
  struct app_mutex status_lock;
  struct app_mutex sse_queue_lock;
  struct mg_connection *mqtt_client;
  struct mg_mgr *http_mgr;           /* woken via mg_wakeup() by the MQTT thread */
  unsigned long http_listener_id;    /* target connection for mg_wakeup() */
  struct persistence persistence;
  size_t total_events;
  sqlite3_int64 max_db_id;
  sqlite3_int64 min_db_id;
};

static void broadcast_sse(struct app_state *state, const char *json);

/* Remove whitespace padding at the beginning and end of str in-place.  The
   buffer must be mutable and NUL terminated; empty strings are accepted.  */
static void trim(char *str) {
  char *end;
  while (*str == ' ' || *str == '\t' || *str == '\r' || *str == '\n') str++;
  if (*str == 0) return;
  end = str + strlen(str) - 1;
  while (end > str && (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n'))
    *end-- = '\0';
}

/* Load KEY=VALUE pairs from path and populate the process environment.  Lines
   starting with '#' or blank are ignored, and errors while opening are silent.  */
static void load_env_file(const char *path) {
  FILE *fp = fopen(path, "r");
  if (!fp) return;
  char line[512];
  while (fgets(line, sizeof(line), fp)) {
    char *start = line;
    trim(start);
    if (*start == '#' || *start == 0) continue;
    char *eq = strchr(start, '=');
    if (!eq) continue;
    *eq = '\0';
    char *key = start;
    char *value = eq + 1;
    trim(key);
    trim(value);
    if (*key == 0) continue;
    SETENV(key, value);
  }
  fclose(fp);
}

/* Format ts (seconds since epoch) as an ISO-8601 UTC string written into buf.
   The buffer size is given by buf_size and the string is always NUL terminated.  */
static void format_iso(time_t ts, char *buf, size_t buf_size) {
  struct tm t;
#ifdef _WIN32
  gmtime_s(&t, &ts);
#else /* !_WIN32 */
  gmtime_r(&ts, &t);
#endif /* !_WIN32 */
  strftime(buf, buf_size, "%Y-%m-%dT%H:%M:%SZ", &t);
}

/* Print a structured log message at level with a textual tag and printf-style
   format.  Messages below g_log_level are dropped; the function returns void.  */
static void app_log(enum log_level level, const char *tag, const char *fmt, ...) {
  if (level > g_log_level) return;
  char ts[32];
  format_iso(time(NULL), ts, sizeof ts);
  char msg[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(msg, sizeof msg, fmt, ap);
  va_end(ap);
  printf("[%s][%s][%s] %s\n", ts, log_level_name(level), tag, msg);
  fflush(stdout);
}

/* Return the environment variable for key or def when unset/empty.  When
   overridden is non-NULL it is set to 1 only when the env var is non-empty and
   differs from def (case-sensitive).  The caller must not free the returned
   pointer.  */
static const char *get_env(const char *key, const char *def, int *overridden) {
  if (overridden) *overridden = 0;
  const char *value = getenv(key);
  if (value && *value) {
    if (overridden && (!def || strcmp(value, def) != 0)) {
      *overridden = 1;
    }
    return value;
  }
  return def;
}

/* Emit a log message showing an overridden string env var.  Intended for
   configuration keys so callers can see which default was replaced at startup. */
static void log_config_override_str(const char *key, const char *value, int secret) {
  const char *printed = "(empty)";
  if (value && *value) printed = value;
  if (secret) printed = "<hidden>";
  app_log(LOG_LEVEL_INFO, "CFG", "Override %s=%s", key ? key : "(null)", printed);
}

/* Emit a log message showing an overridden numeric env var.  */
static void log_config_override_int(const char *key, int value) {
  app_log(LOG_LEVEL_INFO, "CFG", "Override %s=%d", key ? key : "(null)", value);
}

/* Unified helper to print startup steps so logs have a consistent prefix.  */
static void log_startup_step(int step, const char *description) {
  app_log(LOG_LEVEL_INFO, "APP", "Step avvio %d/%d: %s", step, STARTUP_STEP_COUNT,
          description ? description : "");
}

/* Acquire the app_state data_lock if state is non-NULL.  This guards accesses
   to the event store and counters.  */
static void state_data_lock(struct app_state *state) {
  if (state) app_mutex_lock(&state->data_lock);
}

/* Release the app_state data_lock if state is non-NULL.  Pair with
   state_data_lock around protected sections.  */
static void state_data_unlock(struct app_state *state) {
  if (state) app_mutex_unlock(&state->data_lock);
}

/* Acquire the mutex protecting the MQTT status snapshot of state.  NULL state
   arguments are ignored so callers can remain simple.  */
static void status_lock(struct app_state *state) {
  if (state) app_mutex_lock(&state->status_lock);
}

/* Release the mutex protecting the MQTT status snapshot.  Callers should match
   each status_lock with a status_unlock.  */
static void status_unlock(struct app_state *state) {
  if (state) app_mutex_unlock(&state->status_lock);
}

/* Extract a query parameter named name from hm and parse it as a signed 64-bit
   integer.  Returns default_value when the parameter is missing or invalid.  */
static sqlite3_int64 query_param_i64(struct mg_http_message *hm, const char *name,
                                     sqlite3_int64 default_value) {
  if (!hm || !name) return default_value;
  char buf[64];
  int len = mg_http_get_var(&hm->query, name, buf, sizeof(buf));
  if (len <= 0) return default_value;
#ifdef _WIN32
  return _strtoi64(buf, NULL, 10);
#else /* !_WIN32 */
  return strtoll(buf, NULL, 10);
#endif /* !_WIN32 */
}

/* Populate cfg with configuration values sourced from environment variables or
   defaults.  This routine also clamps buffer sizes and log levels to safe
   ranges.  */
static void config_load(struct config *cfg) {
  const char *env = NULL;
  int overridden = 0;
  env = get_env("MQTT_BROKER_HOST", "broker.hivemq.com", &overridden);
  snprintf(cfg->broker_host, sizeof(cfg->broker_host), "%s", env);
  if (overridden) log_config_override_str("MQTT_BROKER_HOST", cfg->broker_host, 0);
  env = get_env("MQTT_BROKER_PORT", "1883", &overridden);
  cfg->broker_port = atoi(env);
  if (overridden) log_config_override_int("MQTT_BROKER_PORT", cfg->broker_port);
  env = get_env("MQTT_TOPIC", "public/test/topic", &overridden);
  snprintf(cfg->topic, sizeof(cfg->topic), "%s", env);
  if (overridden) log_config_override_str("MQTT_TOPIC", cfg->topic, 0);
  env = get_env("MQTT_USERNAME", "", &overridden);
  snprintf(cfg->username, sizeof(cfg->username), "%s", env);
  if (overridden) log_config_override_str("MQTT_USERNAME", cfg->username, 0);
  env = get_env("MQTT_PASSWORD", "", &overridden);
  snprintf(cfg->password, sizeof(cfg->password), "%s", env);
  if (overridden) log_config_override_str("MQTT_PASSWORD", cfg->password, 1);
  env = get_env("MQTT_CLIENT_ID", "", &overridden);
  snprintf(cfg->client_id, sizeof(cfg->client_id), "%s", env);
  if (overridden) log_config_override_str("MQTT_CLIENT_ID", cfg->client_id, 0);

  env = get_env("MQTT_KEEPALIVE", "60", &overridden);
  cfg->keepalive = atoi(env);
  if (overridden) log_config_override_int("MQTT_KEEPALIVE", cfg->keepalive);

  env = get_env("EVENT_BUFFER_SIZE", "8192", &overridden);
  cfg->event_buffer = atoi(env);
  if (cfg->event_buffer <= 0) cfg->event_buffer = DEFAULT_BUFFER_SIZE;
  if (overridden) log_config_override_int("EVENT_BUFFER_SIZE", cfg->event_buffer);

  env = get_env("HTTP_HOST", "localhost", &overridden);
  snprintf(cfg->http_host, sizeof(cfg->http_host), "%s", env);
  if (overridden) log_config_override_str("HTTP_HOST", cfg->http_host, 0);
  env = get_env("PORT", "5000", &overridden);
  cfg->http_port = atoi(env);
  if (cfg->http_port <= 0) cfg->http_port = DEFAULT_HTTP_PORT;
  if (overridden) log_config_override_int("PORT", cfg->http_port);

  env = get_env("EVENT_DB_PATH", "viewer-events.db", &overridden);
  snprintf(cfg->db_path, sizeof(cfg->db_path), "%s", env);
  if (overridden) log_config_override_str("EVENT_DB_PATH", cfg->db_path, 0);
  env = get_env("EVENT_DB_ENABLED", "1", &overridden);
  cfg->db_enabled = atoi(env) != 0;
  if (overridden) log_config_override_int("EVENT_DB_ENABLED", cfg->db_enabled ? 1 : 0);

  env = get_env("EVENT_DB_MAX_EVENTS", "5000", &overridden);
  cfg->db_max_events = atoi(env);
  if (cfg->db_max_events < 0) cfg->db_max_events = 0;
  if (overridden) log_config_override_int("EVENT_DB_MAX_EVENTS", cfg->db_max_events);

  env = get_env("EVENT_DB_PURGE_BATCH", "250", &overridden);
  cfg->db_purge_batch = atoi(env);
  if (cfg->db_purge_batch <= 0) cfg->db_purge_batch = 250;
  if (overridden) log_config_override_int("EVENT_DB_PURGE_BATCH", cfg->db_purge_batch);

  env = get_env("SSE_CACHE_LIMIT", "200", &overridden);
  cfg->sse_cache_limit = atoi(env);
  if (cfg->sse_cache_limit < 0) cfg->sse_cache_limit = DEFAULT_SSE_CACHE_LIMIT;
  if (overridden) log_config_override_int("SSE_CACHE_LIMIT", cfg->sse_cache_limit);

  env = get_env("LOG_LEVEL", "INFO", &overridden);
  cfg->log_level = parse_log_level(env);
  if (overridden) log_config_override_str("LOG_LEVEL", log_level_name(cfg->log_level), 0);
  g_log_level = cfg->log_level;
#if !HAVE_SQLITE3
  if (cfg->db_enabled) {
    app_log(LOG_LEVEL_WARN, "DB", "EVENT_DB_ENABLED impostato ma SQLite non è disponibile nella build corrente");
    cfg->db_enabled = 0;
  }
#endif /* !HAVE_SQLITE3 */
}

/* Load integer query argument name from hm with a fallback default_value.
   Nonexistent or malformed values leave the default untouched.  */
static int query_param_int(struct mg_http_message *hm, const char *name, int default_value) {
  if (!hm || !name) return default_value;
  char buf[32];
  int len = mg_http_get_var(&hm->query, name, buf, sizeof(buf));
  if (len <= 0) return default_value;
  return atoi(buf);
}

/* Initialize the in-memory ring buffer pointed by store with space for
   max_events records.  Existing allocations are not freed here.  */
static void event_store_init(struct event_store *store, size_t max_events) {
  store->items = (struct event_record *)calloc(max_events, sizeof(struct event_record));
  store->max = max_events;
  store->count = 0;
  store->head = 0;
}

/* Free every JSON string owned by store and release the array itself.  After
   this call store->items becomes NULL.  */
static void event_store_free(struct event_store *store) {
  if (!store->items) return;
  for (size_t i = 0; i < store->max; ++i) {
    free(store->items[i].json);
  }
  free(store->items);
  store->items = NULL;
}

/* Insert json_owned into the ring buffer, taking ownership of the memory.
   Returns the inserted pointer and overwrites the oldest entry when the buffer
   is full.  */
static const char *event_store_add(struct event_store *store, char *json_owned) {
  size_t idx = store->head;
  if (store->items[idx].json) {
    free(store->items[idx].json);
  }
  store->items[idx].json = json_owned;
  store->head = (idx + 1) % store->max;
  if (store->count < store->max) store->count++;
  return json_owned;
}

#if HAVE_SQLITE3
/* Release prepared statements and the SQLite handle referenced by persistent.
   The struct is left in a disabled state even when some fields are NULL.  */
static void persistence_close(struct persistence *persistent) {
  if (!persistent) return;
  if (persistent->insert_stmt) {
    sqlite3_finalize(persistent->insert_stmt);
    persistent->insert_stmt = NULL;
  }
  if (persistent->db) {
    sqlite3_close(persistent->db);
    persistent->db = NULL;
  }
  persistent->enabled = 0;
}

/* Update the cached total-events counters within state from the SQLite
   database.  When persistence is disabled, counts fall back to the in-memory
   ring buffer lengths.  */
static void persistence_snapshot(struct app_state *state) {
  if (!state) return;
  if (!state->persistence.enabled || !state->persistence.db) {
    state->total_events = state->store.count;
    state->max_db_id = 0;
    state->min_db_id = 0;
    return;
  }
  const char *sql = "SELECT COUNT(*), MIN(id), MAX(id) FROM events";
  sqlite3_stmt *stmt = NULL;
  size_t total = 0;
  sqlite3_int64 min_id = 0, max_id = 0;
  if (sqlite3_prepare_v2(state->persistence.db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      total = (size_t) sqlite3_column_int64(stmt, 0);
      min_id = sqlite3_column_int64(stmt, 1);
      max_id = sqlite3_column_int64(stmt, 2);
    }
  }
  if (stmt) sqlite3_finalize(stmt);
  state->total_events = total;
  state->min_db_id = min_id;
  state->max_db_id = max_id;
}

/* Initialize persistent using cfg, opening the SQLite file and creating schema
   objects.  Returns 0 on success, negative on fatal errors, and leaves the
   struct disabled when cfg->db_enabled is false.  */
static int persistence_init(struct persistence *persistent, const struct config *cfg) {
  if (!persistent) return -1;
  memset(persistent, 0, sizeof *persistent);
  if (!cfg || !cfg->db_enabled) return 0;
  persistent->max_events = cfg->db_max_events > 0 ? (size_t) cfg->db_max_events : 0;
  persistent->purge_batch = cfg->db_purge_batch > 0 ? (size_t) cfg->db_purge_batch : 0;
  if (cfg->db_path[0]) {
    snprintf(persistent->path, sizeof persistent->path, "%s", cfg->db_path);
  } else {
    snprintf(persistent->path, sizeof persistent->path, "%s", "viewer-events.db");
  }
  int rc = sqlite3_open(persistent->path, &persistent->db);
  if (rc != SQLITE_OK) {
    app_log(LOG_LEVEL_ERROR, "DB", "Impossibile aprire %s: %s", persistent->path,
            persistent->db ? sqlite3_errmsg(persistent->db) : "sqlite3_open failed");
    persistence_close(persistent);
    return -1;
  }
  sqlite3_busy_timeout(persistent->db, 2000);
  /* WAL lets the HTTP thread read while the MQTT thread writes, and
     synchronous=NORMAL drops the per-insert fsync (in WAL mode the database
     stays consistent; at worst the last commits are lost on power failure).  */
  char *errmsg = NULL;
  rc = sqlite3_exec(persistent->db,
                    "PRAGMA journal_mode=WAL;"
                    "PRAGMA synchronous=NORMAL;",
                    NULL, NULL, &errmsg);
  if (rc != SQLITE_OK) {
    app_log(LOG_LEVEL_WARN, "DB", "PRAGMA WAL non applicato: %s", errmsg ? errmsg : "sconosciuto");
    sqlite3_free(errmsg);
    errmsg = NULL;
  }
  /* idx_events_received_at was never used by any query and only slowed down
     inserts and purges: drop it from databases created by older versions.  */
  const char *schema =
      "CREATE TABLE IF NOT EXISTS events ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "received_at TEXT NOT NULL,"
        "event_json TEXT NOT NULL"
      ");"
      "DROP INDEX IF EXISTS idx_events_received_at;";
  rc = sqlite3_exec(persistent->db, schema, NULL, NULL, &errmsg);
  if (rc != SQLITE_OK) {
    app_log(LOG_LEVEL_ERROR, "DB", "Errore inizializzazione schema: %s", errmsg ? errmsg : "sconosciuto");
    sqlite3_free(errmsg);
    persistence_close(persistent);
    return -1;
  }
  const char *insert_sql =
      "INSERT INTO events (received_at, event_json) VALUES (?1, ?2);";
  rc = sqlite3_prepare_v2(persistent->db, insert_sql, -1, &persistent->insert_stmt, NULL);
  if (rc != SQLITE_OK) {
    app_log(LOG_LEVEL_ERROR, "DB", "Errore prepare insert: %s", sqlite3_errmsg(persistent->db));
    persistence_close(persistent);
    return -1;
  }
  persistent->enabled = 1;
  app_log(LOG_LEVEL_INFO, "DB", "Persistenza attiva su %s", persistent->path);
  return 0;
}

/* Store the event_json payload into the events table.  Returns the SQLite
   rowid of the inserted row or 0 on error.  */
static sqlite3_int64 persistence_store(struct persistence *persistent, const char *event_json) {
  if (!persistent || !persistent->enabled || !persistent->insert_stmt || !event_json) return 0;
  char ts[64];
  format_iso(time(NULL), ts, sizeof ts);
  sqlite3_reset(persistent->insert_stmt);
  sqlite3_clear_bindings(persistent->insert_stmt);
  sqlite3_bind_text(persistent->insert_stmt, 1, ts, -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(persistent->insert_stmt, 2, event_json, -1, SQLITE_TRANSIENT);
  int rc = sqlite3_step(persistent->insert_stmt);
  if (rc != SQLITE_DONE) {
    app_log(LOG_LEVEL_ERROR, "DB", "Errore salvataggio evento: %s", sqlite3_errmsg(persistent->db));
    sqlite3_reset(persistent->insert_stmt);
    return 0;
  }
  sqlite3_int64 inserted = sqlite3_last_insert_rowid(persistent->db);
  sqlite3_reset(persistent->insert_stmt);
  return inserted;
}

/* Backfill the in-memory store with the most recent rows from SQLite so new
   SSE clients have immediate history.  Requires both persistent and store.  */
static void persistence_fill_store(struct persistence *persistent, struct event_store *store) {
  if (!persistent || !store || !persistent->enabled || !persistent->db) return;
  size_t limit = store->max ? store->max : DEFAULT_BUFFER_SIZE;
  // Preload the most recent rows so SSE clients immediately see past events.
  const char *sql =
      "SELECT event_json FROM events ORDER BY id DESC LIMIT ?1";
  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(persistent->db, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    app_log(LOG_LEVEL_ERROR, "DB", "Errore preload eventi: %s", sqlite3_errmsg(persistent->db));
    if (stmt) sqlite3_finalize(stmt);
    return;
  }
  sqlite3_bind_int64(stmt, 1, (sqlite3_int64) limit);
  char **rows = (char **)calloc(limit, sizeof(char *));
  size_t count = 0;
  if (!rows) {
    sqlite3_finalize(stmt);
    return;
  }
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const unsigned char *txt = sqlite3_column_text(stmt, 0);
    if (!txt) continue;
    char *copy = duplicate_json_string((const char *) txt);
    if (!copy) continue;
    rows[count++] = copy;
    if (count >= limit) break;
  }
  if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
    app_log(LOG_LEVEL_WARN, "DB", "Preload eventi interrotto: %s", sqlite3_errmsg(persistent->db));
  }
  sqlite3_finalize(stmt);
  for (size_t i = 0; i < count; ++i) {
    size_t idx = count - 1 - i;
    if (rows[idx]) event_store_add(store, rows[idx]);
  }
  free(rows);
  app_log(LOG_LEVEL_DEBUG, "DB", "Precaricati %zu eventi da SQLite", count);
}

/* Delete up to rows_to_delete of the oldest events so that disk usage stays
   within limits.  When provided, new_min_id receives the new minimum row id,
   and the size_t return value reports how many rows were removed.  */
static size_t persistence_purge_oldest(struct persistence *persistent, size_t rows_to_delete,
                                       sqlite3_int64 *new_min_id) {
  if (!persistent || !persistent->enabled || !persistent->db) return 0;
  if (rows_to_delete == 0) return 0;
  const char *sql =
      "WITH doomed AS (SELECT id FROM events ORDER BY id ASC LIMIT ?1) "
      "DELETE FROM events WHERE id IN (SELECT id FROM doomed);";
  sqlite3_stmt *stmt = NULL;
  if (sqlite3_prepare_v2(persistent->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    app_log(LOG_LEVEL_ERROR, "DB", "Errore purge eventi: %s", sqlite3_errmsg(persistent->db));
    if (stmt) sqlite3_finalize(stmt);
    return 0;
  }
  sqlite3_bind_int64(stmt, 1, (sqlite3_int64) rows_to_delete);
  int rc = sqlite3_step(stmt);
  size_t deleted = (size_t) sqlite3_changes(persistent->db);
  if (rc != SQLITE_DONE) {
    app_log(LOG_LEVEL_ERROR, "DB", "Errore DELETE eventi: %s", sqlite3_errmsg(persistent->db));
  } else if (deleted > 0) {
    app_log(LOG_LEVEL_INFO, "DB", "Eliminati %zu eventi piu' vecchi per mantenere il limite", deleted);
  }
  sqlite3_finalize(stmt);
  if (deleted > 0 && new_min_id) {
    sqlite3_stmt *min_stmt = NULL;
    sqlite3_int64 min_id = 0;
    if (sqlite3_prepare_v2(persistent->db, "SELECT MIN(id) FROM events", -1, &min_stmt, NULL) ==
        SQLITE_OK) {
      if (sqlite3_step(min_stmt) == SQLITE_ROW && sqlite3_column_type(min_stmt, 0) != SQLITE_NULL) {
        min_id = sqlite3_column_int64(min_stmt, 0);
      }
      sqlite3_finalize(min_stmt);
    } else if (min_stmt) {
      sqlite3_finalize(min_stmt);
    }
    *new_min_id = min_id;
  } else if (new_min_id) {
    *new_min_id = 0;
  }
  return deleted;
}

/* Ensure the persistent store stays within configured retention limits by
   deleting rows when total_events exceeds the threshold.  */
static void persistence_enforce_limit(struct app_state *state) {
  if (!state || !state->persistence.enabled) return;
  size_t max_allowed = state->persistence.max_events;
  if (max_allowed == 0) return;
  state_data_lock(state);
  size_t current_total = state->total_events;
  state_data_unlock(state);
  if (current_total <= max_allowed) return;
  size_t over_limit = current_total - max_allowed;
  size_t purge_size = state->persistence.purge_batch > 0 ? state->persistence.purge_batch : 1;
  if (purge_size < over_limit) purge_size = over_limit;
  if (purge_size > current_total) purge_size = current_total;
  sqlite3_int64 new_min_id = state->min_db_id;
  size_t deleted = persistence_purge_oldest(&state->persistence, purge_size, &new_min_id);
  if (deleted > 0) {
    state_data_lock(state);
    if (state->total_events >= deleted) state->total_events -= deleted;
    else state->total_events = 0;
    state->min_db_id = new_min_id;
    if (state->total_events == 0) {
      state->min_db_id = 0;
      state->max_db_id = 0;
    }
    state_data_unlock(state);
  }
}

/* Fetch up to per_page events using keyset pagination from persistent, filling
   page with JSON rows and metadata constrained by before_id/after_id and the
   optional timestamp window.  */
static int persistence_fetch_page(struct persistence *persistent, int per_page,
                                  sqlite3_int64 before_id, sqlite3_int64 after_id,
                                  sqlite3_int64 from_ts, sqlite3_int64 to_ts,
                                  struct events_page *page) {
  // Keyset pagination plus optional UTC timestamp window.
  if (!persistent || !persistent->enabled || !persistent->db || !page) return 0;
  memset(page, 0, sizeof(*page));
  if (per_page <= 0) return 0;
  if (before_id > 0) after_id = 0;
  sqlite3_stmt *stmt = NULL;
  const char *sql = NULL;
  sqlite3_int64 param = 0;
  int use_time_filter = (from_ts > 0 || to_ts > 0);
  int fetch_limit = per_page + 1;
  if (after_id > 0) {
    if (use_time_filter) {
      sql =
          "SELECT id, event_json, json_extract(event_json,'$.event_data.timestamp') AS evt_ts "
          "FROM events WHERE id > ?1 "
          "AND (?3 = 0 OR json_extract(event_json,'$.event_data.timestamp') >= ?3) "
          "AND (?4 = 0 OR json_extract(event_json,'$.event_data.timestamp') <= ?4) "
          "ORDER BY id DESC LIMIT ?2";
    } else {
      sql =
          "SELECT id, event_json, NULL AS evt_ts "
          "FROM events WHERE id > ?1 "
          "ORDER BY id DESC LIMIT ?2";
    }
    param = after_id;
  } else {
    if (use_time_filter) {
      sql =
          "SELECT id, event_json, json_extract(event_json,'$.event_data.timestamp') AS evt_ts "
          "FROM events WHERE (?1 = 0 OR id < ?1) "
          "AND (?3 = 0 OR json_extract(event_json,'$.event_data.timestamp') >= ?3) "
          "AND (?4 = 0 OR json_extract(event_json,'$.event_data.timestamp') <= ?4) "
          "ORDER BY id DESC LIMIT ?2";
    } else {
      sql =
          "SELECT id, event_json, NULL AS evt_ts "
          "FROM events WHERE (?1 = 0 OR id < ?1) "
          "ORDER BY id DESC LIMIT ?2";
    }
    param = before_id;
  }
  if (sqlite3_prepare_v2(persistent->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    app_log(LOG_LEVEL_ERROR, "DB", "Errore SELECT eventi: %s", sqlite3_errmsg(persistent->db));
    if (stmt) sqlite3_finalize(stmt);
    return 0;
  }
  sqlite3_bind_int64(stmt, 1, param);
  sqlite3_bind_int64(stmt, 2, (sqlite3_int64)fetch_limit);
  if (use_time_filter) {
    sqlite3_bind_int64(stmt, 3, from_ts);
    sqlite3_bind_int64(stmt, 4, to_ts);
  }
  page->rows = (char **)calloc((size_t)fetch_limit, sizeof(char *));
  page->ids = (sqlite3_int64 *)calloc((size_t)fetch_limit, sizeof(sqlite3_int64));
  page->timestamps =
      (sqlite3_int64 *)calloc((size_t)fetch_limit, sizeof(sqlite3_int64));
  if (!page->rows || !page->ids || !page->timestamps) {
    sqlite3_finalize(stmt);
    events_page_free(page);
    return 0;
  }
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    sqlite3_int64 row_id = sqlite3_column_int64(stmt, 0);
    const unsigned char *txt = sqlite3_column_text(stmt, 1);
    sqlite3_int64 evt_ts = 0;
    if (sqlite3_column_type(stmt, 2) != SQLITE_NULL) {
      evt_ts = sqlite3_column_int64(stmt, 2);
    }
    if (!txt) continue;
    char *copy = duplicate_json_string((const char *)txt);
    if (!copy) continue;
    page->ids[page->count] = row_id;
    page->timestamps[page->count] = evt_ts;
    page->rows[page->count] = copy;
    page->count++;
    if ((int)page->count >= fetch_limit) break;
  }
  sqlite3_finalize(stmt);
  if (page->count > (size_t)per_page) {
    page->trimmed_extra = 1;
    free(page->rows[page->count - 1]);
    page->rows[page->count - 1] = NULL;
    page->ids[page->count - 1] = 0;
    page->timestamps[page->count - 1] = 0;
    page->count--;
  }
  return 1;
}

/* Return nonzero when the SQLite store contains rows newer than
   greater_than_id and within the optional timestamp bounds.  */
static int persistence_has_newer_than(struct persistence *persistent, sqlite3_int64 greater_than_id,
                                      sqlite3_int64 from_ts, sqlite3_int64 to_ts) {
  if (!persistent || !persistent->enabled || !persistent->db) return 0;
  int use_time_filter = (from_ts > 0 || to_ts > 0);
  const char *sql = use_time_filter
                        ? "SELECT 1 FROM events WHERE id > ?1 "
                          "AND (?2 = 0 OR json_extract(event_json,'$.event_data.timestamp') >= ?2) "
                          "AND (?3 = 0 OR json_extract(event_json,'$.event_data.timestamp') <= ?3) "
                          "LIMIT 1"
                        : "SELECT 1 FROM events WHERE id > ?1 LIMIT 1";
  sqlite3_stmt *stmt = NULL;
  if (sqlite3_prepare_v2(persistent->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    if (stmt) sqlite3_finalize(stmt);
    return 0;
  }
  sqlite3_bind_int64(stmt, 1, greater_than_id);
  if (use_time_filter) {
    sqlite3_bind_int64(stmt, 2, from_ts);
    sqlite3_bind_int64(stmt, 3, to_ts);
  }
  int exists = sqlite3_step(stmt) == SQLITE_ROW;
  sqlite3_finalize(stmt);
  return exists;
}
#else /* !HAVE_SQLITE3 */
/* Stub used when SQLite support is not compiled in; no handles to close.  */
static void persistence_close(struct persistence *persistent) {
  (void)persistent;
}

/* Update counters only from the in-memory store when SQLite is missing.  */
static void persistence_snapshot(struct app_state *state) {
  if (!state) return;
  state->total_events = state->store.count;
  state->max_db_id = 0;
  state->min_db_id = 0;
}

/* Warn the operator that SQLite persistence is unavailable in this build while
   keeping the subsystem disabled.  Returns 0 so startup proceeds.  */
static int persistence_init(struct persistence *persistent, const struct config *cfg) {
  if (persistent) memset(persistent, 0, sizeof *persistent);
  if (cfg && cfg->db_enabled) {
    app_log(LOG_LEVEL_WARN, "DB", "Supporto SQLite non disponibile in compilazione: persistenza disattivata");
  }
  return 0;
}

/* Stubbed persistence_store: accepts parameters for API compatibility but does
   not save anything and always returns 0.  */
static sqlite3_int64 persistence_store(struct persistence *persistent, const char *event_json) {
  (void)persistent;
  (void)event_json;
  return 0;
}

/* Stubbed preload helper when SQLite is not compiled; parameters are unused.  */
static void persistence_fill_store(struct persistence *persistent, struct event_store *store) {
  (void)persistent;
  (void)store;
}
#endif /* HAVE_SQLITE3 */

/* Allocate a heap-duplicated copy of text terminated with NUL.  Returns the
   new buffer or NULL on allocation failure.  */
static char *duplicate_json_string(const char *text) {
  size_t len = strlen(text);
  char *copy = (char *)malloc(len + 1);
  if (!copy) return NULL;
  memcpy(copy, text, len);
  copy[len] = '\0';
  return copy;
}

/* Copy json and append it to the SSE pending queue in state.  Ownership of the
   copied string passes to the queue item.  */
static void enqueue_sse_event(struct app_state *state, const char *json) {
  if (!state || !json) return;
  char *copy = duplicate_json_string(json);
  if (!copy) return;
  struct sse_pending *item = (struct sse_pending *)calloc(1, sizeof(struct sse_pending));
  if (!item) {
    free(copy);
    return;
  }
  item->json = copy;
  app_mutex_lock(&state->sse_queue_lock);
  int was_empty = state->sse_head == NULL;
  if (state->sse_tail) {
    state->sse_tail->next = item;
  } else {
    state->sse_head = item;
  }
  state->sse_tail = item;
  app_mutex_unlock(&state->sse_queue_lock);
  /* Interrupt the HTTP thread's mg_mgr_poll() so the event is broadcast right
     away instead of after the poll timeout.  One wakeup per non-empty queue
     is enough: the HTTP loop drains everything before sleeping again.  */
  if (was_empty && state->http_mgr && state->http_listener_id) {
    mg_wakeup(state->http_mgr, state->http_listener_id, "", 0);
  }
}

/* Drain up to a bounded number of queued SSE events, broadcasting them to all
   active clients.  This function frees queue entries as it goes and returns
   nonzero when entries are still queued.  */
static int flush_sse_queue(struct app_state *state) {
  if (!state) return 0;
  const size_t max_per_loop = 100;
  size_t processed = 0;
  while (processed < max_per_loop) {
    app_mutex_lock(&state->sse_queue_lock);
    struct sse_pending *item = state->sse_head;
    if (item) {
      state->sse_head = item->next;
      if (state->sse_head == NULL) state->sse_tail = NULL;
    }
    app_mutex_unlock(&state->sse_queue_lock);
    if (!item) break;
    broadcast_sse(state, item->json);
    free(item->json);
    free(item);
    processed++;
  }
  app_mutex_lock(&state->sse_queue_lock);
  int remaining = state->sse_head != NULL;
  app_mutex_unlock(&state->sse_queue_lock);
  return remaining;
}

/* Return the cached total number of events tracked by state.  The value is
   read under a lock for consistency.  */
static size_t get_total_events(struct app_state *state) {
  if (!state) return 0;
  size_t total = 0;
  state_data_lock(state);
  total = state->total_events;
  state_data_unlock(state);
  return total;
}

/* Convert an MQTT message payload into the enriched JSON payload consumed by
   the web dashboard.  Returns a newly allocated string or NULL on failure.  */
static char *create_event_json(const char *topic, const void *payload, size_t payload_len) {
  (void)topic;
  cJSON *root = cJSON_CreateObject();
  if (!root) return NULL;

  char iso_now[64];
  format_iso(time(NULL), iso_now, sizeof iso_now);
  cJSON_AddStringToObject(root, "timestamp", iso_now);

  char *payload_copy = NULL;
  if (payload && payload_len > 0) {
    payload_copy = (char *)malloc(payload_len + 1);
    if (payload_copy) {
      memcpy(payload_copy, payload, payload_len);
      payload_copy[payload_len] = '\0';
    }
  } else {
    payload_copy = duplicate_json_string("");
  }
  if (!payload_copy) {
    cJSON_Delete(root);
    return NULL;
  }
  cJSON *payload_json = NULL;
  if (payload_copy && *payload_copy) {
    payload_json = cJSON_Parse(payload_copy);
  }

  if (payload_json) {
    cJSON *event_data = cJSON_CreateObject();
    if (event_data) {
      cJSON *src = cJSON_GetObjectItemCaseSensitive(payload_json, "src");
      cJSON *id = cJSON_GetObjectItemCaseSensitive(payload_json, "id");
      cJSON *type = cJSON_GetObjectItemCaseSensitive(payload_json, "type");
      cJSON *desc = cJSON_GetObjectItemCaseSensitive(payload_json, "desc");
      cJSON *event_ts = cJSON_GetObjectItemCaseSensitive(payload_json, "timestamp");
      if (cJSON_IsString(src)) cJSON_AddStringToObject(event_data, "src", src->valuestring);
      if (cJSON_IsNumber(id)) cJSON_AddNumberToObject(event_data, "id", id->valuedouble);
      if (cJSON_IsString(type)) cJSON_AddStringToObject(event_data, "type", type->valuestring);
      if (cJSON_IsString(desc)) cJSON_AddStringToObject(event_data, "desc", desc->valuestring);
      if (cJSON_IsNumber(event_ts)) {
        cJSON_AddNumberToObject(event_data, "timestamp", event_ts->valuedouble);
        char iso_event[64];
        format_iso((time_t)event_ts->valuedouble, iso_event, sizeof iso_event);
        cJSON_AddStringToObject(event_data, "timestamp_iso", iso_event);
      }
      cJSON *rule = cJSON_GetObjectItemCaseSensitive(payload_json, "rule");
      cJSON *anc = cJSON_GetObjectItemCaseSensitive(payload_json, "anc");
      cJSON *meas = cJSON_GetObjectItemCaseSensitive(payload_json, "meas");
      if (rule) cJSON_AddItemToObject(event_data, "rule", cJSON_Duplicate(rule, 1));
      if (anc) cJSON_AddItemToObject(event_data, "anc", cJSON_Duplicate(anc, 1));
      if (meas) cJSON_AddItemToObject(event_data, "meas", cJSON_Duplicate(meas, 1));
      cJSON_AddItemToObject(root, "event_data", event_data);
    }
    cJSON_Delete(payload_json);
  }

  char *json_text = cJSON_PrintUnformatted(root);
  cJSON_Delete(root);
  free(payload_copy);
  return json_text;
}

/* Handle a received MQTT publication by updating the store, persistence, and
   SSE queue.  */
static void process_mqtt_message(struct app_state *state, const char *topic, const void *payload, size_t payload_len) {
  if (!state) return;
  char *json = create_event_json(topic, payload, payload_len);
  if (json) {
    state_data_lock(state);
    event_store_add(&state->store, json);
    state->total_events += 1;
    state_data_unlock(state);
    sqlite3_int64 inserted_id = persistence_store(&state->persistence, json);
#if HAVE_SQLITE3
    if (state->persistence.enabled) {
      if (inserted_id > 0) {
        state_data_lock(state);
        if (state->max_db_id < inserted_id) state->max_db_id = inserted_id;
        if (state->min_db_id == 0 || inserted_id < state->min_db_id) {
          state->min_db_id = inserted_id;
        }
        state_data_unlock(state);
      }
      persistence_enforce_limit(state);
    }
#endif /* HAVE_SQLITE3 */
    enqueue_sse_event(state, json);
    app_log(LOG_LEVEL_DEBUG, "MQTT", "Event on %s (%zu bytes)", topic ? topic : "(null)", payload_len);
  }
}

/* Write the JSON payload specified by json to every connected SSE client
   stored in state while cleaning up dead connections.  */
static void broadcast_sse(struct app_state *state, const char *json) {
  struct sse_client **client = &state->sse_clients;
  size_t delivered = 0;
  static uint64_t last_no_client_warn_ms = 0;
  while (*client) {
    struct sse_client *curr = *client;
    if (curr->conn->is_closing) {
      *client = curr->next;
      free(curr);
    } else {
      mg_http_write_chunk(curr->conn, "data: ", 6);
      mg_http_write_chunk(curr->conn, json, strlen(json));
      mg_http_write_chunk(curr->conn, "\n\n", 2);
      delivered++;
      client = &curr->next;
    }
  }
  if (delivered == 0) {
    uint64_t now_ms = mg_millis();
    if (now_ms - last_no_client_warn_ms >= 10000) {
      app_log(LOG_LEVEL_WARN, "SSE", "Evento ricevuto ma nessun client attivo (refresh necessario?)");
      last_no_client_warn_ms = now_ms;
    }
  } else {
    app_log(LOG_LEVEL_DEBUG, "SSE", "Evento inoltrato a %zu client", delivered);
  }
}

/* Timer callback that sends periodic ": keepalive" comments to SSE clients so
   intermediaries keep the connection open.  Argument arg is cast to struct
   app_state *.  */
static void send_sse_ping(void *arg) {
  struct app_state *state = (struct app_state *)arg;
  if (!state) return;
  const char *ping = ": keepalive\n\n";
  struct sse_client **client = &state->sse_clients;
  while (*client) {
    struct sse_client *curr = *client;
    if (curr->conn->is_closing) {
      *client = curr->next;
      free(curr);
    } else {
      mg_http_write_chunk(curr->conn, ping, strlen(ping));
      client = &curr->next;
    }
  }
}

/* Serialize up to limit events from store starting at offset into connection c
   as part of a chunked JSON array.  The integer pointed by first tracks comma
   placement so existing output is respected.  */
static void stream_events_from_store(const struct event_store *store, struct mg_connection *c,
                                     size_t offset, size_t limit, int *first) {
  if (!store || !c || !limit) {
    app_log(LOG_LEVEL_DEBUG, "HTTP", "stream_events_from_store: early return store=%p c=%p limit=%zu", store, c, limit);
    return;
  }
  if (offset >= store->count) {
    app_log(LOG_LEVEL_DEBUG, "HTTP", "stream_events_from_store: offset=%zu >= count=%zu", offset, store->count);
    return;
  }
  size_t end = offset + limit;
  if (end > store->count) end = store->count;
  app_log(LOG_LEVEL_DEBUG, "HTTP", "stream_events_from_store: offset=%zu limit=%zu count=%zu end=%zu", offset, limit, store->count, end);
  for (size_t i = offset; i < end; ++i) {
    size_t idx = (store->head + store->max - 1 - i) % store->max;
    const char *json = store->items[idx].json;
    if (!json) {
      app_log(LOG_LEVEL_DEBUG, "HTTP", "stream_events_from_store: null json at i=%zu idx=%zu", i, idx);
      continue;
    }
    mg_http_printf_chunk(c, "%s%s", *first ? "" : ",", json);
    *first = 0;
  }
  app_log(LOG_LEVEL_DEBUG, "HTTP", "stream_events_from_store: done");
}

static const char *k_csp_header =
    "Content-Security-Policy: default-src 'self'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'; "
    "form-action 'self'; "
    "object-src 'none'; "
    "connect-src 'self' http: https: ws: wss:; "
    "img-src 'self' data:; "
    "style-src 'self'; "
    "script-src 'self';\r\n";

static const char *k_json_headers_with_csp =
    "Content-Type: application/json\r\n"
    "Cache-Control: no-cache\r\n"
 //   "Access-Control-Allow-Origin: *\r\n"
    "Content-Security-Policy: default-src 'self'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'; "
    "form-action 'self'; "
    "object-src 'none'; "
    "connect-src 'self' http: https: ws: wss:; "
    "img-src 'self' data:; "
    "style-src 'self'; "
    "script-src 'self';\r\n";

static const char *k_plain_error_headers_with_csp =
    "Content-Type: text/plain\r\n"
    "Cache-Control: no-cache\r\n"
  //  "Access-Control-Allow-Origin: *\r\n"
    "Content-Security-Policy: default-src 'self'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'; "
    "form-action 'self'; "
    "object-src 'none'; "
    "connect-src 'self' http: https: ws: wss:; "
    "img-src 'self' data:; "
    "style-src 'self'; "
    "script-src 'self';\r\n";

static const char *k_static_extra_headers =
    /*"Access-Control-Allow-Origin: *\r\n"*/
    "Content-Security-Policy: default-src 'self'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'; "
    "form-action 'self'; "
    "object-src 'none'; "
    "connect-src 'self' http: https: ws: wss:; "
    "img-src 'self' data:; "
    "style-src 'self'; "
    "script-src 'self';\r\n";

/* Send HTTP response headers indicating a status 200 chunked reply and set the
   Content-Type header to content_type (defaults to JSON).  */
static void http_send_chunked_begin(struct mg_connection *c, const char *content_type) {
  if (!c) return;
  if (!content_type) content_type = "application/json";
  mg_printf(c,
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: %s\r\n"
            "Cache-Control: no-cache\r\n"
            "Transfer-Encoding: chunked\r\n"
          //  "Access-Control-Allow-Origin: *\r\n"
            "%s\r\n",
            content_type, k_csp_header);
}

/* Finish a chunked response by sending the empty chunk terminator.  */
static void http_send_chunked_end(struct mg_connection *c) {
  if (!c) return;
  mg_http_write_chunk(c, "", 0);
}

/* Handle GET /api/events by streaming paginated events from either SQLite or
   the in-memory ring buffer into c.  The request hm supplies pagination and
   filtering parameters, and state provides backing data.  */
static void send_events_json(struct app_state *state, struct mg_connection *c,
                             struct mg_http_message *hm) {
  if (!state) {
    mg_http_reply(c, 500, k_plain_error_headers_with_csp, "Server state unavailable");
    return;
  }
  int per_page = query_param_int(hm, "per_page", EVENTS_DEFAULT_PER_PAGE);
  if (per_page < 1) per_page = EVENTS_DEFAULT_PER_PAGE;
  if (per_page > EVENTS_MAX_PER_PAGE) per_page = EVENTS_MAX_PER_PAGE;
#if HAVE_SQLITE3
  if (state->persistence.enabled) {
    sqlite3_int64 before_id = query_param_i64(hm, "before_id", 0);
    sqlite3_int64 after_id = query_param_i64(hm, "after_id", 0);
    sqlite3_int64 from_ts = query_param_i64(hm, "from_ts", 0);
    sqlite3_int64 to_ts = query_param_i64(hm, "to_ts", 0);
    if (before_id < 0) before_id = 0;
    if (after_id < 0) after_id = 0;
    if (before_id > 0) after_id = 0;
    struct events_page page = {0};
    int fetched =
        persistence_fetch_page(&state->persistence, per_page, before_id, after_id, from_ts, to_ts,
                               &page);
    http_send_chunked_begin(c, "application/json");
    mg_http_printf_chunk(c, "{\"events\":[");
    int first = 1;
    if (fetched && page.count) {
      for (size_t i = 0; i < page.count; ++i) {
        mg_http_printf_chunk(c, "%s%s", first ? "" : ",", page.rows[i]);
        first = 0;
      }
    }
    sqlite3_int64 first_id = page.count ? page.ids[0] : 0;
    sqlite3_int64 last_id = page.count ? page.ids[page.count - 1] : 0;
    // We grab one extra row to know if "older" pages exist; "newer" requires a lookup.
    int has_older = page.trimmed_extra ? 1 : 0;
    int has_newer =
        (after_id > 0 && page.count)
            ? persistence_has_newer_than(&state->persistence, first_id, from_ts, to_ts)
            : 0;
    sqlite3_int64 prev_after = has_newer ? first_id : 0;
    sqlite3_int64 next_before = has_older ? last_id : 0;
    mg_http_printf_chunk(
        c,
        "],\"per_page\":%d,\"has_newer\":%s,\"has_older\":%s,"
        "\"first_id\":%lld,\"last_id\":%lld,"
        "\"prev_after_id\":%lld,\"next_before_id\":%lld,\"total\":%llu}",
        per_page, has_newer ? "true" : "false", has_older ? "true" : "false",
        (long long) first_id, (long long) last_id, (long long) prev_after, (long long) next_before,
        (unsigned long long) get_total_events(state));
    http_send_chunked_end(c);
    events_page_free(&page);
    app_log(LOG_LEVEL_DEBUG, "HTTP",
            "send_events_json keyset before_id=%lld after_id=%lld from_ts=%lld to_ts=%lld",
            (long long) before_id, (long long) after_id, (long long) from_ts, (long long) to_ts);
    return;
  }
#endif /* HAVE_SQLITE3 */
  int page = query_param_int(hm, "page", 1);
  if (page < 1) page = 1;
  size_t total = 0;
  size_t total_pages = 1;
  size_t offset = 0;
  int first = 1;
  state_data_lock(state);
  total = state->store.count;
  total_pages = (per_page > 0 && total > 0) ? ((total - 1) / (size_t)per_page) + 1 : 1;
  if (total_pages < 1) total_pages = 1;
  if (page > (int)total_pages) page = (int)total_pages;
  if (page < 1) page = 1;
  offset = (total > 0) ? (size_t)(page - 1) * (size_t)per_page : 0;
  http_send_chunked_begin(c, "application/json");
  mg_http_printf_chunk(c, "{\"events\":[");
  stream_events_from_store(&state->store, c, offset, (size_t)per_page, &first);
  state_data_unlock(state);
  mg_http_printf_chunk(c, "],\"page\":%d,\"per_page\":%d,\"total\":%llu}", page, per_page,
                       (unsigned long long)total);
  http_send_chunked_end(c);
  app_log(LOG_LEVEL_DEBUG, "HTTP", "send_events_json ring buffer page=%d per_page=%d total=%zu", page, per_page,
          total);
}

/* Return the entire known event dataset, preferring SQLite when enabled and
   otherwise falling back to the in-memory ring buffer.  */
static void send_all_events(struct app_state *state, struct mg_connection *c) {
  if (!state) {
    mg_http_reply(c, 500, k_plain_error_headers_with_csp, "Server state unavailable");
    return;
  }
  http_send_chunked_begin(c, "application/json");
  mg_http_printf_chunk(c, "{\"events\":[");
  int first = 1;
  size_t total = 0;
#if HAVE_SQLITE3
  if (state->persistence.enabled) {
    const char *sql = "SELECT event_json FROM events ORDER BY id DESC";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(state->persistence.db, sql, -1, &stmt, NULL) == SQLITE_OK) {
      while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *txt = sqlite3_column_text(stmt, 0);
        if (!txt) continue;
        mg_http_printf_chunk(c, "%s%s", first ? "" : ",", (const char *)txt);
        first = 0;
      }
      sqlite3_finalize(stmt);
    } else {
      app_log(LOG_LEVEL_ERROR, "DB", "Errore dump eventi: %s", sqlite3_errmsg(state->persistence.db));
    }
    total = get_total_events(state);
  } else
#endif /* HAVE_SQLITE3 */
  {
    state_data_lock(state);
    total = state->store.count;
    stream_events_from_store(&state->store, c, 0, state->store.count, &first);
    state_data_unlock(state);
  }
  mg_http_printf_chunk(c, "],\"total\":%llu}", (unsigned long long)total);
  http_send_chunked_end(c);
}
/* Reply to /api/status with the MQTT connectivity snapshot stored inside
   state.  The response is small and not chunked.  */
static void send_status_json(struct app_state *state, struct mg_connection *c) {
  if (!state) {
    mg_http_reply(c, 500, k_plain_error_headers_with_csp, "Server state unavailable");
    return;
  }
  struct mqtt_status snapshot;
  status_lock(state);
  snapshot = state->status;
  status_unlock(state);
  mg_http_reply(c, 200, k_json_headers_with_csp,
                "{"
                "\"connected\":%s,"
                "\"last_connected\":\"%s\","
                "\"last_error\":\"%s\""
                "}",
                snapshot.connected ? "true" : "false", snapshot.last_connected, snapshot.last_error);
}

/* Serve /api/info by returning selected configuration fields like MQTT
   destination and buffer limits.  */
static void send_info_json(const struct config *cfg, struct mg_connection *c) {
  char buf[1024];
  snprintf(buf, sizeof(buf),
           "{\"version\":\"%s\",\"mqtt\":{\"host\":\"%s\",\"port\":%d,\"topic\":\"%s\"},\"buffer_limit\":%d}",
           APP_VERSION,
           cfg->broker_host,
           cfg->broker_port,
           cfg->topic,
           cfg->event_buffer);
  app_log(LOG_LEVEL_DEBUG, "HTTP", "Sending /api/info: %s", buf);
  mg_http_reply(c, 200, k_json_headers_with_csp, "%s", buf);
}

/* Track a new SSE client connection c inside state.  The function allocates a
   small list node and prepends it to the client list.  */
static void add_sse_client(struct app_state *state, struct mg_connection *c) {
  struct sse_client *client = (struct sse_client *)calloc(1, sizeof(struct sse_client));
  if (!client) return;
  client->conn = c;
  client->next = state->sse_clients;
  state->sse_clients = client;
  app_log(LOG_LEVEL_INFO, "SSE", "Client %lu connected", (unsigned long)c->id);
}

/* Remove connection c from the tracked SSE client list and free the node.  */
static void remove_sse_client(struct app_state *state, struct mg_connection *c) {
  struct sse_client **curr = &state->sse_clients;
  while (*curr) {
    if ((*curr)->conn == c) {
      struct sse_client *tmp = *curr;
      *curr = tmp->next;
      free(tmp);
      app_log(LOG_LEVEL_INFO, "SSE", "Client %lu disconnected", (unsigned long)c->id);
      break;
    }
    curr = &(*curr)->next;
  }
}

/* Compare hm->uri with target for equality.  Returns nonzero when both match
   exactly, allowing simple routing checks.  */
static int uri_is(struct mg_http_message *hm, const char *target) {
  size_t len = strlen(target);
  return hm->uri.len == len && strncmp(hm->uri.buf, target, len) == 0;
}

/* Stream up to sse_cache_limit recent events over c right after an SSE client
   connects, helping the UI display immediate history.  A positive requested
   value (the ?replay=N query parameter) lowers that limit further; negative
   means "use the configured limit".  */
static void sse_send_cached_events(struct app_state *state, struct mg_connection *c,
                                   int requested) {
  if (!state || !c || requested == 0) return;
  state_data_lock(state);
  if (state->store.count == 0) {
    state_data_unlock(state);
    return;
  }
  size_t to_send = state->store.count;
  size_t limit =
      state->cfg.sse_cache_limit > 0 ? (size_t)state->cfg.sse_cache_limit : state->store.count;
  if (requested > 0 && (size_t)requested < limit) limit = (size_t)requested;
  if (limit > 0 && to_send > limit) to_send = limit;
  app_log(LOG_LEVEL_DEBUG, "SSE", "Invio %zu eventi cached al nuovo client (limit %d)", to_send,
          state->cfg.sse_cache_limit);
  for (size_t i = to_send; i > 0; --i) {
    size_t idx = (state->store.head + state->store.max - i) % state->store.max;
    const char *json = state->store.items[idx].json;
    if (!json) continue;
    mg_http_write_chunk(c, "data: ", 6);
    mg_http_write_chunk(c, json, strlen(json));
    mg_http_write_chunk(c, "\n\n", 2);
  }
  state_data_unlock(state);
}

/* Mongoose HTTP callback that routes REST endpoints, static files, and SSE
   stream handling.  The event code ev and data ev_data follow Mongoose
   semantics, and the connection-level fn_data or mgr->userdata provides the
   app_state pointer.  */
static void http_handler(struct mg_connection *c, int ev, void *ev_data) {
  struct app_state *state = (struct app_state *)c->fn_data;
  if (!state && c->mgr) state = (struct app_state *)c->mgr->userdata;
  static struct mg_http_serve_opts opts;
  if (opts.root_dir == NULL && opts.extra_headers == NULL) {
    opts.root_dir = "web";
    opts.extra_headers = k_static_extra_headers;
  }

  switch (ev) {
    case MG_EV_HTTP_MSG: {
      struct mg_http_message *hm = (struct mg_http_message *)ev_data;
      if (!state) {
        mg_http_reply(c, 500, k_plain_error_headers_with_csp, "Server state unavailable");
      } else if (uri_is(hm, "/api/events")) {
        app_log(LOG_LEVEL_DEBUG, "HTTP", "GET /api/events");
        send_events_json(state, c, hm);
      } else if (uri_is(hm, "/api/events/all")) {
        app_log(LOG_LEVEL_DEBUG, "HTTP", "GET /api/events/all");
        send_all_events(state, c);
      } else if (uri_is(hm, "/api/status")) {
        app_log(LOG_LEVEL_DEBUG, "HTTP", "GET /api/status");
        send_status_json(state, c);
      } else if (uri_is(hm, "/api/info")) {
        app_log(LOG_LEVEL_DEBUG, "HTTP", "GET /api/info");
        send_info_json(&state->cfg, c);
      } else if (uri_is(hm, "/stream")) {
        app_log(LOG_LEVEL_DEBUG, "HTTP", "SSE stream requested (conn %lu)", (unsigned long)c->id);
        mg_printf(c,
                  "HTTP/1.1 200 OK\r\n"
                  "Content-Type: text/event-stream\r\n"
                  "Cache-Control: no-cache\r\n"
                  "Connection: keep-alive\r\n"
                  "Transfer-Encoding: chunked\r\n"
               //   "Access-Control-Allow-Origin: *\r\n"
                  "%s\r\n",
                  k_csp_header);
        const char *hello = ": connected\n\n";
        mg_http_write_chunk(c, hello, strlen(hello));
        add_sse_client(state, c);
        /* ?replay=0 skips the cached replay (clients that already load the
           history from /api/events); ?replay=N caps it to N events.  */
        sse_send_cached_events(state, c, query_param_int(hm, "replay", -1));
      } else {
        mg_http_serve_dir(c, hm, &opts);
      }
      break;
    }
    case MG_EV_CLOSE:
      if (state) remove_sse_client(state, c);
      break;
    default:
      break;
  }
}

static void mqtt_event_handler(struct mg_connection *c, int ev, void *ev_data);

static void mqtt_update_status(struct app_state *state, int connected, const char *error_fmt, ...) {
  if (!state) return;
  status_lock(state);
  state->status.connected = connected;
  if (connected) {
    format_iso(time(NULL), state->status.last_connected, sizeof state->status.last_connected);
    state->status.last_error[0] = '\0';
  } else if (error_fmt) {
    va_list args;
    va_start(args, error_fmt);
    vsnprintf(state->status.last_error, sizeof state->status.last_error, error_fmt, args);
    va_end(args);
  } else if (state->status.last_error[0] == '\0') {
    snprintf(state->status.last_error, sizeof state->status.last_error, "MQTT disconnected");
  }
  status_unlock(state);
}

/* Attempt to connect to the MQTT broker via mgr using Mongoose MQTT.  */
static bool start_mongoose_mqtt(struct app_state *state, struct mg_mgr *mgr) {
  if (!state || !mgr) return false;
  int keepalive = state->cfg.keepalive > 0 ? state->cfg.keepalive : 60;
  const char *host = state->cfg.broker_host;
  if (host == NULL || *host == '\0') host = "broker.hivemq.com";

  char url[512];
  if (strstr(host, "://") != NULL) {
    snprintf(url, sizeof url, "%s", host);
  } else {
    snprintf(url, sizeof url, "mqtt://%s:%d", host, state->cfg.broker_port);
  }

  struct mg_mqtt_opts opts;
  memset(&opts, 0, sizeof opts);
  opts.keepalive = (uint16_t)keepalive;
  opts.clean = true;
  opts.qos = 0;
  opts.user = mg_str(state->cfg.username);
  opts.pass = mg_str(state->cfg.password);

  char client_id[128];
  if (state->cfg.client_id[0]) {
    snprintf(client_id, sizeof client_id, "%s", state->cfg.client_id);
  } else {
    snprintf(client_id, sizeof client_id, "event-viewer-%lu", (unsigned long)time(NULL));
  }
  opts.client_id = mg_str(client_id);

  app_log(LOG_LEVEL_INFO, "MQTT", "Connecting to %s with mongoose mqtt", url);
  struct mg_connection *conn = mg_mqtt_connect(mgr, url, &opts, mqtt_event_handler, state);
  if (conn == NULL) {
    mqtt_update_status(state, 0, "Connessione MQTT fallita verso %.200s", url);
    return false;
  }
  conn->fn_data = state;
  state->mqtt_client = conn;
  return true;
}

static void mqtt_event_handler(struct mg_connection *c, int ev, void *ev_data) {
  struct app_state *state = c ? (struct app_state *)c->fn_data : NULL;
  if (!state && c && c->mgr) state = (struct app_state *)c->mgr->userdata;
  switch (ev) {
    case MG_EV_MQTT_OPEN: {
      int connack = ev_data ? *(int *)ev_data : -1;
      if (connack == 0) {
        mqtt_update_status(state, 1, NULL);
        if (state && state->cfg.topic[0]) {
          struct mg_mqtt_opts sub_opts;
          memset(&sub_opts, 0, sizeof sub_opts);
          sub_opts.topic = mg_str(state->cfg.topic);
          sub_opts.qos = 0;
          mg_mqtt_sub(c, &sub_opts);
          app_log(LOG_LEVEL_INFO, "MQTT", "Subscribed to %s", state->cfg.topic);
        } else {
          mqtt_update_status(state, 0, "MQTT topic vuoto");
          app_log(LOG_LEVEL_WARN, "MQTT", "MQTT topic vuoto");
        }
      } else {
        mqtt_update_status(state, 0, "MQTT connack error %d", connack);
        app_log(LOG_LEVEL_WARN, "MQTT", "Connack error %d", connack);
      }
      break;
    }
    case MG_EV_MQTT_MSG: {
      if (!state) break;
      struct mg_mqtt_message *mm = (struct mg_mqtt_message *)ev_data;
      if (!mm) break;
      char *topic = NULL;
      if (mm->topic.buf && mm->topic.len > 0) {
        topic = (char *)malloc(mm->topic.len + 1);
        if (topic) {
          memcpy(topic, mm->topic.buf, mm->topic.len);
          topic[mm->topic.len] = '\0';
        }
      }
      process_mqtt_message(state, topic ? topic : "", mm->data.buf, mm->data.len);
      free(topic);
      break;
    }
    case MG_EV_ERROR: {
      const char *err = (const char *)ev_data;
      if (err && *err) {
        mqtt_update_status(state, 0, "MQTT error: %s", err);
        app_log(LOG_LEVEL_WARN, "MQTT", "Error: %s", err);
      }
      break;
    }
    case MG_EV_CLOSE: {
      if (state && state->mqtt_client == c) state->mqtt_client = NULL;
      mqtt_update_status(state, 0, NULL);
      app_log(LOG_LEVEL_INFO, "MQTT", "Connection closed");
      break;
    }
    default:
      break;
  }
}

/* Dedicated thread procedure that runs its own mg_mgr to service the MQTT
   connection and reconnect timer.  Argument arg is the struct app_state *,
   and cross-platform return types are used.  */
#ifdef _WIN32
static unsigned __stdcall mqtt_thread_main(void *arg) {
#else /* !_WIN32 */
static void *mqtt_thread_main(void *arg) {
#endif /* !_WIN32 */
  struct app_state *state = (struct app_state *)arg;
  struct mg_mgr mqtt_mgr;
  mg_mgr_init(&mqtt_mgr);
  mqtt_mgr.userdata = state;

  uint64_t last_attempt = 0;
  uint64_t last_ping = 0;
  if (!start_mongoose_mqtt(state, &mqtt_mgr)) {
    app_log(LOG_LEVEL_ERROR, "MQTT", "Unable to start mongoose mqtt client");
  }
  for (;;) {
    if (state->mqtt_client == NULL) {
      uint64_t now = mg_millis();
      if (now - last_attempt >= 3000) {
        last_attempt = now;
        start_mongoose_mqtt(state, &mqtt_mgr);
      }
    } else if (state->status.connected && state->cfg.keepalive > 0) {
      uint64_t now = mg_millis();
      uint64_t interval = (uint64_t)state->cfg.keepalive * 500;
      if (interval < 1000) interval = 1000;
      if (now - last_ping >= interval) {
        mg_mqtt_ping(state->mqtt_client);
        last_ping = now;
      }
    }
    mg_mgr_poll(&mqtt_mgr, 200);
  }
#ifdef _WIN32
  return 0;
#else /* !_WIN32 */
  return NULL;
#endif /* !_WIN32 */
}

/* Entry point: load configuration, start HTTP and MQTT loops, then block
   pumping both event managers.  Returns nonzero on initialization failure.  */
int main(void) {
  app_log(LOG_LEVEL_INFO, "APP", "Versione %s (build %s %s)", APP_VERSION, __DATE__, __TIME__);
  log_startup_step(1, "caricamento configurazione da .env");
  load_env_file(".env");

  struct app_state state;
  memset(&state, 0, sizeof state);
  app_mutex_init(&state.data_lock);
  app_mutex_init(&state.status_lock);
  app_mutex_init(&state.sse_queue_lock);
  log_startup_step(2, "inizializzazione configurazione e persistenza");
  config_load(&state.cfg);
  g_log_level = state.cfg.log_level;
  event_store_init(&state.store, (size_t)state.cfg.event_buffer);
  if (persistence_init(&state.persistence, &state.cfg) != 0) {
    app_log(LOG_LEVEL_WARN, "DB", "Persistenza SQLite disabilitata");
  }
#if HAVE_SQLITE3
  if (state.persistence.enabled) {
    persistence_fill_store(&state.persistence, &state.store);
  }
#endif /* HAVE_SQLITE3 */
  persistence_snapshot(&state);
#if HAVE_SQLITE3
  persistence_enforce_limit(&state);
#endif /* HAVE_SQLITE3 */
  state.status.connected = 0;
  state.status.last_connected[0] = '\0';
  state.status.last_error[0] = '\0';
  state.mqtt_client = NULL;

  struct mg_mgr http_mgr;
  mg_mgr_init(&http_mgr);
  http_mgr.userdata = &state;
  log_startup_step(3, "bootstrap server HTTP");

  char http_addr[128];
  //snprintf(http_addr, sizeof http_addr, "http://0.0.0.0:%d", state.cfg.http_port);
  snprintf(http_addr, sizeof http_addr, "http://%s:%d", state.cfg.http_host, state.cfg.http_port);
  struct mg_connection *http_conn = mg_http_listen(&http_mgr, http_addr, http_handler, NULL);
  if (http_conn == NULL) {
    fprintf(stderr, "Failed to start HTTP server on %s\n", http_addr);
    return 1;
  }
  http_conn->fn_data = &state;
  app_log(LOG_LEVEL_INFO, "HTTP", "Listening on %s", http_addr);
  /* Must be ready before the MQTT thread starts calling enqueue_sse_event().  */
  if (mg_wakeup_init(&http_mgr)) {
    state.http_mgr = &http_mgr;
    state.http_listener_id = http_conn->id;
  } else {
    app_log(LOG_LEVEL_WARN, "HTTP", "mg_wakeup non disponibile: eventi SSE inoltrati entro 100 ms");
  }

  log_startup_step(4, "preparazione MQTT e avvio worker");

#ifdef _WIN32
  uintptr_t mqtt_thread_handle =
      _beginthreadex(NULL, 0, mqtt_thread_main, &state, 0, NULL);
  if (mqtt_thread_handle == 0) {
    fprintf(stderr, "Failed to start MQTT thread\n");
    return 1;
  }
#else /* !_WIN32 */
  pthread_t mqtt_thread_handle;
  if (pthread_create(&mqtt_thread_handle, NULL, mqtt_thread_main, &state) != 0) {
    fprintf(stderr, "Failed to start MQTT thread\n");
    return 1;
  }
#endif /* !_WIN32 */
  (void)mqtt_thread_handle;

  app_log(LOG_LEVEL_INFO, "APP", "Avvio completato: HTTP %s, sottoscrizione %s", http_addr, state.cfg.topic);
  struct mg_timer ping_timer;
  mg_timer_init(&http_mgr.timers, &ping_timer, 5000, MG_TIMER_REPEAT, send_sse_ping, &state);
  int sse_backlog = 0;
  for (;;) {
    /* Don't sleep while SSE events are still queued after a bounded flush.  */
    mg_mgr_poll(&http_mgr, sse_backlog ? 0 : 100);
    sse_backlog = flush_sse_queue(&state);
  }

  event_store_free(&state.store);
  persistence_close(&state.persistence);
  mg_mgr_free(&http_mgr);
  app_mutex_free(&state.data_lock);
  app_mutex_free(&state.status_lock);
  app_mutex_free(&state.sse_queue_lock);
  return 0;
}
