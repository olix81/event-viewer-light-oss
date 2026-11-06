# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2025-2026 Olix
import argparse
import json
import os
import secrets
import sys
import time
from typing import Iterable, List

import paho.mqtt.client as mqtt


def load_events(file_path: str, start_id: int | None = None) -> List[str]:
    events: List[str] = []
    event_ids: List[int | None] = []
    with open(file_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            # Ensure the payload is valid JSON; store as string to preserve formatting
            parsed = json.loads(line)
            events.append(line)
            event_ids.append(parsed.get("id") if isinstance(parsed, dict) else None)
    if start_id is not None:
        normalized_start = int(start_id)
        start_index = None
        for idx, event_id in enumerate(event_ids):
            if event_id == normalized_start:
                start_index = idx
                break
        if start_index is None:
            raise ValueError(f"Evento con id {normalized_start} non trovato in {file_path}")
        if start_index > 0:
            events = events[start_index:]
    if not events:
        raise ValueError("Nessun evento disponibile dopo l'applicazione dei filtri richiesti")
    return events


def build_mqtt_client(client_id: str | None = None) -> mqtt.Client:
    generated_id = client_id or f"publisher-{secrets.token_hex(4)}"
    return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=generated_id)


def publish_events(
    events: Iterable[str],
    broker_host: str,
    broker_port: int,
    topic: str,
    delay: float,
    username: str | None = None,
    password: str | None = None,
) -> None:
    client = build_mqtt_client()
    if username:
        client.username_pw_set(username, password)

    def on_connect(
        client: mqtt.Client,
        userdata,
        flags,
        reason_code,
        properties=None,
    ):
        if reason_code == 0:
            print(f"Connected to {broker_host}:{broker_port}", flush=True)
        else:
            print(
                f"MQTT connection failed: {mqtt.error_string(reason_code)}",
                file=sys.stderr,
            )

    client.on_connect = on_connect
    client.connect(broker_host, broker_port, keepalive=60)
    client.loop_start()

    try:
        for idx, payload in enumerate(events, start=1):
            print(f"[{idx}] Publishing to {topic}: {payload}")
            client.publish(topic, payload, qos=0, retain=False)
            time.sleep(delay)
    finally:
        client.loop_stop()
        client.disconnect()


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Publish sample MQTT events to a broker at a fixed rate."
    )
    parser.add_argument(
        "--file",
        default=os.getenv("SAMPLE_FILE", "sample-events.jsonl"),
        help="Path to the JSONL file containing events (default: sample-events.jsonl)",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("MQTT_BROKER_HOST", "broker.hivemq.com"),
        help="MQTT broker host (default: broker.hivemq.com)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MQTT_BROKER_PORT", "1883")),
        help="MQTT broker port (default: 1883)",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("MQTT_TOPIC", "public/test/topic"),
        help="Target topic (default: public/test/topic)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=float(os.getenv("PUBLISH_DELAY_SECONDS", "4")),
        help="Delay between publishes in seconds (default: 4)",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("MQTT_USERNAME"),
        help="Optional MQTT username",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("MQTT_PASSWORD"),
        help="Optional MQTT password",
    )
    parser.add_argument(
        "--start-id",
        type=int,
        default=None,
        help="ID evento dal quale iniziare la pubblicazione (default: primo evento del file)",
    )

    args = parser.parse_args(argv)

    try:
        events = load_events(args.file, start_id=args.start_id)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Failed to load events from {args.file}: {exc}", file=sys.stderr)
        return 1

    try:
        publish_events(
            events=events,
            broker_host=args.host,
            broker_port=args.port,
            topic=args.topic,
            delay=args.delay,
            username=args.username,
            password=args.password,
        )
    except KeyboardInterrupt:
        print("Interrupted, stopping publisher.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
