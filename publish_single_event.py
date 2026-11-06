# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2025-2026 Olix
import argparse
import json
import os
import sys
from typing import List, Optional

from publish_samples import build_mqtt_client, load_events
import paho.mqtt.client as mqtt


def resolve_payload(
    payload: Optional[str],
    file_path: str,
    start_id: int | None,
    index: int,
) -> str:
    if payload is not None:
        payload = payload.strip()
        if not payload:
            raise ValueError("Provided payload is empty")
        return payload

    events: List[str] = load_events(file_path, start_id=start_id)
    try:
        return events[index]
    except IndexError:
        raise ValueError(
            f"Event index {index} is out of bounds; file only has {len(events)} events"
        ) from None


def publish_single_event(
    payload: str,
    broker_host: str,
    broker_port: int,
    topic: str,
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
        print(f"Publishing to {topic}: {payload}")
        result = client.publish(topic, payload, qos=0, retain=False)
        result.wait_for_publish()
    finally:
        client.loop_stop()
        client.disconnect()


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Publish a single MQTT event to a broker."
    )
    parser.add_argument(
        "--payload",
        help="Payload to publish. If omitted, a single event is read from --file.",
    )
    parser.add_argument(
        "--file",
        default=os.getenv("SAMPLE_FILE", "sample-events.jsonl"),
        help="Path to the JSONL file containing events (default: sample-events.jsonl)",
    )
    parser.add_argument(
        "--event-id",
        type=int,
        default=None,
        help="ID of the event to publish (looked up in the JSONL file)",
    )
    parser.add_argument(
        "--index",
        type=int,
        default=0,
        help="Zero-based index of the event to publish from the file (default: 0)",
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
        "--username",
        default=os.getenv("MQTT_USERNAME"),
        help="Optional MQTT username",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("MQTT_PASSWORD"),
        help="Optional MQTT password",
    )

    args = parser.parse_args(argv)

    try:
        payload = resolve_payload(
            payload=args.payload,
            file_path=args.file,
            start_id=args.event_id,
            index=args.index,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Failed to select payload: {exc}", file=sys.stderr)
        return 1

    try:
        publish_single_event(
            payload=payload,
            broker_host=args.host,
            broker_port=args.port,
            topic=args.topic,
            username=args.username,
            password=args.password,
        )
    except KeyboardInterrupt:
        print("Interrupted, stopping publisher.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
