"""Registers (or updates) the Debezium connectors. Idempotent: PUT /connectors/<name>/config.

    python data-platform/cdc/register.py            # http://localhost:8093
    CONNECT_URL=... PLATFORM_DB_PASSWORD=... FINERACT_DB_PASSWORD=... python data-platform/cdc/register.py

Database passwords come from the environment and are never written to git.
"""
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8093")
PASSWORDS = {
    "postgres": os.environ.get("PLATFORM_DB_PASSWORD", "postgres"),
    "fineract-db": os.environ.get("FINERACT_DB_PASSWORD", ""),
}


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(CONNECT_URL + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def wait_for_connect(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            call("GET", "/connectors")
            return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(3)
    sys.exit(f"Kafka Connect did not answer on {CONNECT_URL} within {timeout} s")


def main():
    wait_for_connect()
    for path in sorted(pathlib.Path(__file__).parent.glob("connectors/*.json")):
        spec = json.loads(path.read_text())
        config = spec["config"]
        password = PASSWORDS.get(config["database.hostname"])
        if not password:
            sys.exit(f"No password for host {config['database.hostname']}: set FINERACT_DB_PASSWORD / PLATFORM_DB_PASSWORD")
        config["database.password"] = password
        call("PUT", f"/connectors/{spec['name']}/config", config)
        print("registered", spec["name"])
    deadline = time.time() + 120
    while time.time() < deadline:
        states = {n: call("GET", f"/connectors/{n}/status") for n in call("GET", "/connectors")}
        bad = {n: s for n, s in states.items()
               if s["connector"]["state"] == "FAILED" or any(t["state"] == "FAILED" for t in s["tasks"])}
        if bad:
            sys.exit("connector failed: " + json.dumps(bad, indent=2)[:2000])
        if states and all(s["connector"]["state"] == "RUNNING" and s["tasks"] and all(t["state"] == "RUNNING" for t in s["tasks"])
                          for s in states.values()):
            print("all connectors RUNNING:", ", ".join(sorted(states)))
            return
        time.sleep(3)
    sys.exit("connectors did not reach RUNNING in time")


if __name__ == "__main__":
    main()
