from __future__ import annotations

import json
import threading
import time
from flask import Flask, Response, jsonify, render_template, request

from core2_kafka import Config, Controller, Engine

app = Flask(__name__, template_folder=".")
engine = Engine(Config(kafka_enabled=True))
lock = threading.RLock()


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/snapshot")
def snapshot():
    return jsonify(engine.snapshot())


@app.get("/api/stream")
def stream():
    def generate():
        while True:
            snap = engine.snapshot()
            yield "data: " + json.dumps(snap, separators=(",", ":")) + "\n\n"
            if not snap["running"]:
                break
            time.sleep(0.20)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/configure")
def configure():
    try:
        payload = request.get_json(force=True) or {}
        engine.configure(payload)
        return jsonify({"ok": True, "config": engine.snapshot()["config"]})
    except RuntimeError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 409
    except (ValueError, TypeError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/controller_mode")
def controller_mode():
    payload = request.get_json(force=True) or {}
    mode = str(payload.get("mode", "fixed_rate"))
    if mode not in Controller.MODES:
        return jsonify({"ok": False, "error": "invalid controller mode"}), 400
    with engine.lock:
        if engine.running:
            return jsonify({"ok": False, "error": "stop the booking window before changing controller mode"}), 409
        engine.cfg.controller_mode = mode
        engine.controller = Controller(engine.cfg)
    return jsonify({"ok": True, "mode": mode})


@app.post("/api/start")
def start():
    return jsonify({"ok": engine.start()})


@app.post("/api/stop")
def stop():
    engine.stop()
    return jsonify({"ok": True})


@app.post("/api/reset")
def reset():
    engine.reset()
    return jsonify({"ok": True})


@app.get("/api/export")
def export_csv():
    from flask import Response
    return Response(
        engine.csv(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=tatkal_x_live_run.csv"},
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
