#!/usr/bin/env python3
"""
JSON Zap Interpreter - Flask App (v2)

- Subida de archivos JSON y análisis básico.
- Filtro avanzado por condiciones (event_name/isfire/etc).
- Intérprete (mini-DSL) con endpoints de consulta y exportación.
"""

import io
import json
import os
import pickle
import re
import tempfile
import uuid
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.utils import secure_filename

# Intérprete / Normalizador
from analyzer import normalize_events, build_catalog, run_query

# ------------------------------------------------------------------------------
# App & Config
# ------------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me-in-prod")

UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"json"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024  # 30MB

# Simple storage de archivos temporales (id -> path)
TEMP_DATA_FILES: Dict[str, str] = {}

# ------------------------------------------------------------------------------
# Utilidades base
# ------------------------------------------------------------------------------
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def load_json_file(file_path: str) -> Dict[str, Any]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file '{file_path}': {e}")
    except Exception as e:
        raise Exception(f"Error reading file '{file_path}': {e}")

# ------------------------------------------------------------------------------
# Compat (análisis antiguo por "filter_param" + "root_id")
# ------------------------------------------------------------------------------
def _has_filter_for_root(
    event_data: Dict[str, Any], root_id: str, filter_param: str
) -> Tuple[bool, Optional[str], Optional[Any]]:
    candidates = [
        f"output__{root_id}__querystring___{filter_param}",
        f"output__{root_id}__meta__{filter_param}",
        f"output__{root_id}__meta__handl__{filter_param}",
        f"output__{root_id}__{filter_param}",
        f"input__{root_id}__data__{filter_param}",
    ]
    for k in candidates:
        if k in event_data and event_data[k] not in (None, ""):
            return True, k, event_data[k]

    prefix_regex = rf"^(?:input|output)__{re.escape(root_id)}__"
    suffix_regex = rf"__{re.escape(filter_param)}$"
    for k, v in event_data.items():
        if v in (None, ""):
            continue
        if re.search(prefix_regex, k) and re.search(suffix_regex, k):
            return True, k, v

    return False, None, None


def analyze_events(
    data: Dict[str, Any], filter_param: str, root_id: str
) -> Tuple[int, int, List[str], List[str]]:
    total_events = 0
    target_events = 0
    target_event_ids: List[str] = []
    failed_event_ids: List[str] = []

    for event_id, event_data in data.items():
        total_events += 1
        has_match, _, _ = _has_filter_for_root(event_data, root_id, filter_param)
        if has_match:
            target_events += 1
            target_event_ids.append(event_id)
        else:
            failed_event_ids.append(event_id)

    return total_events, target_events, target_event_ids, failed_event_ids


def format_output(
    total_events: int,
    target_events: int,
    target_event_ids: List[str],
    failed_event_ids: List[str],
    show_ids: bool = False,
) -> str:
    out = [f"total events: {total_events}", f"target events: {target_events}"]
    if show_ids:
        if target_event_ids:
            out.append("\nlist of ids of target events")
            out.extend(target_event_ids)
        if failed_event_ids:
            out.append("\nlist of ids of failed events")
            out.extend(failed_event_ids)
    return "\n".join(out)

# ------------------------------------------------------------------------------
# Filtro avanzado (multi-condición) — útil cuando NO quieres DSL todavía
# ------------------------------------------------------------------------------
TRUE_LIKE = {"yes", "true", "1"}
FALSE_LIKE = {"no", "false", "0", ""}

def _normalize_scalar(val: Any) -> Optional[str]:
    if val is None or isinstance(val, (list, dict)):
        return None
    return str(val).strip()

def _iter_param_hits(event_data: Dict[str, Any], root_id: str, param_suffix: str):
    # 1) Claves directas …__<param>
    suffix_regex = rf"__{re.escape(param_suffix)}$"
    prefix_regex = rf"^(?:input|output)__{re.escape(root_id)}__"
    for k, v in event_data.items():
        if re.search(prefix_regex, k) and re.search(suffix_regex, k):
            vs = _normalize_scalar(v)
            if vs is not None:
                yield (k, vs)

    # 2) input__<root>__filter_criteria (lista de dicts)
    fc_key = f"input__{root_id}__filter_criteria"
    if fc_key in event_data and isinstance(event_data[fc_key], list):
        for row in event_data[fc_key]:
            if isinstance(row, dict):
                key_field = _normalize_scalar(row.get("key"))
                if key_field and key_field.endswith(f"__{param_suffix}"):
                    v = row.get("value", row.get("sample"))
                    vs = _normalize_scalar(v)
                    if vs is not None:
                        yield (f"{fc_key}[*].{param_suffix}", vs)

    # 3) output__<root>___zap_data_filter_meta[].sample → como event_name
    zmeta_key = f"output__{root_id}___zap_data_filter_meta"
    if zmeta_key in event_data and isinstance(event_data[zmeta_key], list):
        for row in event_data[zmeta_key]:
            if isinstance(row, dict):
                sample = _normalize_scalar(row.get("sample"))
                if sample is not None and param_suffix.lower() in {"event_name", "eventname"}:
                    yield (f"{zmeta_key}[*].sample", sample)

def _values_for_param(event_data: Dict[str, Any], root_id: str, param_suffix: str) -> List[str]:
    vals: List[str] = []
    for _, vs in _iter_param_hits(event_data, root_id, param_suffix):
        vals.append(vs)
    return vals

def _matches_condition(values: List[str], expected: Any) -> bool:
    if not values:
        return False
    norm_vals = [v.strip().lower() for v in values if v is not None]

    if isinstance(expected, bool):
        target = TRUE_LIKE if expected else FALSE_LIKE
        return any(v in target for v in norm_vals)

    if isinstance(expected, (list, tuple, set)):
        exp_norm = {str(x).strip().lower() for x in expected}
        return any(v in exp_norm for v in norm_vals)

    exp_norm = str(expected).strip().lower()
    return any(v == exp_norm for v in norm_vals)

def filter_events_by_conditions(
    data: Dict[str, Any],
    root_id: str,
    conditions: Dict[str, Any],
) -> List[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    hits: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = []
    for event_id, event_data in data.items():
        matched_per_param: Dict[str, List[str]] = {}
        ok_all = True
        for param, expected in conditions.items():
            p = param.strip().lower()
            if p in {"is_fire", "is-fire"}:
                p = "isfire"
            vals = _values_for_param(event_data, root_id, p)
            if not _matches_condition(vals, expected):
                ok_all = False
                break
            matched_per_param[p] = vals
        if ok_all:
            hits.append((event_id, event_data, matched_per_param))
    return hits

# ------------------------------------------------------------------------------
# Rutas
# ------------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    """Sube archivo, lo persiste como pickle y muestra resultados básicos."""
    if "file" not in request.files:
        flash("No file selected", "error")
        return redirect(request.url)

    file = request.files["file"]
    if file.filename == "":
        flash("No file selected", "error")
        return redirect(request.url)

    if not (file and allowed_file(file.filename)):
        flash("Invalid file type. Please upload a JSON file.", "error")
        return redirect(url_for("index"))

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(filepath)

    show_ids = request.form.get("show_ids") == "on"

    try:
        data = load_json_file(filepath)

        # Store the data for DSL analysis
        temp_id = str(uuid.uuid4())
        temp_file = os.path.join(tempfile.gettempdir(), f"json_data_{temp_id}.pkl")
        with open(temp_file, "wb") as f:
            pickle.dump(data, f)
        TEMP_DATA_FILES[temp_id] = temp_file

        # For backward compatibility, run a basic analysis if no DSL is used
        # This is mainly for the legacy "show_ids" feature
        total_events = len(data)
        
        # Create basic results for legacy compatibility
        results = {
            "total_events": total_events,
            "target_events": 0,  # Will be calculated via DSL queries
            "success_rate": 0.0,
            "target_event_ids": [],
            "failed_event_ids": [],
            "output_content": f"total events: {total_events}\nUse the DSL interpreter below to analyze your data.",
            "filename": filename,
            "filter_param": "N/A (use DSL queries)",
            "root_id": "N/A (use DSL queries)", 
            "show_ids": show_ids,
            "temp_id": temp_id,
        }
        return render_template("results.html", results=results)

    except Exception as e:
        flash(f"Error processing file: {str(e)}", "error")
        return redirect(url_for("index"))
    finally:
        try:
            os.remove(filepath)
        except Exception:
            pass

@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """Compat: analiza por filter_param + root_id sobre JSON literal."""
    try:
        data = request.get_json() or {}
        json_data = data.get("json_data")
        filter_param = data.get("filter_param")
        root_id = data.get("root_id")
        show_ids = bool(data.get("show_ids", False))

        if not all([json_data, filter_param, root_id]):
            return jsonify({"error": "Missing json_data/filter_param/root_id"}), 400

        total_events, target_events, target_ids, failed_ids = analyze_events(
            json_data, filter_param, root_id
        )
        output_content = format_output(
            total_events, target_events, target_ids, failed_ids, show_ids
        )
        success_rate = (target_events / total_events * 100) if total_events > 0 else 0.0

        return jsonify(
            {
                "total_events": total_events,
                "target_events": target_events,
                "success_rate": round(success_rate, 2),
                "target_event_ids": target_ids,
                "failed_event_ids": failed_ids,
                "output_content": output_content,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/analyze_advanced", methods=["POST"])
def api_analyze_advanced():
    """
    Body:
    {
      "json_data": {...} | null,
      "temp_id": "xxxx",          
      "root_id": "299911514",
      "conditions": {"event_name":"Schedule","isfire":true},
      "export": true
    }
    """
    try:
        payload = request.get_json() or {}
        json_data = payload.get("json_data")
        root_id = payload.get("root_id")
        conditions = payload.get("conditions") or {}
        do_export = bool(payload.get("export", False))

        # Permitir usar data previamente subida vía temp_id
        temp_id = payload.get("temp_id")
        if json_data is None and temp_id:
            if temp_id not in TEMP_DATA_FILES:
                return jsonify({"error": "invalid temp_id"}), 400
            with open(TEMP_DATA_FILES[temp_id], "rb") as f:
                json_data = pickle.load(f)

        if not json_data or not root_id or not conditions:
            return jsonify({"error": "Missed json_data/root_id/conditions"}), 400

        matches = filter_events_by_conditions(json_data, root_id, conditions)

        result = {
            "total_events": len(json_data),
            "matched": len(matches),
            "unmatched": len(json_data) - len(matches),
            "sample_ids": [eid for eid, _, _ in matches[:50]],
        }

        if do_export:
            rows: List[Dict[str, Any]] = []
            for event_id, event_data, matched in matches:
                row = {
                    "event_id": event_id,
                    "date": event_data.get("date"),
                    "status": event_data.get("status"),
                }
                for p, vals in matched.items():
                    row[p] = ", ".join(vals)
                rows.append(row)

            df = pd.DataFrame(rows)
            file_id = str(uuid.uuid4())[:8]
            xlsx_mem = io.BytesIO()
            with pd.ExcelWriter(xlsx_mem, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="matches")
            xlsx_mem.seek(0)

            temp_path = os.path.join(tempfile.gettempdir(), f"events_{file_id}.xlsx")
            with open(temp_path, "wb") as f:
                f.write(xlsx_mem.read())

            TEMP_DATA_FILES[f"xlsx_{file_id}"] = temp_path
            result["download_url"] = url_for(
                "download_file", temp_id=f"xlsx_{file_id}", file_id=file_id, _external=False
            )

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------- Intérprete (DSL) --------------------------
@app.route("/api/catalog", methods=["POST"])
def api_catalog():
    try:
        payload = request.get_json() or {}
        temp_id = payload.get("temp_id")
        if not temp_id or temp_id not in TEMP_DATA_FILES:
            return jsonify({"error": "invalid temp_id"}), 400

        path = TEMP_DATA_FILES[temp_id]
        try:
            with open(path, "rb") as f:
                json_data = pickle.load(f)
        except Exception as e:
            return jsonify({"error": f"could not load data: {e}"}), 500

        # normalize events into dataframe and build catalog
        try:
            df_events, _ = normalize_events(json_data)
            catalog = build_catalog(df_events)
            return jsonify({"ok": True, "catalog": catalog})
        except Exception as e:
            return jsonify({"error": f"error building catalog: {str(e)}"}), 500
            
    except Exception as e:
        return jsonify({"error": f"unexpected error: {str(e)}"}), 500

@app.route("/api/query", methods=["POST"])
def api_query():
    """
    Body:
    {
      "temp_id": "<id>",
      "dsl": "where event_name == \"Schedule\" and isfire == true | count by status"
    }
    """
    try:
        payload = request.get_json() or {}
        temp_id = payload.get("temp_id")
        dsl = (payload.get("dsl") or "").strip()
        if not temp_id or temp_id not in TEMP_DATA_FILES:
            return jsonify({"error": "temp_id inválido"}), 400
        if not dsl:
            return jsonify({"error": "Missing dsl"}), 400

        with open(TEMP_DATA_FILES[temp_id], "rb") as f:
            json_data = pickle.load(f)

        df_events, _ = normalize_events(json_data)
        out = run_query(df_events, dsl)
        return jsonify({"ok": True, "result": out})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/export", methods=["POST"])
def api_export():
    """
    Body:
    {
      "temp_id": "<id>",
      "dsl": "where event_name == \"Schedule\" and isfire == true | select *",
      "format": "xlsx" | "csv"
    }
    """
    try:
        payload = request.get_json() or {}
        temp_id = payload.get("temp_id")
        dsl = (payload.get("dsl") or "").strip()
        fmt = (payload.get("format") or "xlsx").lower()

        if not temp_id or temp_id not in TEMP_DATA_FILES:
            return jsonify({"error": "temp_id inválido"}), 400
        if not dsl:
            return jsonify({"error": "Missing dsl"}), 400

        with open(TEMP_DATA_FILES[temp_id], "rb") as f:
            json_data = pickle.load(f)

        df_events, _ = normalize_events(json_data)
        out = run_query(df_events, dsl)
        rows = out.get("rows", [])

        if fmt == "csv":
            import csv
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            cols = sorted({k for r in rows for k in r.keys()}) if rows else []
            with open(tmp.name, "w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=cols)
                w.writeheader()
                for r in rows:
                    w.writerow({c: r.get(c, "") for c in cols})
            return send_file(tmp.name, as_attachment=True, download_name="query.csv")

        # default XLSX
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        df = pd.DataFrame(rows)
        with pd.ExcelWriter(tmp.name, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name="query")
        return send_file(tmp.name, as_attachment=True, download_name="query.xlsx")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------- Download and detail -------------------
@app.route("/download/<temp_id>/<file_id>")
def download_file(temp_id, file_id):
    path = TEMP_DATA_FILES.get(temp_id)
    if not path or not os.path.exists(path):
        flash("File not available. Please regenerate the report.", "error")
        return redirect(url_for("index"))
    return send_file(path, as_attachment=True, download_name=f"events_{file_id}.xlsx")

@app.route("/event/<temp_id>/<event_id>")
def event_detail(temp_id, event_id):
    if temp_id not in TEMP_DATA_FILES:
        flash("Event data not available. Please upload the file again.", "error")
        return redirect(url_for("index"))

    temp_file = TEMP_DATA_FILES[temp_id]
    try:
        with open(temp_file, "rb") as f:
            json_data = pickle.load(f)
    except Exception:
        flash("Event data corrupted. Please upload the file again.", "error")
        return redirect(url_for("index"))

    if event_id not in json_data:
        flash(f"Event ID '{event_id}' not found.", "error")
        return redirect(url_for("index"))

    event_data = json_data[event_id]
    return render_template("event_detail.html", event_id=event_id, event_data=event_data)

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port, use_reloader=False)
    
    
    
