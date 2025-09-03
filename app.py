#!/usr/bin/env python3
"""
Flask Web Application for JSON Event Analysis

This Flask app provides a web interface for the JSON parser script,
allowing users to upload JSON files and analyze events based on filter parameters.
"""

import json
import os
import pickle
import tempfile
import re
from datetime import datetime
from typing import Dict, List, Tuple, Any
from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    flash,
    redirect,
    url_for,
)
from werkzeug.utils import secure_filename
from typing import Optional, Tuple, List, Dict, Any


app = Flask(__name__)
app.secret_key = "your-secret-key-here"  # Change this in production

# Configuration
UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"json"}

# Create upload folder if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024  # 30MB max file size

# Store temporary data files
TEMP_DATA_FILES = {}


def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load and parse JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file '{file_path}': {e}")
    except Exception as e:
        raise Exception(f"Error reading file '{file_path}': {e}")

def _has_filter_for_root(event_data: Dict[str, Any], root_id: str, filter_param: str) -> Tuple[bool, Optional[str], Optional[Any]]:

    candidates = [
        f"output__{root_id}__querystring___{filter_param}",
        f"output__{root_id}__meta__{filter_param}",
        f"output__{root_id}__meta__handl__{filter_param}",
        f"output__{root_id}__{filter_param}",
        f"input__{root_id}__data__{filter_param}",
    ]
    for k in candidates:
        if k in event_data and event_data[k] is not None and event_data[k] != "":
            return True, k, event_data[k]

    prefix_regex = rf"^(?:input|output)__{re.escape(root_id)}__"
    suffix_regex = rf"__{re.escape(filter_param)}$"
    for k, v in event_data.items():
        if v is None or v == "":
            continue
        if re.search(prefix_regex, k) and re.search(suffix_regex, k):
            return True, k, v

    return False, None, None

def analyze_events(
    data: Dict[str, Any], filter_param: str, root_id: str
) -> Tuple[int, int, List[str], List[str]]:

    total_events = 0
    target_events = 0
    target_event_ids = []
    failed_event_ids = []

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
    """Format the output string."""
    output_lines = [f"total events: {total_events}", f"target events: {target_events}"]

    if show_ids:
        if target_event_ids:
            output_lines.append("\nlist of ids of target events")
            output_lines.extend(target_event_ids)

        if failed_event_ids:
            output_lines.append("\nlist of ids of failed events")
            output_lines.extend(failed_event_ids)

    return "\n".join(output_lines)


@app.route("/")
def index():
    """Main page with file upload form."""
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_file():
    """Handle file upload and analysis."""
    if "file" not in request.files:
        flash("No file selected", "error")
        return redirect(request.url)

    file = request.files["file"]
    if file.filename == "":
        flash("No file selected", "error")
        return redirect(request.url)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(filepath)

        # Get form parameters
        filter_param = request.form.get("filter_param", "").strip()
        root_id = request.form.get("root_id", "").strip()
        show_ids = request.form.get("show_ids") == "on"

        if not filter_param or not root_id:
            flash("Please provide both filter parameter and root ID", "error")
            return redirect(url_for("index"))

        try:
            # Load and analyze the JSON file
            data = load_json_file(filepath)

            # Store the data in a temporary file instead of session
            import uuid

            temp_id = str(uuid.uuid4())
            temp_file = os.path.join(tempfile.gettempdir(), f"json_data_{temp_id}.pkl")

            with open(temp_file, "wb") as f:
                pickle.dump(data, f)

            TEMP_DATA_FILES[temp_id] = temp_file

            total_events, target_events, target_event_ids, failed_event_ids = (
                analyze_events(data, filter_param, root_id)
            )

            # Format results
            output_content = format_output(
                total_events,
                target_events,
                target_event_ids,
                failed_event_ids,
                show_ids,
            )

            # Calculate success rate
            success_rate = (
                (target_events / total_events * 100) if total_events > 0 else 0
            )

            results = {
                "total_events": total_events,
                "target_events": target_events,
                "success_rate": round(success_rate, 2),
                "target_event_ids": target_event_ids,
                "failed_event_ids": failed_event_ids,
                "output_content": output_content,
                "filename": filename,
                "filter_param": filter_param,
                "root_id": root_id,
                "show_ids": show_ids,
                "temp_id": temp_id,  # Add temp_id to results
            }

            return render_template("results.html", results=results)

        except Exception as e:
            flash(f"Error processing file: {str(e)}", "error")
            return redirect(url_for("index"))
        finally:
            # Clean up uploaded file
            try:
                os.remove(filepath)
            except:
                pass
    else:
        flash("Invalid file type. Please upload a JSON file.", "error")
        return redirect(url_for("index"))


@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """API endpoint for programmatic analysis."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        json_data = data.get("json_data")
        filter_param = data.get("filter_param")
        root_id = data.get("root_id")
        show_ids = data.get("show_ids", False)

        if not all([json_data, filter_param, root_id]):
            return jsonify({"error": "Missing required parameters"}), 400

        # Analyze the events
        total_events, target_events, target_event_ids, failed_event_ids = (
            analyze_events(json_data, filter_param, root_id)
        )

        # Format output
        output_content = format_output(
            total_events, target_events, target_event_ids, failed_event_ids, show_ids
        )

        success_rate = (target_events / total_events * 100) if total_events > 0 else 0

        return jsonify(
            {
                "total_events": total_events,
                "target_events": target_events,
                "success_rate": round(success_rate, 2),
                "target_event_ids": target_event_ids,
                "failed_event_ids": failed_event_ids,
                "output_content": output_content,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/event/<temp_id>/<event_id>")
def event_detail(temp_id, event_id):
    """Show detailed information for a specific event."""
    # Get the original JSON data from temporary file
    if temp_id not in TEMP_DATA_FILES:
        flash("Event data not available. Please upload the file again.", "error")
        return redirect(url_for("index"))

    temp_file = TEMP_DATA_FILES[temp_id]

    try:
        with open(temp_file, "rb") as f:
            json_data = pickle.load(f)
    except:
        flash("Event data corrupted. Please upload the file again.", "error")
        return redirect(url_for("index"))

    if event_id not in json_data:
        flash(f"Event ID '{event_id}' not found.", "error")
        return redirect(url_for("index"))

    event_data = json_data[event_id]

    return render_template(
        "event_detail.html", event_id=event_id, event_data=event_data
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port, use_reloader=False)
