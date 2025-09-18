import re
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional

TRUE_LIKE = {"yes", "true", "1"}
FALSE_LIKE = {"no", "false", "0", ""}


def normalize_events(raw: dict):
    """
    Returns:
      df_events: one row per event with handy top-level fields
      df_kv: exploded key/values (if you already produce it)
    """
    rows = []
    kv_rows = []

    for event_id, event_data in raw.items():
        # Find input event ID from output__*__event_id fields
        input_event_id = None
        email = None
        event_url = None
        zap_url = None
        updated_by_name = None
        
        # Facebook tracking fields
        fbc = None
        fbp = None
        ip_address = None
        landing_url = None
        user_agent = None
        
        # UTM tracking fields
        utm_campaign = None
        utm_content = None
        utm_medium = None
        utm_source = None
        
        # Contact information fields
        contact_name = None
        contact_phone = None
        contact_phone_country = None

        for key, value in event_data.items():
            if key.startswith("output__") and key.endswith("__event_id"):
                input_event_id = value
            elif key.endswith("__primary_email"):
                email = value
            elif key.endswith("__event_url"):
                event_url = value
            elif key.endswith("__parent_task_history_link"):
                zap_url = value
            elif key.endswith("__updated_by_name") and updated_by_name is None:
                # Only extract once per event (first occurrence)
                updated_by_name = value
            # Facebook tracking fields
            elif key.endswith("__handl_fbc"):
                fbc = value
            elif key.endswith("__handl_fbp"):
                fbp = value
            elif key.endswith("__handl_ip"):
                ip_address = value
            elif key.endswith("__handl_url"):
                landing_url = value
            elif key.endswith("__handl_user_agent"):
                user_agent = value
            # UTM tracking fields
            elif key.endswith("__handl_utm_campaign"):
                utm_campaign = value
            elif key.endswith("__handl_utm_content"):
                utm_content = value
            elif key.endswith("__handl_utm_medium"):
                utm_medium = value
            elif key.endswith("__handl_utm_source"):
                utm_source = value
            # Contact information fields
            elif key.endswith("__lead__contact__name") and contact_name is None:
                contact_name = value
            elif key.endswith("__lead__contact__phone__phone") and contact_phone is None:
                contact_phone = value
            elif key.endswith("__lead__contact__phone__country") and contact_phone_country is None:
                contact_phone_country = value

        base = {
            "event_id": event_id,
            "input_event_id": input_event_id,
            "date": event_data.get("date"),
            "status": event_data.get("status"),
            "object_id": event_data.get("object_id"),
            "object_title": event_data.get("object_title"),
            "email": email,
            "event_url": event_url,
            "zap_url": zap_url,
            "updated_by_name": updated_by_name,
            # Facebook tracking
            "fbc": fbc,
            "fbp": fbp,
            "ip_address": ip_address,
            "landing_url": landing_url,
            "user_agent": user_agent,
            # UTM tracking
            "utm_campaign": utm_campaign,
            "utm_content": utm_content,
            "utm_medium": utm_medium,
            "utm_source": utm_source,
            # Contact information
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "contact_phone_country": contact_phone_country,
        }

        # Add only the event_name and isfire fields (skip the root and bool variants)
        obj_root = str(event_data.get("object_id") or "")
        ev_name, _ = _first_output_scalar(
            event_data, "event_name", prefer_root=obj_root
        )
        isfire_s, _ = _first_output_scalar(
            event_data, "isfire", prefer_root=None
        )

        base["event_name"] = ev_name  # e.g., "CompleteRegistration"
        base["isfire"] = isfire_s  # e.g., "yes"
        # ---------- /NEW canonical fields ----------

        rows.append(base)

        # (optional) if you already populate kv_rows for a long table, keep your logic here

    df_events = pd.DataFrame(rows)
    df_kv = (
        pd.DataFrame(kv_rows)
        if kv_rows
        else pd.DataFrame(columns=["event_id", "key", "value"])
    )
    return df_events, df_kv


def _value_counts_safe(s: pd.Series, top: int | None = None) -> dict:
    """
    Count values without ever comparing None vs str.
    We sort by count (numbers), not by the labels (strings/None).
    """
    # represent None as a placeholder for counting/sorting, then map back
    placeholder = "<None>"
    s2 = s.astype("string").fillna(placeholder)
    vc = s2.value_counts(dropna=False, sort=True)  # sort by count desc
    if top is not None:
        vc = vc.head(top)
    out = {}
    for k, v in vc.items():
        out[None if k == placeholder else k] = int(v)
    return out


def build_catalog(df_events: pd.DataFrame) -> dict:
    cat = {"columns": list(df_events.columns), "events_counts": {}}
    
    if "status" in df_events:
        cat["events_counts"]["by_status"] = _value_counts_safe(df_events["status"])
    if "event_name" in df_events:
        cat["events_counts"]["by_event_name"] = _value_counts_safe(
            df_events["event_name"], top=50
        )
    if "isfire" in df_events:
        cat["events_counts"]["by_isfire"] = _value_counts_safe(df_events["isfire"])
    if "event_name_root" in df_events:
        cat["events_counts"]["by_event_name_root"] = _value_counts_safe(
            df_events["event_name_root"]
        )
    if "isfire_root" in df_events:
        cat["events_counts"]["by_isfire_root"] = _value_counts_safe(
            df_events["isfire_root"]
        )
    return cat


# where event_name == "Schedule" and isfire == true | count by status
def run_query(df_events: pd.DataFrame, dsl: str) -> Dict[str, Any]:
    if df_events.empty:
        return {"rows": [], "meta": {"note": "no data"}}

    # Basic parse
    where_clause = None
    group_by = None
    select_all = False
    want_count = False
    limit_n = None  # None = no explicit limit from DSL
    offset_n = 0  # default 0
    DEFAULT_LIMIT = 100  # safety guardrail

    parts = [p.strip() for p in dsl.split("|")]
    for part in parts:
        low = part.lower()
        if low.startswith("where "):
            where_clause = part[6:].strip()
        elif low.startswith("count by "):
            want_count = True
            group_by = [c.strip() for c in part[9:].split(",")]
        elif low.startswith("group by "):
            group_by = [c.strip() for c in part[9:].split(",")]
        elif low.startswith("select *"):
            select_all = True
        elif low.startswith("limit "):
            token = part[6:].strip()
            if token.lower() in {"all", "*"}:
                limit_n = None
            else:
                try:
                    limit_n = max(0, int(token))
                except Exception:
                    raise ValueError(f"Invalid LIMIT value: {token}")
        elif low.startswith("offset "):
            token = part[7:].strip()
            try:
                offset_n = max(0, int(token))
            except Exception:
                raise ValueError(f"Invalid OFFSET value: {token}")

    df = df_events.copy()

    # WHERE (very lightweight)
    if where_clause:
        expr = where_clause
        # booleans
        expr = re.sub(r"\btrue\b", "True", expr, flags=re.I)
        expr = re.sub(r"\bfalse\b", "False", expr, flags=re.I)
        # IN (...) naive transform to .isin([...])
        expr = re.sub(
            r"(\w+)\s+in\s+\(([^)]+)\)",
            lambda m: f"{m.group(1)}.isin([{','.join([x.strip() for x in m.group(2).split(',')])}])",
            expr,
            flags=re.I,
        )
        try:
            df = df.query(expr)
        except Exception:
            # fallback: simple `col == "value"` and booleans joined by AND
            conditions = []
            for token in re.split(r"\band\b", where_clause, flags=re.I):
                token = token.strip()
                m = re.match(r'(\w+)\s*==\s*"(.*)"', token)
                if m and m.group(1) in df.columns:
                    conditions.append(df[m.group(1)] == m.group(2))
                else:
                    m2 = re.match(r"(\w+)\s*==\s*(True|False)", token, flags=re.I)
                    if m2 and m2.group(1) in df.columns:
                        conditions.append(
                            df[m2.group(1)].astype("boolean")
                            == (m2.group(2).lower() == "true")
                        )
            if conditions:
                mask = conditions[0]
                for c in conditions[1:]:
                    mask = mask & c
                df = df[mask]

    # Helper to window result by offset/limit (applies to df/grp uniformly)
    def _window(frame: pd.DataFrame) -> pd.DataFrame:
        start = offset_n or 0
        if limit_n is None:
            # No explicit limit: apply default only for non-aggregated, non-select-all case later
            return frame.iloc[start:]
        return frame.iloc[start : start + limit_n]

    # Aggregations
    if want_count and group_by:
        grp = (
            df.groupby(group_by, dropna=False, sort=False)
            .size()
            .reset_index(name="count")
        )
        total = len(grp)
        out = grp if (limit_n is None and offset_n == 0) else _window(grp)
        return {
            "rows": out.to_dict(orient="records"),
            "meta": {
                "count": True,
                "group_by": group_by,
                "total_rows": total,
                "limit": limit_n,
                "offset": offset_n,
            },
        }

    if group_by:
        grp = (
            df.groupby(group_by, dropna=False, sort=False)
            .agg(count=("event_id", "count"))
            .reset_index()
        )
        total = len(grp)
        out = grp if (limit_n is None and offset_n == 0) else _window(grp)
        return {
            "rows": out.to_dict(orient="records"),
            "meta": {
                "group_by": group_by,
                "total_rows": total,
                "limit": limit_n,
                "offset": offset_n,
            },
        }

    # Projections
    total_rows = len(df)
    if select_all:
        # select * respects offset/limit if provided; otherwise no limit
        out = df if (limit_n is None and offset_n == 0) else _window(df)
        return {
            "rows": out.to_dict(orient="records"),
            "meta": {
                "select": "*",
                "rows": len(out),
                "total_rows": total_rows,
                "limit": limit_n,
                "offset": offset_n,
            },
        }

    # Default: apply safety default if user didn't specify limit/offset
    if limit_n is None and offset_n == 0:
        out = df.head(DEFAULT_LIMIT)
        return {
            "rows": out.to_dict(orient="records"),
            "meta": {
                "rows": len(out),
                "total_rows": total_rows,
                "limit": DEFAULT_LIMIT,
                "offset": 0,
                "note": "default limit applied",
            },
        }

    # If user provided limit/offset, apply them
    out = _window(df)
    return {
        "rows": out.to_dict(orient="records"),
        "meta": {
            "rows": len(out),
            "total_rows": total_rows,
            "limit": limit_n,
            "offset": offset_n,
        },
    }


def _is_scalar(x):
    return x is not None and not isinstance(x, (list, dict))


def _first_io_scalar(
    event_data: dict, suffixes: list[str], prefer_root: str | None = None
):
    """
    Find first scalar value for keys that end with any of the given suffixes.
    Search priority:
      1) output__<prefer_root>__*__<suffix> or output__<prefer_root>__<suffix>
      2) any output__*__<suffix>
      3) input__<prefer_root>__*__<suffix> or input__<prefer_root>__<suffix>
      4) any input__*__<suffix>
    Returns (value_str, root_id or None)
    """

    def _scan(prefix: str, prefer: bool):
        hits = []
        for k, v in event_data.items():
            if not k.startswith(prefix):
                continue
            if not any(k.endswith(f"__{suf}") for suf in suffixes):
                continue
            if not _is_scalar(v):
                continue
            # k looks like: prefix + "<root>" + "__..." + "__<suffix>"
            parts = k.split("__")
            root = parts[1] if len(parts) > 1 else None
            if prefer and prefer_root and root != prefer_root:
                continue
            hits.append((len(k), k, str(v).strip(), root))
        if hits:
            hits.sort(key=lambda t: t[0])  # shortest key first
            _, _, val, root = hits[0]
            return val, root
        return None, None

    # 1) output with prefer_root
    val, root = _scan("output__", prefer=True)
    if val is not None:
        return val, root
    # 2) any output
    val, root = _scan("output__", prefer=False)
    if val is not None:
        return val, root
    # 3) input with prefer_root
    val, root = _scan("input__", prefer=True)
    if val is not None:
        return val, root
    # 4) any input
    return _scan("input__", prefer=False)


def _first_output_scalar(event_data: dict, suffix: str, prefer_root: str | None = None):
    """
    Return (value, root_id) for the first output__<root>__...__<suffix> or output__<root>__<suffix>.
    Priority:
      1) output__<prefer_root>__<suffix> if present
      2) any output__*__<suffix>, preferring shorter keys (stable)
    """
    # 1) strictly prefer the given root
    if prefer_root:
        for k, v in event_data.items():
            if (
                k.startswith(f"output__{prefer_root}__")
                and k.endswith(f"__{suffix}")
                and _is_scalar(v)
            ):
                # allow keys that are exactly output__<root>__<suffix> or deeper
                return str(v), prefer_root

    # 2) any output key that ends with the suffix
    hits = []
    for k, v in event_data.items():
        if k.startswith("output__") and k.endswith(f"__{suffix}") and _is_scalar(v):
            # extract the root between "output__" and "__"
            try:
                root = k.split("__", 2)[1]
            except Exception:
                root = None
            hits.append((len(k), k, str(v), root))
    if hits:
        hits.sort(key=lambda t: t[0])  # shortest key first, stable
        _, _, val, root = hits[0]
        return val, root
    return None, None


def _to_bool_like(s: str | None) -> bool | None:
    if s is None:
        return None
    v = s.strip().lower()
    if v in TRUE_LIKE:
        return True
    if v in FALSE_LIKE:
        return False
    return None
