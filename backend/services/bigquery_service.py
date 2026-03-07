# bigquery_service.py

import os
import json
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any, Optional

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)


def _is_int_like_scalar(value: Any) -> bool:
    """Returns True for scalar integers (excluding booleans)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_oversized_int(value: Any) -> bool:
    """Checks if a scalar integer is outside the signed 64-bit range."""
    if not _is_int_like_scalar(value):
        return False
    return value > INT64_MAX or value < INT64_MIN


def _sanitize_for_storage(data: Any) -> Any:
    """
    Recursively traverses nested structures and normalizes values for storage:
    - empty dictionaries -> None
    - oversized integers (outside int64 range) -> str
    """
    if isinstance(data, dict):
        if not data:
            return None
        return {k: _sanitize_for_storage(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_sanitize_for_storage(item) for item in data]
    if _is_oversized_int(data):
        return str(data)
    return data


class BigQueryService:
    """
    BigQuery service with dual backend support:
    - mock: local pandas/parquet cache (dev/qa)
    - gcp: real Google BigQuery client (prod)
    """

    def __init__(self):
        self.mode = os.getenv("DATA_BACKEND_MODE", "mock").strip().lower()
        if self.mode not in {"mock", "gcp"}:
            raise ValueError("DATA_BACKEND_MODE must be 'mock' or 'gcp'.")

        if self.mode == "gcp":
            self._init_gcp_backend()
            print(f"BigQueryService initialized in GCP mode (table: {self._table_id}).")
        else:
            self._init_mock_backend()
            print("BigQueryService initialized in MOCK mode (local parquet cache).")

    def _init_gcp_backend(self):
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise RuntimeError(
                "google-cloud-bigquery is required for DATA_BACKEND_MODE=gcp."
            ) from e

        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        if not project_id:
            raise ValueError("BIGQUERY_PROJECT_ID must be set for DATA_BACKEND_MODE=gcp.")

        dataset_id = os.getenv("BIGQUERY_DATASET_ID", "kairyx")
        table_name = os.getenv("BIGQUERY_TABLE_NAME", "processed_events")
        self._table_id = os.getenv("BIGQUERY_TABLE_ID", f"{project_id}.{dataset_id}.{table_name}")
        self._curated_table_id = os.getenv(
            "BIGQUERY_EVENTS_CURATED_TABLE_ID",
            f"{project_id}.{dataset_id}.events_curated",
        )
        self._player_latest_state_table_id = os.getenv(
            "BIGQUERY_PLAYER_LATEST_STATE_TABLE_ID",
            f"{project_id}.{dataset_id}.player_latest_state",
        )
        self._dead_letter_table_id = os.getenv(
            "BIGQUERY_PIPELINE_DEAD_LETTERS_TABLE_ID",
            f"{project_id}.{dataset_id}.pipeline_dead_letters",
        )
        self._prediction_results_table_id = os.getenv(
            "BIGQUERY_PREDICTION_RESULTS_TABLE_ID",
            f"{project_id}.{dataset_id}.prediction_results",
        )

        self._bigquery = bigquery
        self._client = bigquery.Client(project=project_id)

    def _init_mock_backend(self):
        self._cache_path = ".cache/bigquery_table.parquet"
        self._curated_cache_path = ".cache/events_curated.parquet"
        self._player_latest_state_cache_path = ".cache/player_latest_state.parquet"
        self._dead_letter_cache_path = ".cache/pipeline_dead_letters.parquet"
        self._prediction_results_cache_path = ".cache/prediction_results.parquet"
        self._table = self._load_mock_table(self._cache_path)
        self._curated_table = self._load_mock_table(self._curated_cache_path)
        self._player_latest_state_table = self._load_mock_table(self._player_latest_state_cache_path)
        self._dead_letter_table = self._load_mock_table(self._dead_letter_cache_path)
        self._prediction_results_table = self._load_mock_table(self._prediction_results_cache_path)
        os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)

    def _load_mock_table(self, cache_path: str) -> pd.DataFrame:
        if os.path.exists(cache_path):
            print(f"Loading BigQuery cache from {cache_path}")
            table = pd.read_parquet(cache_path)
            return self._restore_complex_columns_from_parquet(table)
        return pd.DataFrame()

    def _prepare_for_parquet(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Converts dict/list values in object columns into JSON strings so Parquet
        can store mixed nested types without schema-conversion failures.
        """
        table_to_persist = table.copy()
        for column in table_to_persist.columns:
            series = table_to_persist[column]
            if series.dtype != "object":
                continue

            non_null = series[series.notna()]
            if non_null.empty:
                continue

            has_complex_values = non_null.map(lambda v: isinstance(v, (dict, list))).any()
            if not has_complex_values:
                continue

            table_to_persist[column] = series.map(
                lambda value: json.dumps(value) if isinstance(value, (dict, list)) else value
            )

        return table_to_persist

    def _restore_complex_columns_from_parquet(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Restores JSON-serialized dict/list values after loading Parquet data.
        """
        restored = table.copy()
        for column in restored.columns:
            series = restored[column]
            if series.dtype != "object":
                continue

            non_null = series[series.notna()]
            if non_null.empty:
                continue

            sample = non_null.iloc[0]
            if not isinstance(sample, str):
                continue

            if not (sample.startswith("{") or sample.startswith("[")):
                continue

            def _maybe_parse_json(value: Any) -> Any:
                if not isinstance(value, str):
                    return value
                if not (value.startswith("{") or value.startswith("[")):
                    return value
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return value

            restored[column] = series.map(_maybe_parse_json)
        return restored

    def _coerce_oversized_integer_columns(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Parquet cannot store integers outside int64 range.
        If a scalar-integer column contains oversized values, cast the full
        column to string to keep a consistent, writable schema.
        """
        coerced = table.copy()
        for column in coerced.columns:
            series = coerced[column]
            non_null = series[series.notna()]
            if non_null.empty:
                continue

            if not non_null.map(_is_int_like_scalar).all():
                continue

            if non_null.map(_is_oversized_int).any():
                coerced[column] = series.map(
                    lambda value: str(value) if _is_int_like_scalar(value) else value
                )
        return coerced

    def _persist_mock_table(self, table: pd.DataFrame, cache_path: str):
        if table.empty:
            if os.path.exists(cache_path):
                os.remove(cache_path)
            return
        table_to_persist = self._prepare_for_parquet(table)
        table_to_persist.to_parquet(cache_path)

    def _target_meta(self, target: str) -> Dict[str, str]:
        mapping = {
            "events_staging": {
                "table_attr": "_table",
                "cache_path": getattr(self, "_cache_path", ""),
                "table_id": getattr(self, "_table_id", ""),
            },
            "events_curated": {
                "table_attr": "_curated_table",
                "cache_path": getattr(self, "_curated_cache_path", ""),
                "table_id": getattr(self, "_curated_table_id", ""),
            },
            "player_latest_state": {
                "table_attr": "_player_latest_state_table",
                "cache_path": getattr(self, "_player_latest_state_cache_path", ""),
                "table_id": getattr(self, "_player_latest_state_table_id", ""),
            },
            "pipeline_dead_letters": {
                "table_attr": "_dead_letter_table",
                "cache_path": getattr(self, "_dead_letter_cache_path", ""),
                "table_id": getattr(self, "_dead_letter_table_id", ""),
            },
            "prediction_results": {
                "table_attr": "_prediction_results_table",
                "cache_path": getattr(self, "_prediction_results_cache_path", ""),
                "table_id": getattr(self, "_prediction_results_table_id", ""),
            },
        }
        if target not in mapping:
            raise ValueError(f"Unknown BigQueryService target '{target}'.")
        return mapping[target]

    def _load_rows_to_gcp_table(
        self,
        rows: List[Dict[str, Any]],
        table_id: str,
        write_disposition: Any,
    ):
        if rows:
            job_config = self._bigquery.LoadJobConfig(
                source_format=self._bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=write_disposition,
                create_disposition=self._bigquery.CreateDisposition.CREATE_IF_NEEDED,
                autodetect=True,
                ignore_unknown_values=True,
            )
            load_job = self._client.load_table_from_json(rows, table_id, job_config=job_config)
            load_job.result()
            return

        try:
            self._client.query(f"TRUNCATE TABLE `{table_id}`").result()
        except Exception:
            return

    def _append_rows(self, rows: List[Dict[str, Any]], target: str = "events_staging"):
        prepared_events = []
        for event in rows:
            event_copy = _sanitize_for_storage(dict(event))
            prepared_events.append(_sanitize_for_storage(event_copy))

        if not prepared_events:
            return

        meta = self._target_meta(target)
        if self.mode == "gcp":
            self._load_rows_to_gcp_table(
                prepared_events,
                meta["table_id"],
                self._bigquery.WriteDisposition.WRITE_APPEND,
            )
            print(f"Wrote {len(prepared_events)} rows to BigQuery table {meta['table_id']}.")
            return

        table_attr = meta["table_attr"]
        cache_path = meta["cache_path"]
        current_table = getattr(self, table_attr)
        new_data_df = pd.DataFrame(prepared_events)
        if current_table.empty:
            current_table = new_data_df
        else:
            current_table = pd.concat([current_table, new_data_df], ignore_index=True)

        print("Sanitizing DataFrame for Parquet compatibility...")
        if hasattr(current_table, "map"):
            current_table = current_table.map(_sanitize_for_storage)
        else:
            current_table = current_table.applymap(_sanitize_for_storage)
        current_table = self._coerce_oversized_integer_columns(current_table)
        setattr(self, table_attr, current_table)

        print(f"Wrote {len(new_data_df)} rows to local BigQuery mock target '{target}'. Table now has {len(current_table)} total rows.")
        self._persist_mock_table(current_table, cache_path)

    def _replace_rows(self, rows: List[Dict[str, Any]], target: str):
        prepared_rows = [_sanitize_for_storage(dict(row)) for row in rows]
        meta = self._target_meta(target)

        if self.mode == "gcp":
            self._load_rows_to_gcp_table(
                prepared_rows,
                meta["table_id"],
                self._bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            return

        table = pd.DataFrame(prepared_rows)
        if not table.empty:
            if hasattr(table, "map"):
                table = table.map(_sanitize_for_storage)
            else:
                table = table.applymap(_sanitize_for_storage)
            table = self._coerce_oversized_integer_columns(table)
        setattr(self, meta["table_attr"], table)
        self._persist_mock_table(table, meta["cache_path"])

    def _get_local_rows(self, target: str) -> List[Dict[str, Any]]:
        table = getattr(self, self._target_meta(target)["table_attr"])
        if table.empty:
            return []
        return table.to_dict(orient="records")

    def _filter_table_by_job(self, table: pd.DataFrame, job_id: Optional[str] = None) -> pd.DataFrame:
        if table.empty or not job_id:
            return table

        match_value = str(job_id)
        masks = []
        for column in ("job_id", "job_identifier", "last_job_id"):
            if column not in table.columns:
                continue
            masks.append(table[column].map(lambda value: str(value) == match_value if pd.notna(value) else False))

        if not masks:
            return table

        combined_mask = masks[0]
        for mask in masks[1:]:
            combined_mask = combined_mask | mask
        return table[combined_mask].copy()

    def _get_local_events_for_identity(
        self,
        player_id: Any,
        table: Optional[pd.DataFrame] = None,
        job_id: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        current_table = table if table is not None else self._table
        if current_table.empty:
            return None
        current_table = self._filter_table_by_job(current_table, job_id=job_id)
        if current_table.empty:
            return None

        match_value = str(player_id)
        candidate_frames = []
        for column in ("player_id", "canonical_user_id"):
            if column not in current_table.columns:
                continue
            mask = current_table[column].map(lambda value: str(value) == match_value if pd.notna(value) else False)
            matched = current_table[mask]
            if not matched.empty:
                candidate_frames.append(matched)

        if not candidate_frames:
            return None

        player_df = pd.concat(candidate_frames, ignore_index=True)
        row_signature = player_df.apply(
            lambda row: json.dumps(_sanitize_for_storage(row.to_dict()), sort_keys=True, default=str),
            axis=1,
        )
        player_df = player_df.loc[~row_signature.duplicated()].copy()
        return player_df.copy() if not player_df.empty else None

    def _query_rows_by_identity_gcp(
        self,
        table_id: str,
        player_id: Any,
        limit: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT *
            FROM `{table_id}`
            WHERE (
                CAST(player_id AS STRING) = @player_id
                OR CAST(canonical_user_id AS STRING) = @player_id
            )
        """
        if job_id:
            query += """
               AND (
                    CAST(job_id AS STRING) = @job_id
                    OR CAST(job_identifier AS STRING) = @job_id
               )
            """
        if limit is not None:
            query += f"\nLIMIT {max(1, int(limit))}"
        query_parameters = [
            self._bigquery.ScalarQueryParameter("player_id", "STRING", str(player_id))
        ]
        if job_id:
            query_parameters.append(self._bigquery.ScalarQueryParameter("job_id", "STRING", str(job_id)))
        job_config = self._bigquery.QueryJobConfig(query_parameters=query_parameters)
        try:
            return [dict(row.items()) for row in self._client.query(query, job_config=job_config).result()]
        except Exception:
            return []

    def _load_all_rows_from_target(self, target: str) -> List[Dict[str, Any]]:
        if self.mode == "gcp":
            table_id = self._target_meta(target)["table_id"]
            try:
                rows = [dict(row.items()) for row in self._client.query(f"SELECT * FROM `{table_id}`").result()]
                return rows
            except Exception:
                return []
        return self._get_local_rows(target)

    @staticmethod
    def _is_missing_key_part(value: Any) -> bool:
        if value is None:
            return True
        try:
            if pd.isna(value):
                return True
        except Exception:
            pass
        if isinstance(value, str) and value.strip() in {"", "None", "nan", "NaN"}:
            return True
        return False

    def _dedupe_events(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        dedupe_map: Dict[tuple, Dict[str, Any]] = {}
        for row in rows:
            event = dict(row)
            source = str(event.get("source", "unknown"))
            job_scope = str(event.get("job_identifier") or event.get("job_id") or "unknown_job")
            source_event_id = event.get("source_event_id")
            event_fingerprint = event.get("event_fingerprint")
            key = (
                "srcid",
                job_scope,
                source,
                str(source_event_id),
            ) if not self._is_missing_key_part(source_event_id) else (
                "fingerprint",
                job_scope,
                str(
                    event_fingerprint
                    if not self._is_missing_key_part(event_fingerprint)
                    else f"{source}:{event.get('canonical_user_id')}:{event.get('event_type')}:{event.get('event_time')}"
                ),
            )
            dedupe_map[key] = event
        return list(dedupe_map.values())

    def _build_latest_state_from_events(
        self,
        player_id: Any,
        player_df: pd.DataFrame,
        job_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if player_df is None or player_df.empty:
            return None

        df = player_df.copy()
        if "event_time" not in df.columns:
            return None

        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
        df = df[df["event_time"].notna()].copy()
        if df.empty:
            return None

        df = df.sort_values(by="event_time")
        first_seen = df["event_time"].iloc[0]
        last_seen = df["event_time"].iloc[-1]
        now = pd.Timestamp.now(tz="UTC")

        def _count_sessions(frame: pd.DataFrame) -> int:
            if frame.empty:
                return 0
            ordered = frame.sort_values(by="event_time")
            new_sessions = ordered["event_time"].diff() > pd.Timedelta(minutes=15)
            return int(1 + new_sessions.fillna(False).sum())

        total_revenue = 0.0
        if "event_properties" in df.columns and "event_type" in df.columns:
            purchases = df[df["event_type"] == "item_purchased"]
            if not purchases.empty:
                total_revenue = float(
                    purchases["event_properties"].apply(
                        lambda value: (
                            value.get("revenue_usd", 0)
                            if isinstance(value, dict)
                            else 0
                        )
                    ).sum()
                )

        last_campaign = None
        last_media_source = None
        email = None
        if "event_properties" in df.columns or "user_properties" in df.columns:
            for _, row in df.iloc[::-1].iterrows():
                props = row["event_properties"] if isinstance(row.get("event_properties"), dict) else {}
                user_props = row["user_properties"] if isinstance(row.get("user_properties"), dict) else {}
                if last_campaign is None and props.get("campaign") is not None:
                    last_campaign = props.get("campaign")
                if last_media_source is None and props.get("media_source") is not None:
                    last_media_source = props.get("media_source")
                if email is None:
                    email = user_props.get("email") or props.get("email") or props.get("user_email")
                if last_campaign is not None and last_media_source is not None and email is not None:
                    break

        window_7d = df[df["event_time"] >= (now - pd.Timedelta(days=7))]
        window_30d = df[df["event_time"] >= (now - pd.Timedelta(days=30))]
        canonical_user_id = None
        if "canonical_user_id" in df.columns:
            non_null_canon = df["canonical_user_id"].dropna()
            if not non_null_canon.empty:
                canonical_user_id = str(non_null_canon.iloc[-1])

        player_id_value = None
        if "player_id" in df.columns:
            non_null_player_ids = df["player_id"].dropna()
            if not non_null_player_ids.empty:
                player_id_value = str(non_null_player_ids.iloc[-1])

        last_job_id = None
        for column in ("job_id", "job_identifier"):
            if column in df.columns:
                non_null_values = df[column].dropna()
                if not non_null_values.empty:
                    last_job_id = str(non_null_values.iloc[-1])
                    break
        resolved_job_id = str(job_id or last_job_id or "unknown_job")

        total_sessions = _count_sessions(df)
        lifetime_events = int(len(df))
        return {
            "player_id": player_id_value or str(player_id),
            "canonical_user_id": canonical_user_id,
            "email": email,
            "first_seen_at": first_seen.isoformat(),
            "last_seen_at": last_seen.isoformat(),
            "lifetime_events": lifetime_events,
            "total_events": lifetime_events,
            "lifetime_revenue_usd": total_revenue,
            "total_revenue": total_revenue,
            "total_sessions": total_sessions,
            "sessions_7d": _count_sessions(window_7d),
            "sessions_30d": _count_sessions(window_30d),
            "days_since_last_seen": int((now - last_seen).days),
            "last_campaign": last_campaign,
            "last_media_source": last_media_source,
            "last_job_id": resolved_job_id,
            "job_id": resolved_job_id,
            "job_identifier": resolved_job_id,
        }

    def write_events_staging(self, rows: List[Dict[str, Any]], job_id: Optional[str] = None):
        if not rows:
            return

        resolved_job_id = job_id
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            if resolved_job_id is None:
                resolved_job_id = row_copy.get("job_id") or row_copy.get("job_identifier")
            prepared_rows.append(row_copy)

        resolved_job_id = str(resolved_job_id or "unknown_job")
        for row in prepared_rows:
            row.setdefault("job_id", resolved_job_id)
            row.setdefault("job_identifier", resolved_job_id)

        self._append_rows(prepared_rows, target="events_staging")

    def write_pipeline_dead_letters(self, rows: List[Dict[str, Any]], job_id: Optional[str] = None):
        if not rows:
            return

        resolved_job_id = job_id
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            if resolved_job_id is None:
                resolved_job_id = row_copy.get("job_id") or row_copy.get("job_identifier")
            prepared_rows.append(row_copy)

        resolved_job_id = str(resolved_job_id or "unknown_job")
        for row in prepared_rows:
            row.setdefault("job_id", resolved_job_id)
            row.setdefault("job_identifier", resolved_job_id)

        self._append_rows(prepared_rows, target="pipeline_dead_letters")

    def write_processed_events(self, events: List[Dict[str, Any]], job_identifier: str):
        if not events:
            return

        prepared_events = []
        for event in events:
            event_copy = dict(event)
            event_copy["job_identifier"] = job_identifier
            event_copy.setdefault("job_id", job_identifier)
            prepared_events.append(event_copy)

        self.write_events_staging(prepared_events, job_id=job_identifier)

    def run_events_curation(self, job_id: Optional[str] = None, event_date: Optional[str] = None) -> Dict[str, Any]:
        staging_rows = self._load_all_rows_from_target("events_staging")
        deduped_rows = self._dedupe_events(staging_rows)
        self._replace_rows(deduped_rows, target="events_curated")
        return {
            "job_id": job_id,
            "event_date": event_date,
            "full_recompute": True,
            "staging_rows": len(staging_rows),
            "curated_rows": len(deduped_rows),
            "duplicates_removed": max(0, len(staging_rows) - len(deduped_rows)),
        }

    def get_player_events_curated(self, player_id: Any, limit: int = 1000, job_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if self.mode == "gcp":
            rows = self._query_rows_by_identity_gcp(
                self._curated_table_id,
                player_id,
                limit=limit,
                job_id=job_id,
            )
            return rows

        player_df = self._get_local_events_for_identity(player_id, table=self._curated_table, job_id=job_id)
        if player_df is None or player_df.empty:
            return []
        return player_df.head(max(1, int(limit))).to_dict(orient="records")

    def get_events_for_player(self, player_id: Any, job_id: Optional[str] = None) -> Optional[pd.DataFrame]:
        if self.mode == "gcp":
            curated_rows = self._query_rows_by_identity_gcp(self._curated_table_id, player_id, job_id=job_id)
            if curated_rows:
                return pd.DataFrame(curated_rows)
            staging_rows = self._query_rows_by_identity_gcp(self._table_id, player_id, job_id=job_id)
            if staging_rows:
                return pd.DataFrame(staging_rows)
            return None

        player_df = self._get_local_events_for_identity(player_id, table=self._curated_table, job_id=job_id)
        if player_df is not None and not player_df.empty:
            return player_df
        return self._get_local_events_for_identity(player_id, table=self._table, job_id=job_id)

    def get_all_player_ids(self, job_id: Optional[str] = None) -> List[Any]:
        if self.mode == "gcp":
            job_filter = ""
            if job_id:
                job_filter = """
                    AND (
                        CAST(job_id AS STRING) = @job_id
                        OR CAST(job_identifier AS STRING) = @job_id
                    )
                """
            queries = [
                f"""
                    SELECT DISTINCT COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) AS player_id
                    FROM `{self._player_latest_state_table_id}`
                    WHERE COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) IS NOT NULL
                    {job_filter}
                """,
                f"""
                    SELECT DISTINCT COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) AS player_id
                    FROM `{self._curated_table_id}`
                    WHERE COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) IS NOT NULL
                    {job_filter}
                """,
                f"""
                    SELECT DISTINCT COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) AS player_id
                    FROM `{self._table_id}`
                    WHERE COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) IS NOT NULL
                    {job_filter}
                """,
            ]
            for query in queries:
                try:
                    job_config = None
                    if job_id:
                        job_config = self._bigquery.QueryJobConfig(
                            query_parameters=[
                                self._bigquery.ScalarQueryParameter("job_id", "STRING", str(job_id))
                            ]
                        )
                    rows = []
                    for row in self._client.query(query, job_config=job_config).result():
                        value = row["player_id"]
                        if value is not None:
                            rows.append(value)
                    if rows:
                        return rows
                except Exception:
                    continue
            return []

        for table in (self._player_latest_state_table, self._curated_table, self._table):
            filtered_table = self._filter_table_by_job(table, job_id=job_id)
            if filtered_table.empty:
                continue
            ids: List[str] = []
            for row in filtered_table.to_dict(orient="records"):
                value = row.get("player_id") or row.get("canonical_user_id")
                if value is not None:
                    ids.append(str(value))
            if ids:
                return list(dict.fromkeys(ids))
        return []

    def refresh_player_latest_state(self, job_id: Optional[str] = None, event_date: Optional[str] = None) -> Dict[str, Any]:
        curated_rows = self._load_all_rows_from_target("events_curated")
        if not curated_rows:
            self._replace_rows([], target="player_latest_state")
            return {
                "job_id": job_id,
                "event_date": event_date,
                "full_recompute": True,
                "players_aggregated": 0,
                "source_curated_rows": 0,
            }

        curated_df = pd.DataFrame(curated_rows)
        identity_keys = []
        for row in curated_df.to_dict(orient="records"):
            canonical = row.get("canonical_user_id")
            player = row.get("player_id")
            identity_keys.append(str(canonical or player or "unknown_user"))
        curated_df["_identity_key"] = identity_keys
        curated_df["_job_scope"] = curated_df.apply(
            lambda row: str(row.get("job_identifier") or row.get("job_id") or "unknown_job"),
            axis=1,
        )

        latest_state_rows: List[Dict[str, Any]] = []
        for (job_scope, identity_key), group_df in curated_df.groupby(["_job_scope", "_identity_key"], sort=False):
            latest_state = self._build_latest_state_from_events(identity_key, group_df.copy(), job_id=job_scope)
            if latest_state:
                latest_state_rows.append(latest_state)

        self._replace_rows(latest_state_rows, target="player_latest_state")
        return {
            "job_id": job_id,
            "event_date": event_date,
            "full_recompute": True,
            "players_aggregated": len(latest_state_rows),
            "source_curated_rows": len(curated_rows),
        }

    def get_player_latest_state(self, player_id: Any, job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if self.mode == "gcp":
            rows = self._query_rows_by_identity_gcp(self._player_latest_state_table_id, player_id, limit=1, job_id=job_id)
            if rows:
                return rows[0]
        else:
            latest_state_df = self._get_local_events_for_identity(player_id, table=self._player_latest_state_table, job_id=job_id)
            if latest_state_df is not None and not latest_state_df.empty:
                return latest_state_df.iloc[0].to_dict()

        player_df = self.get_events_for_player(player_id, job_id=job_id)
        if player_df is None or player_df.empty:
            return None
        return self._build_latest_state_from_events(player_id, player_df, job_id=job_id)

    def get_pipeline_dead_letters(self, job_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        if self.mode == "gcp":
            query = f"SELECT * FROM `{self._dead_letter_table_id}`"
            if job_id:
                query += " WHERE CAST(job_id AS STRING) = @job_id OR CAST(job_identifier AS STRING) = @job_id"
            query += f" LIMIT {max(1, int(limit))}"

            job_config = None
            if job_id:
                job_config = self._bigquery.QueryJobConfig(
                    query_parameters=[
                        self._bigquery.ScalarQueryParameter("job_id", "STRING", str(job_id))
                    ]
                )
            rows = [dict(row.items()) for row in self._client.query(query, job_config=job_config).result()]
            return rows

        if self._dead_letter_table.empty:
            return []

        table = self._dead_letter_table.copy()
        if job_id:
            match_value = str(job_id)
            job_id_mask = table["job_id"].map(lambda value: str(value) == match_value if pd.notna(value) else False) if "job_id" in table.columns else False
            job_identifier_mask = table["job_identifier"].map(lambda value: str(value) == match_value if pd.notna(value) else False) if "job_identifier" in table.columns else False
            if isinstance(job_id_mask, bool):
                mask = job_identifier_mask
            elif isinstance(job_identifier_mask, bool):
                mask = job_id_mask
            else:
                mask = job_id_mask | job_identifier_mask
            table = table[mask]

        if table.empty:
            return []

        rows = table.head(max(1, int(limit))).to_dict(orient="records")
        return rows

    def replace_prediction_results(self, job_id: str, rows: List[Dict[str, Any]]):
        resolved_job_id = str(job_id)
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            row_copy.setdefault("prediction_job_id", resolved_job_id)
            prepared_rows.append(row_copy)

        if self.mode == "gcp":
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_id", "STRING", resolved_job_id)
                ]
            )
            try:
                self._client.query(
                    f"DELETE FROM `{self._prediction_results_table_id}` WHERE CAST(prediction_job_id AS STRING) = @job_id",
                    job_config=job_config,
                ).result()
            except Exception:
                pass
            self._append_rows(prepared_rows, target="prediction_results")
            return

        table = self._prediction_results_table.copy()
        if not table.empty and "prediction_job_id" in table.columns:
            table = table[
                table["prediction_job_id"].map(
                    lambda value: str(value) != resolved_job_id if pd.notna(value) else True
                )
            ].copy()
        if prepared_rows:
            new_rows = pd.DataFrame(prepared_rows)
            if table.empty:
                table = new_rows
            else:
                table = pd.concat([table, new_rows], ignore_index=True)
        self._prediction_results_table = table
        self._persist_mock_table(table, self._prediction_results_cache_path)

    def append_prediction_results(self, job_id: str, rows: List[Dict[str, Any]]):
        resolved_job_id = str(job_id)
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            row_copy.setdefault("prediction_job_id", resolved_job_id)
            prepared_rows.append(row_copy)
        if not prepared_rows:
            return
        self._append_rows(prepared_rows, target="prediction_results")

    def _sort_prediction_result_dicts(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def _sort_key(item: Dict[str, Any]):
            raw_completed_at = item.get("completed_at")
            try:
                completed_at = datetime.fromisoformat(str(raw_completed_at))
            except (TypeError, ValueError):
                completed_at = datetime.min
            return (completed_at, str(item.get("user_id") or ""))

        return sorted(items, key=_sort_key, reverse=True)

    def list_prediction_results(self, job_id: str, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, int(page_size))
        offset = (page - 1) * page_size

        if self.mode == "gcp":
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_id", "STRING", str(job_id))
                ]
            )
            count_query = (
                f"SELECT COUNT(*) AS total FROM `{self._prediction_results_table_id}` "
                "WHERE CAST(prediction_job_id AS STRING) = @job_id"
            )
            row_query = (
                f"SELECT * FROM `{self._prediction_results_table_id}` "
                "WHERE CAST(prediction_job_id AS STRING) = @job_id"
            )
            total = int(next(iter(self._client.query(count_query, job_config=job_config).result()))["total"])
            items = [self._deserialize_prediction_row(dict(row.items())) for row in self._client.query(row_query, job_config=job_config).result()]
            items = self._sort_prediction_result_dicts(items)[offset: offset + page_size]
            return {"page": page, "page_size": page_size, "total": total, "items": items}

        if self._prediction_results_table.empty:
            return {"page": page, "page_size": page_size, "total": 0, "items": []}
        table = self._prediction_results_table.copy()
        if "prediction_job_id" not in table.columns:
            return {"page": page, "page_size": page_size, "total": 0, "items": []}
        table = table[
            table["prediction_job_id"].map(lambda value: str(value) == str(job_id) if pd.notna(value) else False)
        ].copy()
        if table.empty:
            return {"page": page, "page_size": page_size, "total": 0, "items": []}
        total = len(table)
        items = [self._deserialize_prediction_row(row) for row in table.to_dict(orient="records")]
        items = self._sort_prediction_result_dicts(items)[offset: offset + page_size]
        return {"page": page, "page_size": page_size, "total": total, "items": items}

    def _deserialize_prediction_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        parsed = dict(row)
        for key, value in list(parsed.items()):
            if not isinstance(value, str):
                continue
            if not value.startswith("{") and not value.startswith("["):
                continue
            try:
                parsed[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
        return parsed

    def delete_data_for_job(self, job_identifier: str):
        if self.mode == "gcp":
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_identifier", "STRING", job_identifier)
                ]
            )
            queries = [
                f"""
                    DELETE FROM `{self._table_id}`
                    WHERE job_identifier = @job_identifier OR job_id = @job_identifier
                """,
                f"""
                    DELETE FROM `{self._dead_letter_table_id}`
                    WHERE job_identifier = @job_identifier OR job_id = @job_identifier
                """,
            ]
            for query in queries:
                self._client.query(query, job_config=job_config).result()
            self.run_events_curation()
            self.refresh_player_latest_state()
            print(f"Deleted rows from BigQuery for job '{job_identifier}'.")
            return

        target_tables = [
            ("_table", self._cache_path, "events_staging"),
            ("_dead_letter_table", self._dead_letter_cache_path, "pipeline_dead_letters"),
        ]
        for table_attr, cache_path, target_name in target_tables:
            table = getattr(self, table_attr)
            if table.empty:
                continue

            masks = []
            if "job_identifier" in table.columns:
                masks.append(table["job_identifier"].map(lambda value: str(value) == str(job_identifier) if pd.notna(value) else False))
            if "job_id" in table.columns:
                masks.append(table["job_id"].map(lambda value: str(value) == str(job_identifier) if pd.notna(value) else False))
            if not masks:
                continue

            combined_mask = masks[0]
            for mask in masks[1:]:
                combined_mask = combined_mask | mask

            initial_rows = len(table)
            filtered = table[~combined_mask].copy()
            rows_deleted = initial_rows - len(filtered)
            if rows_deleted > 0:
                setattr(self, table_attr, filtered)
                print(f"Deleted {rows_deleted} rows from local BigQuery mock target '{target_name}' for job '{job_identifier}'.")
                self._persist_mock_table(filtered, cache_path)

        self.run_events_curation()
        self.refresh_player_latest_state()
