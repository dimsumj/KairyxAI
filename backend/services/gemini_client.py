# gemini_client.py

import os
import json
import hashlib
from datetime import datetime
import google.generativeai as genai

class GeminiClient:
    """
    A client to interact with the Google Gemini API.
    """

    def __init__(self):
        """
        Initializes the Gemini client and configures it with an API key.
        """
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY environment variable must be set.")
        
        genai.configure(api_key=self.api_key)
        # Use the model from environment variable, or default to 'gemini-2.5-flash'
        model_name = os.getenv("GOOGLE_GEMINI_MODEL", 'gemini-2.5-flash')
        self.model_name = model_name
        self.model = genai.GenerativeModel(model_name)
        self._cache_path = ".cache/llm_response_cache.json"
        self._usage_path = ".cache/llm_usage.json"
        self._cache_ttl_seconds = int(os.getenv("AI_RESPONSE_CACHE_TTL_SEC", "21600"))
        self._daily_token_limit = self._read_int_env("AI_DAILY_TOKEN_LIMIT")
        self._monthly_token_limit = self._read_int_env("AI_MONTHLY_TOKEN_LIMIT")
        self._daily_budget_limit = self._read_float_env("AI_DAILY_BUDGET_LIMIT_USD")
        self._monthly_budget_limit = self._read_float_env("AI_MONTHLY_BUDGET_LIMIT_USD")
        # Approximate cost per 1K total tokens (input + output) for budget enforcement.
        self._usd_per_1k_tokens = float(os.getenv("AI_COST_PER_1K_TOKENS_USD", "0.002"))
        
        print(f"GeminiClient initialized successfully with model: {model_name}")

    def get_ai_response(self, prompt: str) -> str:
        """
        Sends a prompt to the Gemini model and returns the text response.

        Args:
            prompt: The input prompt for the AI model.

        Returns:
            The generated text response from the model.
        """
        prompt_hash = self._prompt_hash(prompt)
        cached = self._get_cached_response(prompt_hash)
        if cached is not None:
            return cached

        estimated_input_tokens = self._estimate_tokens(prompt)
        self._enforce_limits(estimated_input_tokens)

        response = self.model.generate_content(prompt)
        response_text = response.text or ""
        estimated_output_tokens = self._estimate_tokens(response_text)
        total_tokens = estimated_input_tokens + estimated_output_tokens

        self._record_usage(total_tokens)
        self._set_cached_response(prompt_hash, response_text)
        return response_text

    def get_usage_snapshot(self) -> dict:
        """Returns current daily/monthly usage counters and configured limits."""
        usage = self._load_usage()
        self._rollover_usage_if_needed(usage)
        self._save_usage(usage)
        return {
            "model_name": self.model_name,
            "daily_tokens_used": usage.get("daily_tokens_used", 0),
            "monthly_tokens_used": usage.get("monthly_tokens_used", 0),
            "daily_budget_used_usd": round(usage.get("daily_budget_used_usd", 0.0), 6),
            "monthly_budget_used_usd": round(usage.get("monthly_budget_used_usd", 0.0), 6),
            "limits": {
                "daily_token_limit": self._daily_token_limit,
                "monthly_token_limit": self._monthly_token_limit,
                "daily_budget_limit_usd": self._daily_budget_limit,
                "monthly_budget_limit_usd": self._monthly_budget_limit,
            },
            "cache_ttl_seconds": self._cache_ttl_seconds,
        }

    def _read_int_env(self, key: str):
        value = os.getenv(key)
        if value is None or value == "":
            return None
        try:
            parsed = int(value)
            return parsed if parsed >= 0 else None
        except ValueError:
            return None

    def _read_float_env(self, key: str):
        value = os.getenv(key)
        if value is None or value == "":
            return None
        try:
            parsed = float(value)
            return parsed if parsed >= 0 else None
        except ValueError:
            return None

    def _prompt_hash(self, prompt: str) -> str:
        digest = hashlib.sha256()
        digest.update(self.model_name.encode("utf-8"))
        digest.update(b":")
        digest.update(prompt.encode("utf-8"))
        return digest.hexdigest()

    def _load_cache(self) -> dict:
        if not os.path.exists(self._cache_path):
            return {}
        try:
            with open(self._cache_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def _save_cache(self, cache: dict):
        os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
        with open(self._cache_path, "w") as f:
            json.dump(cache, f)

    def _get_cached_response(self, prompt_hash: str):
        cache = self._load_cache()
        entry = cache.get(prompt_hash)
        if not entry:
            return None

        created_at = entry.get("created_at")
        if not created_at:
            return None
        try:
            created_dt = datetime.fromisoformat(created_at)
        except ValueError:
            return None

        age_seconds = (datetime.utcnow() - created_dt).total_seconds()
        if age_seconds > self._cache_ttl_seconds:
            return None
        return entry.get("response")

    def _set_cached_response(self, prompt_hash: str, response: str):
        cache = self._load_cache()
        cache[prompt_hash] = {
            "created_at": datetime.utcnow().isoformat(),
            "response": response,
        }
        self._save_cache(cache)

    def _estimate_tokens(self, text: str) -> int:
        # Approximation for quick budget protection: 1 token ~= 4 chars.
        if not text:
            return 0
        return max(1, int(len(text) / 4))

    def _load_usage(self) -> dict:
        if not os.path.exists(self._usage_path):
            return {
                "daily_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "monthly_period": datetime.utcnow().strftime("%Y-%m"),
                "daily_tokens_used": 0,
                "monthly_tokens_used": 0,
                "daily_budget_used_usd": 0.0,
                "monthly_budget_used_usd": 0.0,
            }
        try:
            with open(self._usage_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {
                "daily_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "monthly_period": datetime.utcnow().strftime("%Y-%m"),
                "daily_tokens_used": 0,
                "monthly_tokens_used": 0,
                "daily_budget_used_usd": 0.0,
                "monthly_budget_used_usd": 0.0,
            }

    def _save_usage(self, usage: dict):
        os.makedirs(os.path.dirname(self._usage_path), exist_ok=True)
        with open(self._usage_path, "w") as f:
            json.dump(usage, f, indent=2)

    def _rollover_usage_if_needed(self, usage: dict):
        now = datetime.utcnow()
        daily_date = now.strftime("%Y-%m-%d")
        monthly_period = now.strftime("%Y-%m")

        if usage.get("daily_date") != daily_date:
            usage["daily_date"] = daily_date
            usage["daily_tokens_used"] = 0
            usage["daily_budget_used_usd"] = 0.0
        if usage.get("monthly_period") != monthly_period:
            usage["monthly_period"] = monthly_period
            usage["monthly_tokens_used"] = 0
            usage["monthly_budget_used_usd"] = 0.0

    def _enforce_limits(self, incoming_tokens: int):
        usage = self._load_usage()
        self._rollover_usage_if_needed(usage)

        projected_daily_tokens = usage.get("daily_tokens_used", 0) + incoming_tokens
        projected_monthly_tokens = usage.get("monthly_tokens_used", 0) + incoming_tokens
        projected_daily_budget = usage.get("daily_budget_used_usd", 0.0) + (incoming_tokens / 1000.0) * self._usd_per_1k_tokens
        projected_monthly_budget = usage.get("monthly_budget_used_usd", 0.0) + (incoming_tokens / 1000.0) * self._usd_per_1k_tokens

        if self._daily_token_limit is not None and projected_daily_tokens > self._daily_token_limit:
            raise RuntimeError("AI daily token limit reached.")
        if self._monthly_token_limit is not None and projected_monthly_tokens > self._monthly_token_limit:
            raise RuntimeError("AI monthly token limit reached.")
        if self._daily_budget_limit is not None and projected_daily_budget > self._daily_budget_limit:
            raise RuntimeError("AI daily budget limit reached.")
        if self._monthly_budget_limit is not None and projected_monthly_budget > self._monthly_budget_limit:
            raise RuntimeError("AI monthly budget limit reached.")

        self._save_usage(usage)

    def _record_usage(self, total_tokens: int):
        usage = self._load_usage()
        self._rollover_usage_if_needed(usage)
        usage["daily_tokens_used"] = int(usage.get("daily_tokens_used", 0) + total_tokens)
        usage["monthly_tokens_used"] = int(usage.get("monthly_tokens_used", 0) + total_tokens)

        estimated_cost = (total_tokens / 1000.0) * self._usd_per_1k_tokens
        usage["daily_budget_used_usd"] = float(usage.get("daily_budget_used_usd", 0.0) + estimated_cost)
        usage["monthly_budget_used_usd"] = float(usage.get("monthly_budget_used_usd", 0.0) + estimated_cost)
        self._save_usage(usage)
