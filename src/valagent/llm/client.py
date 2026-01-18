"""
LLM Client implementation with support for multiple providers.
Provides a unified interface for interacting with different LLM services.
"""

import json
import logging
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any

import httpx

from valagent.config import get_settings

logger = logging.getLogger(__name__)


class BaseLLMClient(ABC):
    """Base class for LLM clients."""

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict | None = None,
    ) -> str:
        """Generate a response from the LLM."""
        pass

    @abstractmethod
    async def generate_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict | None = None,
    ) -> dict:
        """Generate a JSON response from the LLM."""
        pass


class OpenAIClient(BaseLLMClient):
    """OpenAI API client."""

    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.llm.openai_api_key.get_secret_value()
        self.model = self.settings.llm.openai_model
        self.base_url = self.settings.llm.openai_base_url or "https://api.openai.com/v1"
        self.timeout = self.settings.llm.llm_timeout

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict | None = None,
    ) -> str:
        """Generate a response from OpenAI."""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature or self.settings.llm.llm_temperature,
            "max_tokens": max_tokens or self.settings.llm.llm_max_tokens,
        }

        if response_format:
            payload["response_format"] = response_format

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    async def generate_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict | None = None,
    ) -> dict:
        """Generate a JSON response from OpenAI."""
        response = await self.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            response_format={"type": "json_object"},
        )
        return json.loads(response)


class AzureOpenAIClient(BaseLLMClient):
    """Azure OpenAI API client."""

    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.llm.azure_openai_api_key.get_secret_value()
        self.endpoint = self.settings.llm.azure_openai_endpoint
        self.deployment = self.settings.llm.azure_openai_deployment
        self.api_version = self.settings.llm.azure_openai_api_version
        self.timeout = self.settings.llm.llm_timeout

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict | None = None,
    ) -> str:
        """Generate a response from Azure OpenAI."""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "messages": messages,
            "temperature": temperature or self.settings.llm.llm_temperature,
            "max_tokens": max_tokens or self.settings.llm.llm_max_tokens,
        }

        if response_format:
            payload["response_format"] = response_format

        url = (
            f"{self.endpoint}/openai/deployments/{self.deployment}"
            f"/chat/completions?api-version={self.api_version}"
        )

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                url,
                headers={
                    "api-key": self.api_key,
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    async def generate_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict | None = None,
    ) -> dict:
        """Generate a JSON response from Azure OpenAI."""
        response = await self.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            response_format={"type": "json_object"},
        )
        return json.loads(response)


class AnthropicClient(BaseLLMClient):
    """Anthropic Claude API client."""

    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.llm.anthropic_api_key.get_secret_value()
        self.model = self.settings.llm.anthropic_model
        self.base_url = "https://api.anthropic.com/v1"
        self.timeout = self.settings.llm.llm_timeout

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict | None = None,
    ) -> str:
        """Generate a response from Anthropic Claude."""
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature or self.settings.llm.llm_temperature,
            "max_tokens": max_tokens or self.settings.llm.llm_max_tokens,
        }

        if system_prompt:
            payload["system"] = system_prompt

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/messages",
                headers={
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return data["content"][0]["text"]

    async def generate_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict | None = None,
    ) -> dict:
        """Generate a JSON response from Anthropic Claude."""
        json_prompt = f"""{prompt}

IMPORTANT: Respond ONLY with valid JSON. Do not include any markdown formatting, 
code blocks, or explanatory text. Your entire response must be parseable JSON."""

        response = await self.generate(
            prompt=json_prompt,
            system_prompt=system_prompt,
        )

        # Clean up response if needed
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:]
        if response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]

        return json.loads(response.strip())


class OllamaClient(BaseLLMClient):
    """Ollama local LLM client."""

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.llm.ollama_base_url
        self.model = self.settings.llm.ollama_model
        self.timeout = self.settings.llm.llm_timeout

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict | None = None,
    ) -> str:
        """Generate a response from Ollama."""
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature or self.settings.llm.llm_temperature,
                "num_predict": max_tokens or self.settings.llm.llm_max_tokens,
            },
        }

        if system_prompt:
            payload["system"] = system_prompt

        if response_format and response_format.get("type") == "json_object":
            payload["format"] = "json"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/generate",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            return data["response"]

    async def generate_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict | None = None,
    ) -> dict:
        """Generate a JSON response from Ollama."""
        response = await self.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            response_format={"type": "json_object"},
        )
        return json.loads(response)


class LLMClient:
    """
    Unified LLM client that delegates to the appropriate provider.
    Provides a consistent interface regardless of the underlying LLM service.
    """

    def __init__(self):
        self.settings = get_settings()
        self._client: BaseLLMClient | None = None

    def _get_client(self) -> BaseLLMClient:
        """Get or create the appropriate LLM client based on settings."""
        if self._client is not None:
            return self._client

        provider = self.settings.llm.llm_provider

        if provider == "openai":
            self._client = OpenAIClient()
        elif provider == "azure_openai":
            self._client = AzureOpenAIClient()
        elif provider == "anthropic":
            self._client = AnthropicClient()
        elif provider == "ollama":
            self._client = OllamaClient()
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")

        logger.info(f"Initialized LLM client for provider: {provider}")
        return self._client

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict | None = None,
    ) -> str:
        """Generate a response from the configured LLM provider."""
        client = self._get_client()
        return await client.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
        )

    async def generate_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict | None = None,
    ) -> dict:
        """Generate a JSON response from the configured LLM provider."""
        client = self._get_client()
        return await client.generate_json(
            prompt=prompt,
            system_prompt=system_prompt,
            schema=schema,
        )


_llm_client: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """Get or create the LLM client singleton."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client
