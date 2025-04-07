#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from pydantic import Field, HttpUrl, AfterValidator, BaseModel, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Union, Annotated, Self, List, Dict, Any, Callable
from dremioai.config.tools import ToolType
from enum import auto, StrEnum
from pathlib import Path
from yaml import safe_load
from functools import reduce
from operator import ior
from shutil import which
from contextvars import ContextVar, copy_context
from os import environ
from importlib.util import find_spec


def _resolve_tools_settings(server_mode: Union[ToolType, int, str]) -> ToolType:
    if isinstance(server_mode, str):
        try:
            server_mode = reduce(
                ior, [ToolType[m.upper()] for m in server_mode.split(",")]
            )
        except KeyError:
            return _resolve_tools_settings(int(server_mode))

    if isinstance(server_mode, int):
        return ToolType(server_mode)

    return server_mode


class Tools(BaseModel):
    server_mode: Annotated[
        Optional[Union[ToolType, int, str]], AfterValidator(_resolve_tools_settings)
    ] = Field(default=ToolType.FOR_SELF)


class DremioCloudUri(StrEnum):
    PROD = auto()
    PRODEMEA = auto()


def _resolve_dremio_uri(
    uri: Union[str, DremioCloudUri, HttpUrl],
) -> Union[HttpUrl, str]:
    if isinstance(uri, str):
        try:
            uri = DremioCloudUri[uri.upper()]
        except KeyError:
            uri = HttpUrl(uri)

    if isinstance(uri, DremioCloudUri):
        match uri:
            case DremioCloudUri.PROD:
                return f"https://api.dremio.cloud"
            case DremioCloudUri.PRODEMEA:
                return f"https://api.eu.dremio.cloud"
        return uri

    elif isinstance(uri, HttpUrl):
        uri = str(uri)

    return uri.rstrip("/")


def _resolve_token_file(pat: str) -> str:
    return (
        Path(pat[1:]).expanduser().read_text().strip() if pat.startswith("@") else pat
    )


class Model(StrEnum):
    ollama = auto()
    openai = auto()


class Dremio(BaseModel):
    uri: Annotated[
        Union[str, HttpUrl, DremioCloudUri], AfterValidator(_resolve_dremio_uri)
    ]
    pat: Annotated[str, AfterValidator(_resolve_token_file)]
    project_id: Optional[str] = None
    model_config = ConfigDict(validate_assignment=True)


class OpenAi(BaseModel):
    api_key: Annotated[str, AfterValidator(_resolve_token_file)] = None
    model: Optional[str] = Field(default="gpt-4o")
    org: Optional[str] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class Ollama(BaseModel):
    model: Optional[str] = Field(default="llama3.1")
    model_config = ConfigDict(validate_assignment=True)


class LangChain(BaseModel):
    llm: Optional[Model] = None
    openai: Optional[OpenAi] = Field(default_factory=OpenAi)
    ollama: Optional[Ollama] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class Prometheus(BaseModel):
    uri: Union[HttpUrl, str]
    token: str
    model_config = ConfigDict(validate_assignment=True)


def _resolve_executable(executable: str) -> str:
    executable = Path(executable).expanduser()
    if not executable.is_absolute():
        if (c := which(executable)) is not None:
            executable = Path(c)
    executable = executable.resolve()
    if not executable.is_file():
        raise FileNotFoundError(f"Command {executable} not found.")
    return str(executable)


class MCPServer(BaseModel):
    command: Annotated[str, AfterValidator(_resolve_executable)]
    args: Optional[List[str]] = Field(default_factory=list)
    env: Optional[Dict[str, str]] = Field(default_factory=dict)
    model_config = ConfigDict(validate_assignment=True)


class Anthropic(BaseModel):
    api_key: Annotated[str, AfterValidator(_resolve_token_file)] = None
    chat_model: Optional[str] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class BeeAI(BaseModel):
    mcp_server: Optional[MCPServer] = Field(default=None, alias="mcpServer")
    sliding_memory_size: Optional[int] = Field(default=10, alias="slidingMemorySize")
    anthropic: Optional[Anthropic] = Field(default=None)
    openai: Optional[OpenAi] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class Settings(BaseSettings):
    dremio: Optional[Dremio] = Field(default=None)
    tools: Optional[Tools] = Field(default_factory=Tools)
    prometheus: Optional[Prometheus] = Field(default=None)
    langchain: Optional[LangChain] = Field(default=None)
    beeai: Optional[BeeAI] = Field(default=None)
    model_config = SettingsConfigDict(
        env_file=".env", env_nested_delimiter="_", env_extra="allow"
    )

    def with_overrides(self, overrides: Dict[str, Any]) -> Self:
        def set_values(aparts: List[str], value: Any, obj: Any):
            if len(aparts) == 1 and hasattr(obj, aparts[0]):
                setattr(obj, aparts[0], value)
            elif hasattr(obj, aparts[0]):
                set_values(aparts[1:], value, getattr(obj, aparts[0]))

        for aparts, value in [
            (attr.split("."), value)
            for attr, value in overrides.items()
            if value is not None
        ]:
            set_values(aparts, value, self)

        return self


_settings: ContextVar[Settings] = ContextVar("settings", default=None)


# the default config is ~/.config/dremioai/config.yaml, use it if it exists
def default_config() -> Path:
    _top = "dremioai"
    if (_top := find_spec(__name__)) and _top.name:
        _top = _top.name.split(".")[0]
    return (
        Path(environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        / _top
        / "config.yaml"
    )


# configures the settings using the given config file and overwrites the global
# settings instance if force is True
def configure(cfg: Union[str, Path] = None, force=False) -> ContextVar[Settings]:
    global _settings
    if force and isinstance(_settings.get(), Settings):
        old = _settings.get()
        try:
            _settings.set(None)
            configure(cfg, force=False)
        except:
            # don't replace the old if there is an issue setting the new value
            _settings.set(old)
            raise

    if isinstance(cfg, str):
        cfg = Path(cfg)

    if cfg is None:
        cfg = default_config()

    with cfg.open() as f:
        s = safe_load(f)
        _settings.set(Settings.model_validate(s))

    return _settings


# Get the current settings instance if one has been configured. If not try
# to configure it using the default config file. If that fails, create a new
# empty settings instance.
def instance() -> Settings | None:
    global _settings
    if not isinstance(_settings.get(), Settings):
        try:
            configure()  # use default config, if exists
        except FileNotFoundError:
            # no default config, create a new default one
            _settings.set(Settings())
    return _settings.get()


async def run_with(
    func: Callable,
    overrides: Optional[Dict[str, Any]] = {},
    args: Optional[List[Any]] = [],
    kw: Optional[Dict[str, Any]] = {},
) -> Any:
    global _settings

    async def _call():
        tok = _settings.set(instance().model_copy(deep=True).with_overrides(overrides))
        try:
            return await func(*args, **kw)
        finally:
            _settings.reset(tok)

    ctx = copy_context()
    return await _call()
