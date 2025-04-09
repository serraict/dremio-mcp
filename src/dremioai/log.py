# 
#  Copyright (C) 2017-2025 Dremio Corporation
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

import structlog
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler
from logging import basicConfig


def logger(name=None):
    if not structlog.is_configured():
        configure()
    return structlog.get_logger(name)

basicConfig(level=logging.WARNING,
            filename="/tmp/dremioai.log")

_level = None


def level():
    global _level
    if _level is not None:
        return _level
    return getattr(logging, os.environ.get("LOG_LEVEL", "INFO"), logging.INFO)


def set_level(l):
    _level = l
    # propagate to all loggers
    for name in logging.getLogger().manager.loggerDict:
        logging.getLogger().setLevel(l)


def configure(enable_json_logging=None, to_file=False):
    if enable_json_logging is None:
        enable_json_logging = "JSON_LOGGING" in os.environ
    renderer = (
        structlog.processors.JSONRenderer()
        if enable_json_logging
        else structlog.dev.ConsoleRenderer()
    )
    processors = [
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(),
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
        structlog.processors.EventRenamer("message"),
        renderer,
    ]
    structlog.configure(
        processors=processors,
        context_class=dict,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=(structlog.stdlib.LoggerFactory()),
    )

    logging.getLogger().setLevel(level())
