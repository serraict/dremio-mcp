#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from enum import IntFlag, auto


class ToolType(IntFlag):
    FOR_SELF = auto()  # introspecting dremio cluster and its usage pattersn
    FOR_PROMETHEUS = (
        auto()
    )  # supporting any prometheus stack setup to in conjuction with dremio
    FOR_DATA_PATTERNS = (
        auto()
    )  # discovering data patterns analysis within your data using dremio's semantic layer
