#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from asyncio import Semaphore, gather
from typing import List, Awaitable
from enum import StrEnum


class UStrEnum(StrEnum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


async def run_in_parallel(coroutines: List[Awaitable], max_concurrent_tasks: int = 10):
    semaphore = Semaphore(max_concurrent_tasks)

    async def sem_task(coroutine):
        async with semaphore:
            return await coroutine

    return await gather(*(sem_task(coroutine) for coroutine in coroutines))
