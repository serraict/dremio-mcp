#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from pydantic import BaseModel, Field, AfterValidator, ValidationError
from typing import Annotated, List, Dict, AnyStr, Any, Union, Optional
from datetime import datetime
from enum import StrEnum, auto
from functools import partial

from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
from dremioai.api.util import UStrEnum, run_in_parallel
from dremioai.config import settings
from csv import reader, excel
from io import StringIO


class CatalogItemType(UStrEnum):
    DATASET = auto()
    CONTAINER = auto()
    FILE = auto()


class ContainerSubType(UStrEnum):
    SPACE = auto()
    SOURCE = auto()
    FOLDER = auto()
    HOME = auto()
    FUNCTION = auto()


class DatasetSubType(UStrEnum):
    VIRTUAL = auto()
    PROMOTED = auto()
    DIRECT = auto()


def subset_validator(elem: UStrEnum, values: List[UStrEnum]) -> UStrEnum:
    if elem in values:
        return elem
    raise ValidationError(f"{elem} not in {values}")


class LineageBase(BaseModel):
    id: AnyStr
    path: List[str]
    tag: AnyStr
    created_at: Optional[datetime] = Field(..., alias="createdAt")


class LineageSource(LineageBase):
    type: Annotated[
        CatalogItemType,
        AfterValidator(partial(subset_validator, values=[CatalogItemType.CONTAINER])),
    ]
    container_type: Annotated[
        ContainerSubType,
        AfterValidator(
            partial(
                subset_validator,
                values=[ContainerSubType.HOME, ContainerSubType.SOURCE],
            )
        ),
    ] = Field(..., alias="containerType")


class LineageParents(LineageBase):
    type: Annotated[
        CatalogItemType,
        AfterValidator(partial(subset_validator, values=[CatalogItemType.DATASET])),
    ]
    dataset_type: Annotated[
        DatasetSubType,
        AfterValidator(
            partial(
                subset_validator,
                values=[DatasetSubType.PROMOTED, DatasetSubType.VIRTUAL],
            )
        ),
    ] = Field(..., alias="datasetType")


class LineageChildren(LineageBase):
    type: Annotated[
        CatalogItemType,
        AfterValidator(partial(subset_validator, values=[CatalogItemType.DATASET])),
    ]
    dataset_type: Annotated[
        DatasetSubType,
        AfterValidator(partial(subset_validator, values=[DatasetSubType.VIRTUAL])),
    ] = Field(..., alias="datasetType")


class LineageResponse(BaseModel):
    sources: List[LineageSource]
    parents: List[LineageParents]
    children: List[LineageChildren]


async def get_lineage(dataset_id_or_path: str) -> str:
    client = AsyncHttpClient()
    if "." in dataset_id_or_path:
        response = await get_schema(dataset_id_or_path, by_id=False)
        dataset_id_or_path = response["id"]

    project_id = settings.instance().dremio.project_id
    endpoint = f"/v0/projects/{project_id}/catalog" if project_id else "/api/v3/catalog"
    result: LineageResponse = await client.get(
        f"{endpoint}/{dataset_id_or_path}/graph",
        deser=LineageResponse,
    )
    return result.model_dump_json()


async def get_schema(
    dataset_path_or_id: Optional[Union[List[str], str]],
    by_id: Optional[bool] = False,
    include_tags: Optional[bool] = False,
) -> Dict[str, Any]:
    client = AsyncHttpClient()
    project_id = settings.instance().dremio.project_id
    endpoint = f"/v0/projects/{project_id}/catalog" if project_id else "/api/v3/catalog"
    if by_id:
        endpoint += dataset_path_or_id
    else:
        if isinstance(dataset_path_or_id, str):
            dataset_path_or_id = list(
                reader(StringIO(dataset_path_or_id), delimiter=".", dialect=excel)
            )[0]
        endpoint += f'/by-path/{"/".join(dataset_path_or_id)}'
    schema = await client.get(endpoint)

    if include_tags:

        async def _get(id, suffix):
            client = AsyncHttpClient()
            try:
                ep = (
                    f"/v0/projects/{project_id}/catalog"
                    if project_id
                    else "/api/v3/catalog"
                )
                result = await client.get(f"{ep}/{id}/collaboration/{suffix}")
                return {suffix: result}
            except:
                return {}

        extras = ("tag", "wiki")
        results = await run_in_parallel(
            [_get(schema["id"], s) for s in ("tag", "wiki")]
        )
        for i, r in enumerate(extras):
            schema.update(results[i][r])
    return schema


async def get_schemas(
    dataset_path_or_ids: List[Union[List[str], str]],
    by_id: Optional[bool] = False,
    include_tags: Optional[bool] = False,
) -> Dict[str, Any]:

    return await run_in_parallel(
        [get_schema(p, by_id, include_tags) for p in dataset_path_or_ids]
    )
