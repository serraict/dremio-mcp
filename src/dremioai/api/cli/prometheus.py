#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from typing import Annotated, Optional, List
from typer import Option, Argument, Typer, BadParameter
import asyncio
from rich import print as pp

from dremioai.api.prometheus import vm
from dremioai.config import settings

app = Typer(
    no_args_is_help=True,
    name="metrics",
    help="Run commands related to prometheus metrics",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@app.command("list")
def list_labels_or_values(
    uri: Annotated[
        Optional[str],
        Option(envvar="PROMETHEUS_URI", show_envvar=True, help="The prometheus URI"),
    ] = None,
    token: Annotated[
        Optional[str],
        Option(
            envvar="PROMETHEUS_TOKEN", show_envvar=True, help="The prometheus token"
        ),
    ] = None,
    metric_name: Annotated[
        Optional[str],
        Option(help="Get schema of this metric", rich_help_panel="One of"),
    ] = None,
    label: Annotated[
        Optional[str],
        Option(help="Get values for this label", rich_help_panel="One of"),
    ] = None,
    use_df: Annotated[
        Optional[bool],
        Option(help="Convert results to pandas dataframe"),
    ] = False,
):
    cfg = (
        settings.instance()
        .get()
        .with_overrides({"prometheus.uri": uri, "prometheus.token": token})
    )
    if metric_name is None and label is None:
        raise BadParameter("Either --metric-name or --label must be provided")

    if metric_name is not None:
        result = asyncio.run(vm.get_metrics_schema(metric_name, use_df=use_df))
    else:
        result = asyncio.run(vm.get_label_values(label, use_df=use_df))
    pp(result)


@app.command("query")
def list_labels_or_values(
    query: Annotated[str, Option(help="The promql query")],
    uri: Annotated[
        Optional[str],
        Option(envvar="PROMETHEUS_URI", show_envvar=True, help="The prometheus URI"),
    ] = None,
    token: Annotated[
        Optional[str],
        Option(
            envvar="PROMETHEUS_TOKEN", show_envvar=True, help="The prometheus token"
        ),
    ] = None,
    start: Annotated[
        Optional[str],
        Option(help="The start time"),
    ] = None,
    step: Annotated[
        Optional[str],
        Option(help="The step time"),
    ] = None,
    use_df: Annotated[
        Optional[bool],
        Option(help="Convert results to pandas dataframe"),
    ] = False,
):
    cfg = (
        settings.instance()
        .get()
        .with_overrides({"prometheus.uri": uri, "prometheus.token": token})
    )
    result = asyncio.run(
        vm.get_promql_result(query, start=start, step=step, use_df=use_df)
    )
    pp(result)
