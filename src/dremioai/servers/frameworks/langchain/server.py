#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#


from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import ToolMessage, AIMessage

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML

from dremioai.tools.tools import get_tools, get_resources
from pathlib import Path
from typing import Dict

from typing import List, Optional, Annotated
from dremioai import log
from dremioai.config import settings
from typer import Typer, Option
from rich import print as pp
from dremioai.servers.frameworks.langchain.tools import discover_tools, discover_prompt

# def system_prompt():
#     return """
#     You are helpful AI bot with access to several tools for analyzing Dremio cluster and jobs.
#     The information about jobs is stored in a table called sys.project.jobs_recent.
#     There is a tool avaialble for each of the following operations:
#     """ + "\n".join(
#         f"{t.__name__}: {'\n\t'.join(t.invoke.__doc__.splitlines())}"
#         for t in (get_tools() + get_resources())
#     )


tl = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="langchain-server",
    help="Support for testing tools directly",
)


@tl.command()
def main(
    dremio_uri: Annotated[
        Optional[str], Option(help="Dremio URI", envvar="DREMIO_URI", show_envvar=True)
    ] = None,
    dremio_pat: Annotated[
        Optional[str], Option(help="Dremio PAT", envvar="DREMIO_PAT", show_envvar=True)
    ] = None,
    project_id: Annotated[
        Optional[str],
        Option(help="Dremio Project ID", envvar="DREMIO_PROJECT_ID", show_envvar=True),
    ] = None,
    config_file: Annotated[
        Optional[Path],
        Option("-c", "--cfg", help="The config yaml for various options"),
    ] = None,
    api_key: Annotated[
        Optional[str],
        Option(help="API Key", envvar="LANGCHAIN_API_KEY", show_envvar=True),
    ] = None,
    openai_org: Annotated[
        Optional[str],
        Option(
            help="The OpenAI org Id", envvar="LANGCHAIN_OPENAI_ORG", show_envvar=True
        ),
    ] = None,
    model: Annotated[
        Optional[settings.Model], Option(help="Model to use", show_default=True)
    ] = settings.Model.ollama,
):

    settings.configure(config_file)
    settings.instance().with_overrides(
        {
            "langchain.llm": model,
            "dremio.uri": dremio_uri,
            "dremio.pat": dremio_pat,
            "dremio.project_id": project_id,
            "langchain.openai.org": openai_org,
            "langchain.openai.api_key": api_key,
        }
    )

    custom_style = Style.from_dict(
        {
            "prompt": "ansicyan",  # Input prompt color
            "error": "ansired",  # Error messages color
        }
    )
    session = PromptSession(
        history=FileHistory(Path("~/.mcp.history").expanduser()), style=custom_style
    )

    match settings.instance().langchain.llm:
        case settings.Model.ollama:
            llm = ChatOllama(
                model=settings.instance().langchain.ollama.model,
                temperature=0,
                verbose=True,
            )
        case _:
            llm = ChatOpenAI(
                model=settings.instance().langchain.openai.model,
                temperature=0,
                verbose=True,
                api_key=settings.instance().langchain.openai.api_key,
            )

    executor = create_react_agent(
        model=llm,
        tools=discover_tools(settings.instance().tools.server_mode),
        prompt=discover_prompt(),
        debug=True,
    )

    # agent = create_openai_tools_agent(
    #     llm=llm, tools=registered_tools, prompt=chat_prompt
    # )

    # agent_executor = AgentExecutor(agent=agent, tools=registered_tools, verbose=True)
    # chain = chat_prompt | llm | StrOutputParser()
    chat_history = []

    while True:
        try:
            user_input = session.prompt(HTML("<prompt>>> </prompt>"))
            if user_input.lower() == "q":
                break
        except EOFError:
            break
        args = {"messages": [("human", user_input)]}
        if chat_history:
            args["chat_history"] = chat_history
        response: Dict[str, List[ToolMessage, AIMessage]] = executor.invoke(args)
        for message in response:
            if isinstance(message, ToolMessage):
                message.pretty_print()

        response["messages"][-1].pretty_print()
        chat_history.extend(
            [("human", user_input), ("system", response["messages"][-1].content)]
        )
        # response = agent_executor.invoke({"input": user_input})
        # if output := response.get("output"):
        # pp(output)
        # else:
        # pp(response)


def cli():
    tl()


if __name__ == "__main__":
    cli()
