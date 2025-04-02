from langchain_core.tools.structured import StructuredTool
from langchain_core.tools.base import create_schema_from_function
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from dremioai.tools.tools import Tool, get_tools, ToolType, system_prompt
from typing import Type, List
from asyncio import run


def instantiate(tool_class: Type[Tool]) -> StructuredTool:
    tool_instance = tool_class()
    args_schema = create_schema_from_function(
        tool_class.__name__, tool_instance.invoke, parse_docstring=True
    )
    return StructuredTool.from_function(
        func=lambda *args, **kw: run(tool_instance.invoke(*args, **kw)),
        name=tool_class.__name__,
        description=tool_instance.invoke.__doc__[:1024],
        args_schema=args_schema,
        coroutine=tool_instance.invoke,
        strict=False,
    )


def discover_tools(For: ToolType = None) -> List[StructuredTool]:
    return [instantiate(tool) for tool in get_tools(For=For)]


def discover_prompt() -> ChatPromptTemplate:
    return ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt()),
            (
                "system",
                "You must respond in Markdown or tables in tab separated values format",
            ),
            MessagesPlaceholder("chat_history", optional=True),
            MessagesPlaceholder("messages"),
            MessagesPlaceholder("agent_scratchpad", optional=True),
        ]
    )
