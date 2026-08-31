"""Unit tests for MCP workflow scorers that consume sandboxed-agent logs."""

from __future__ import annotations

import json

from products.posthog_ai.evals.cli_mcp.scorers import FirstRelevantTool


def _tool_call(call_id: str, command: str) -> list[str]:
    return [
        json.dumps(
            {
                "notification": {
                    "method": "session/update",
                    "params": {
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": call_id,
                            "rawInput": {"command": command},
                            "_meta": {"claudeCode": {"toolName": "mcp__posthog__exec"}},
                        }
                    },
                }
            }
        ),
        json.dumps(
            {
                "notification": {
                    "method": "session/update",
                    "params": {
                        "update": {
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": call_id,
                            "status": "completed",
                            "rawOutput": "ok",
                        }
                    },
                }
            }
        ),
    ]


ANALYSIS_QUERY_TOOLS = frozenset({"query-trends", "query-funnel", "query-retention", "execute-sql"})


def test_first_relevant_tool_passes_when_sql_validates_a_typed_query() -> None:
    raw_log = "\n".join(
        [
            *_tool_call("retention", "call query-retention {}"),
            *_tool_call("sql", "call execute-sql {}"),
        ]
    )

    result = FirstRelevantTool(relevant_tools=ANALYSIS_QUERY_TOOLS)._run_eval_sync(
        {"raw_log": raw_log},
        expected={"first_relevant_tool": {"tool": "query-retention"}},
    )

    assert result.score == 1.0
    assert result.metadata["first_relevant_tool"] == "query-retention"


def test_first_relevant_tool_fails_when_sql_is_selected_before_the_typed_query() -> None:
    raw_log = "\n".join(
        [
            *_tool_call("sql", "call execute-sql {}"),
            *_tool_call("retention", "call query-retention {}"),
        ]
    )

    result = FirstRelevantTool(relevant_tools=ANALYSIS_QUERY_TOOLS)._run_eval_sync(
        {"raw_log": raw_log},
        expected={"first_relevant_tool": {"tool": "query-retention"}},
    )

    assert result.score == 0.0
