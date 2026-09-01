"""Unit tests for MCP workflow scorers that consume sandboxed-agent logs."""

from __future__ import annotations

import json

from parameterized import parameterized

from products.posthog_ai.evals.cli_mcp.scorers import DidNotCiteRawSqlAfterTypedQuery, FirstRelevantTool


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


@parameterized.expand(
    [
        ("hogql_tag", 'Here is the trend: <hogql label="pageviews">SELECT count() FROM events</hogql>'),
        ("sql_fence", "Here is the trend:\n```sql\nSELECT count() FROM events\n```"),
    ]
)
def test_did_not_cite_raw_sql_fails_when_answer_hand_writes_sql_after_typed_query(
    _name: str, last_message: str
) -> None:
    raw_log = "\n".join(_tool_call("trends", "call query-trends {}"))

    result = DidNotCiteRawSqlAfterTypedQuery()._run_eval_sync(
        {"raw_log": raw_log, "last_message": last_message},
        expected={"did_not_cite_raw_sql_after_typed_query": {"tool": "query-trends"}},
    )

    assert result.score == 0.0


def test_did_not_cite_raw_sql_passes_when_answer_trusts_the_tool_result() -> None:
    raw_log = "\n".join(_tool_call("trends", "call query-trends {}"))

    result = DidNotCiteRawSqlAfterTypedQuery()._run_eval_sync(
        {"raw_log": raw_log, "last_message": "Pageviews were up 12% over the last 7 days."},
        expected={"did_not_cite_raw_sql_after_typed_query": {"tool": "query-trends"}},
    )

    assert result.score == 1.0


def test_did_not_cite_raw_sql_is_none_when_the_typed_tool_was_never_called() -> None:
    raw_log = "\n".join(_tool_call("sql", "call execute-sql {}"))

    result = DidNotCiteRawSqlAfterTypedQuery()._run_eval_sync(
        {"raw_log": raw_log, "last_message": "<hogql>SELECT count() FROM events</hogql>"},
        expected={"did_not_cite_raw_sql_after_typed_query": {"tool": "query-trends"}},
    )

    assert result.score is None
