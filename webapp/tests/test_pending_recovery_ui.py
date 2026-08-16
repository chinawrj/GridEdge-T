from __future__ import annotations

import ast
import inspect

import gridedge_web.app as app


def test_dashboard_discovers_and_retries_pending_commands_without_rebuilding_requests() -> (
    None
):
    source = inspect.getsource(app)
    tree = ast.parse(source)
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "pending_commands" in called_attributes
    assert "retry_pending" in called_attributes
    assert "待恢复命令" in source
    assert "重试原命令" in source

    retry_functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and "retry" in node.name
        and "pending" in node.name
    ]
    assert retry_functions, "dashboard has no explicit pending-receipt retry handler"
    retry_calls = {
        node.func.attr
        for function in retry_functions
        for node in ast.walk(function)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "retry_pending" in retry_calls
    assert (
        "command" not in retry_calls
    ), "pending recovery must use only run_id/request_id; the UI rebuilt a command envelope"


def test_dashboard_exposes_a_finish_button_bound_to_the_durable_finish_command() -> (
    None
):
    tree = ast.parse(inspect.getsource(app))
    finish_buttons = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "button"
        and node.value.args
        and isinstance(node.value.args[0], ast.Constant)
        and node.value.args[0].value == "运行至结束"
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert (
        finish_buttons
    ), "modern dashboard does not render the promised FINISH control"

    finish_bindings = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "on_click"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in finish_buttons
    ]
    assert finish_bindings, "FINISH control is not bound to a click handler"
    assert any(
        isinstance(call.func, ast.Name)
        and call.func.id == "run_command"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "finish"
        for binding in finish_bindings
        for call in ast.walk(binding)
        if isinstance(call, ast.Call)
    ), "FINISH control bypasses the durable command dispatcher"
