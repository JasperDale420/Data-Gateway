"""Tests for scripts.generate_provider_contract helpers."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_contract_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "generate_provider_contract.py"
    spec = importlib.util.spec_from_file_location("generate_provider_contract_script", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load generate_provider_contract module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


contract = _load_contract_module()


def test_index_function_defs_collects_names_and_positions() -> None:
    text = """
@router.get("/one")
async def first_handler():
    pass

@router.post("/two")
def second_handler():
    pass
"""
    positions, names = contract._index_function_defs(text)

    assert names == ["first_handler", "second_handler"]
    assert len(positions) == 2
    assert positions[0] < positions[1]


def test_resolve_handler_name_uses_next_function_after_route() -> None:
    text = """
@router.get("/one")
@router.get("/one-alt")
async def first_handler():
    pass

@router.post("/two")
async def second_handler():
    pass
"""
    positions, names = contract._index_function_defs(text)
    route_matches = list(contract.ROUTE_PATTERN.finditer(text))

    first = contract._resolve_handler_name(route_matches[0].end(), positions, names)
    second = contract._resolve_handler_name(route_matches[1].end(), positions, names)
    third = contract._resolve_handler_name(route_matches[2].end(), positions, names)

    assert first == "first_handler"
    assert second == "first_handler"
    assert third == "second_handler"
