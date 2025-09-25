# ui/markup.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Iterable, List, Any

def row(*buttons: Any) -> List[Any]:
    return list(buttons)

def inline_kb(rows: Iterable[Iterable[Any]]) -> List[List[Any]]:
    return [list(r) for r in rows]