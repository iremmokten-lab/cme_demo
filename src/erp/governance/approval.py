from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class ApprovalState:
    status: str  # draft/submitted/approved/rejected
    notes: str = ""

def submit_for_approval(notes: str = "") -> ApprovalState:
    return ApprovalState(status="submitted", notes=notes)

def approve(notes: str = "") -> ApprovalState:
    return ApprovalState(status="approved", notes=notes)

def reject(notes: str = "") -> ApprovalState:
    return ApprovalState(status="rejected", notes=notes)
