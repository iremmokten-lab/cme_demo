from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class SamplingItem:
    record_ref: str
    reason: str

@dataclass
class Finding:
    code: str
    description: str
    severity: str  # low/medium/high

@dataclass
class VerificationWorkspaceState:
    sampling_plan: List[SamplingItem]
    findings: List[Finding]
    notes: List[str]

def new_workspace() -> VerificationWorkspaceState:
    return VerificationWorkspaceState(sampling_plan=[], findings=[], notes=[])
