"""
Data models for the streaming event processor - Python equivalent of Java classes
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum
import pandas as pd


class AlertType(Enum):
    WARNING = "WARNING"
    POSITIVE = "POSITIVE"
    REVERSE = "REVERSE"


@dataclass
class Alert:
    """Python equivalent of Alert.java"""
    actual_value: str
    resource_type: str
    alert_type: str
    group: str
    alert_key: str
    event_received_timestamp: datetime
    alert_generated_timestamp: datetime
    machine_id: str
    description: str
    others: str = ""
    tool_name: str = ""
    counter_name: str = ""
    gtm_case_id: Optional[str] = None
    gtm_status: Optional[str] = None
    rule_id: Optional[str] = None
    consecutive_limit: Optional[int] = None
    consecutive_count: Optional[int] = None


@dataclass
class Rule:
    """Python equivalent of Rule.java"""
    rule_id: str
    rule_name: str
    rule_description: str
    rule_message: str
    tool_name: str
    group_names: List[str]
    device_names: List[str]
    rule_expr: str
    resource_type: str
    priority: str
    consecutive_limit: int
    status: str


@dataclass
class EventData:
    """Represents incoming JSON event data"""
    machine_name: str
    timestamp: str
    tool_name: str
    group_name: str
    resource_type: str
    alert_message: Optional[str] = None
    others: Optional[str] = None
    # Dynamic fields for metrics (cpu_time_idle, memory_usage, etc.)
    metrics: Dict[str, Any] = field(default_factory=dict)