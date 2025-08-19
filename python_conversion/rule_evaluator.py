"""
Python equivalent of DefaultRuleEvaluator.java using Pandas for data processing
"""
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import logging
import re
from models import Alert, Rule, EventData, AlertType

logger = logging.getLogger(__name__)


class PandasRuleEvaluator:
    """
    Pandas-optimized version of DefaultRuleEvaluator
    Uses vectorized operations instead of loops for better performance
    """
    
    def __init__(self):
        self.consecutive_counts = pd.DataFrame(columns=['state_key', 'count', 'last_updated'])
        self.positive_alerts = pd.DataFrame(columns=['state_key', 'status', 'last_updated'])
        self.rules_df = pd.DataFrame()
        
    def load_rules(self, rules: List[Rule]) -> None:
        """Load rules into a DataFrame for efficient processing"""
        rules_data = []
        for rule in rules:
            rules_data.append({
                'rule_id': rule.rule_id,
                'rule_name': rule.rule_name,
                'rule_description': rule.rule_description,
                'tool_name': rule.tool_name,
                'group_names': rule.group_names,
                'device_names': rule.device_names,
                'rule_expr': rule.rule_expr,
                'resource_type': rule.resource_type,
                'consecutive_limit': rule.consecutive_limit,
                'status': rule.status
            })
        self.rules_df = pd.DataFrame(rules_data)
        
    def process_events_batch(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process a batch of events using vectorized Pandas operations
        Returns DataFrame of generated alerts
        """
        if self.rules_df.empty or events_df.empty:
            return pd.DataFrame()
            
        # Filter active rules
        active_rules = self.rules_df[self.rules_df['status'] == 'ACTIVE'].copy()
        if active_rules.empty:
            return pd.DataFrame()
            
        alerts_list = []
        
        # Use cross join to evaluate each event against each rule
        for _, rule in active_rules.iterrows():
            relevant_events = self._filter_relevant_events(events_df, rule)
            if not relevant_events.empty:
                rule_alerts = self._evaluate_rule_for_events(relevant_events, rule)
                alerts_list.extend(rule_alerts)
                
        return pd.DataFrame(alerts_list) if alerts_list else pd.DataFrame()
    
    def _filter_relevant_events(self, events_df: pd.DataFrame, rule: pd.Series) -> pd.DataFrame:
        """Filter events relevant to the given rule using vectorized operations"""
        mask = (
            (events_df['tool_name'] == rule['tool_name']) &
            (events_df['resource_type'] == rule['resource_type'])
        )
        
        # Filter by group names if specified
        if rule['group_names']:
            mask &= events_df['group_name'].isin(rule['group_names'])
            
        # Filter by device names if specified  
        if rule['device_names']:
            mask &= events_df['machine_name'].isin(rule['device_names'])
            
        return events_df[mask].copy()
    
    def _evaluate_rule_for_events(self, events_df: pd.DataFrame, rule: pd.Series) -> List[Dict]:
        """Evaluate rule expression against events using vectorized operations"""
        alerts = []
        
        # Extract field names from rule expression
        field_names = self._extract_field_names(rule['rule_expr'])
        
        # Ensure all required fields exist
        missing_fields = set(field_names) - set(events_df.columns)
        if missing_fields:
            logger.warning(f"Missing fields {missing_fields} for rule {rule['rule_id']}")
            return alerts
            
        # Evaluate rule expression vectorized
        try:
            # Create a safe evaluation environment
            eval_df = events_df[field_names + ['machine_name', 'timestamp', 'group_name']].copy()
            
            # Replace field names in expression with DataFrame column references
            expr = rule['rule_expr']
            for field in field_names:
                expr = expr.replace(field, f"eval_df['{field}']")
                
            # Evaluate expression
            rule_matches = eval(expr)
            
            # Process matches
            matched_events = events_df[rule_matches].copy()
            
            for _, event in matched_events.iterrows():
                alert = self._process_rule_match(event, rule)
                if alert:
                    alerts.append(alert)
                    
            # Process non-matches for reverse alerts
            non_matched_events = events_df[~rule_matches].copy()
            for _, event in non_matched_events.iterrows():
                alert = self._process_rule_non_match(event, rule)
                if alert:
                    alerts.append(alert)
                    
        except Exception as e:
            logger.error(f"Error evaluating rule {rule['rule_id']}: {e}")
            
        return alerts
    
    def _process_rule_match(self, event: pd.Series, rule: pd.Series) -> Optional[Dict]:
        """Process a rule match - equivalent to Java's rule match logic"""
        state_key = self._get_state_key(rule['rule_id'], event['machine_name'], rule['resource_type'])
        
        # Initialize state if absent
        self._initialize_state_if_absent(state_key)
        
        # Increment consecutive count
        current_count = self._increment_consecutive_count(state_key)
        
        # Check if we should generate a POSITIVE alert
        if (current_count >= rule['consecutive_limit'] and 
            not self._was_positive_alert_raised(state_key)):
            
            alert_key = self._get_alert_key(state_key, pd.to_datetime(event['timestamp']))
            alert_data = self._build_alert_dict(event, rule, alert_key, AlertType.POSITIVE.value)
            self._set_positive_alert_status(state_key)
            return alert_data
            
        return None
    
    def _process_rule_non_match(self, event: pd.Series, rule: pd.Series) -> Optional[Dict]:
        """Process a rule non-match - check for reverse alerts"""
        state_key = self._get_state_key(rule['rule_id'], event['machine_name'], rule['resource_type'])
        
        if self._was_positive_alert_raised_already(state_key, rule['consecutive_limit']):
            alert_key = self._get_alert_key(state_key, pd.to_datetime(event['timestamp']))
            alert_data = self._build_alert_dict(event, rule, alert_key, AlertType.REVERSE.value)
            self._clear_positive_alert_from_state(state_key)
            return alert_data
            
        return None
    
    def _extract_field_names(self, rule_expr: str) -> List[str]:
        """Extract field names from rule expression"""
        # Simple regex to find identifiers (field names)
        pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
        tokens = re.findall(pattern, rule_expr)
        
        # Filter out operators and keywords
        operators = {'and', 'or', 'not', 'in', 'is', 'if', 'else', 'elif', 'for', 'while', 'def', 'class'}
        field_names = [token for token in tokens if token not in operators and not token.isdigit()]
        
        return list(set(field_names))
    
    def _get_state_key(self, rule_id: str, machine_name: str, resource_type: str) -> str:
        """Generate state key for tracking consecutive counts"""
        return f"ruleId:{rule_id}|machineId:{machine_name}|eventType:{resource_type}"
    
    def _get_alert_key(self, state_key: str, timestamp: datetime) -> str:
        """Generate unique alert key"""
        return f"{state_key}|type:cep_alert|{int(timestamp.timestamp() * 1000)}"
    
    def _initialize_state_if_absent(self, state_key: str) -> None:
        """Initialize state for new state key"""
        if not self._state_exists(state_key):
            # Add to consecutive counts
            new_count = pd.DataFrame([{
                'state_key': state_key,
                'count': 0,
                'last_updated': datetime.now()
            }])
            self.consecutive_counts = pd.concat([self.consecutive_counts, new_count], ignore_index=True)
            
            # Add to positive alerts
            new_alert = pd.DataFrame([{
                'state_key': state_key,
                'status': False,
                'last_updated': datetime.now()
            }])
            self.positive_alerts = pd.concat([self.positive_alerts, new_alert], ignore_index=True)
    
    def _state_exists(self, state_key: str) -> bool:
        """Check if state exists for given key"""
        return (
            (self.consecutive_counts['state_key'] == state_key).any() and
            (self.positive_alerts['state_key'] == state_key).any()
        )
    
    def _increment_consecutive_count(self, state_key: str) -> int:
        """Increment consecutive count for state key"""
        mask = self.consecutive_counts['state_key'] == state_key
        if mask.any():
            current_count = self.consecutive_counts.loc[mask, 'count'].iloc[0]
            new_count = current_count + 1
            self.consecutive_counts.loc[mask, 'count'] = new_count
            self.consecutive_counts.loc[mask, 'last_updated'] = datetime.now()
            return new_count
        return 0
    
    def _was_positive_alert_raised(self, state_key: str) -> bool:
        """Check if positive alert was already raised"""
        mask = self.positive_alerts['state_key'] == state_key
        if mask.any():
            return self.positive_alerts.loc[mask, 'status'].iloc[0]
        return False
    
    def _was_positive_alert_raised_already(self, state_key: str, consecutive_limit: int) -> bool:
        """Check if positive alert was raised and count exceeded limit"""
        count_mask = self.consecutive_counts['state_key'] == state_key
        alert_mask = self.positive_alerts['state_key'] == state_key
        
        if count_mask.any() and alert_mask.any():
            count = self.consecutive_counts.loc[count_mask, 'count'].iloc[0]
            status = self.positive_alerts.loc[alert_mask, 'status'].iloc[0]
            return count >= consecutive_limit and status
        return False
    
    def _set_positive_alert_status(self, state_key: str) -> None:
        """Set positive alert status to True"""
        mask = self.positive_alerts['state_key'] == state_key
        if mask.any():
            self.positive_alerts.loc[mask, 'status'] = True
            self.positive_alerts.loc[mask, 'last_updated'] = datetime.now()
    
    def _clear_positive_alert_from_state(self, state_key: str) -> None:
        """Clear positive alert state"""
        # Remove from consecutive counts
        self.consecutive_counts = self.consecutive_counts[
            self.consecutive_counts['state_key'] != state_key
        ].reset_index(drop=True)
        
        # Remove from positive alerts
        self.positive_alerts = self.positive_alerts[
            self.positive_alerts['state_key'] != state_key
        ].reset_index(drop=True)
    
    def _build_alert_dict(self, event: pd.Series, rule: pd.Series, alert_key: str, alert_type: str) -> Dict:
        """Build alert dictionary from event and rule data"""
        return {
            'alert_key': alert_key,
            'machine_id': event['machine_name'],
            'resource_type': event['resource_type'],
            'group': event['group_name'],
            'actual_value': str(event.get('cpu_time_idle', 'N/A')),  # Example field
            'event_received_timestamp': pd.to_datetime(event['timestamp']),
            'alert_generated_timestamp': datetime.now(timezone.utc),
            'alert_type': alert_type,
            'tool_name': rule['tool_name'],
            'description': f"{rule['rule_description']}|{event.get('alert_message', '')}",
            'others': event.get('others', ''),
            'counter_name': '',
            'gtm_case_id': None,
            'gtm_status': None,
            'rule_id': rule['rule_id'],
            'consecutive_limit': rule['consecutive_limit'],
            'consecutive_count': 0 if alert_type == AlertType.REVERSE.value else None
        }