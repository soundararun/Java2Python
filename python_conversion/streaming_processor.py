"""
Python equivalent of StreamingJob.java - Main streaming processor
Uses Pandas for batch processing instead of real-time streaming
"""
import pandas as pd
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path

from models import Alert, Rule, EventData
from rule_evaluator import PandasRuleEvaluator

logger = logging.getLogger(__name__)


class StreamingProcessor:
    """
    Python equivalent of StreamingJob.java
    Processes events in batches using Pandas instead of real-time streaming
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.rule_evaluator = PandasRuleEvaluator()
        self.rules: List[Rule] = []
        
    def load_rules_from_json(self, rules_file: str) -> None:
        """Load rules from JSON file"""
        try:
            with open(rules_file, 'r') as f:
                rules_data = json.load(f)
                
            self.rules = []
            for rule_dict in rules_data:
                rule = Rule(
                    rule_id=rule_dict['id'],
                    rule_name=rule_dict['name'],
                    rule_description=rule_dict['description'],
                    rule_message=rule_dict.get('message', ''),
                    tool_name=rule_dict['tool_name'],
                    group_names=rule_dict.get('group_name', []),
                    device_names=rule_dict.get('device_name', []),
                    rule_expr=rule_dict['expression'],
                    resource_type=rule_dict['resource_type'],
                    priority=rule_dict.get('priority', 'MEDIUM'),
                    consecutive_limit=rule_dict['consecutive_limit'],
                    status=rule_dict.get('status', 'ACTIVE')
                )
                self.rules.append(rule)
                
            self.rule_evaluator.load_rules(self.rules)
            logger.info(f"Loaded {len(self.rules)} rules")
            
        except Exception as e:
            logger.error(f"Error loading rules: {e}")
            raise
    
    def load_events_from_csv(self, events_file: str) -> pd.DataFrame:
        """Load events from CSV file"""
        try:
            events_df = pd.read_csv(events_file)
            
            # Ensure required columns exist
            required_cols = ['machine_name', 'timestamp', 'tool_name', 'group_name', 'resource_type']
            missing_cols = set(required_cols) - set(events_df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
                
            # Convert timestamp to datetime
            events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])
            
            logger.info(f"Loaded {len(events_df)} events")
            return events_df
            
        except Exception as e:
            logger.error(f"Error loading events: {e}")
            raise
    
    def load_events_from_json(self, events_file: str) -> pd.DataFrame:
        """Load events from JSON file"""
        try:
            with open(events_file, 'r') as f:
                events_data = json.load(f)
                
            # Flatten JSON events into DataFrame
            events_list = []
            for event in events_data:
                # Extract base fields
                event_dict = {
                    'machine_name': event.get('machine_name'),
                    'timestamp': event.get('timestamp'),
                    'tool_name': event.get('tool_name'),
                    'group_name': event.get('group_name'),
                    'resource_type': event.get('resource_type'),
                    'alert_message': event.get('alert_message'),
                    'others': event.get('others')
                }
                
                # Add metric fields dynamically
                for key, value in event.items():
                    if key not in event_dict:
                        event_dict[key] = value
                        
                events_list.append(event_dict)
                
            events_df = pd.DataFrame(events_list)
            events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])
            
            logger.info(f"Loaded {len(events_df)} events from JSON")
            return events_df
            
        except Exception as e:
            logger.error(f"Error loading events from JSON: {e}")
            raise
    
    def process_events_batch(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process a batch of events and generate alerts
        Uses Pandas vectorized operations for efficiency
        """
        if events_df.empty:
            return pd.DataFrame()
            
        # Sort events by timestamp for proper sequential processing
        events_df = events_df.sort_values('timestamp').reset_index(drop=True)
        
        # Process events in batches for memory efficiency
        batch_size = self.config.get('batch_size', 1000)
        all_alerts = []
        
        for i in range(0, len(events_df), batch_size):
            batch = events_df.iloc[i:i+batch_size]
            batch_alerts = self.rule_evaluator.process_events_batch(batch)
            
            if not batch_alerts.empty:
                all_alerts.append(batch_alerts)
                
        if all_alerts:
            alerts_df = pd.concat(all_alerts, ignore_index=True)
            logger.info(f"Generated {len(alerts_df)} alerts")
            return alerts_df
        else:
            return pd.DataFrame()
    
    def save_alerts_to_csv(self, alerts_df: pd.DataFrame, output_file: str) -> None:
        """Save alerts to CSV file"""
        try:
            if not alerts_df.empty:
                alerts_df.to_csv(output_file, index=False)
                logger.info(f"Saved {len(alerts_df)} alerts to {output_file}")
            else:
                logger.info("No alerts to save")
        except Exception as e:
            logger.error(f"Error saving alerts: {e}")
            raise
    
    def save_alerts_to_json(self, alerts_df: pd.DataFrame, output_file: str) -> None:
        """Save alerts to JSON file"""
        try:
            if not alerts_df.empty:
                # Convert datetime columns to ISO format strings
                alerts_json = alerts_df.copy()
                datetime_cols = alerts_json.select_dtypes(include=['datetime64']).columns
                for col in datetime_cols:
                    alerts_json[col] = alerts_json[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                
                alerts_json.to_json(output_file, orient='records', indent=2)
                logger.info(f"Saved {len(alerts_df)} alerts to {output_file}")
            else:
                logger.info("No alerts to save")
        except Exception as e:
            logger.error(f"Error saving alerts to JSON: {e}")
            raise
    
    def get_processing_statistics(self, events_df: pd.DataFrame, alerts_df: pd.DataFrame) -> Dict[str, Any]:
        """Get processing statistics"""
        stats = {
            'total_events_processed': len(events_df),
            'total_alerts_generated': len(alerts_df),
            'alert_types': alerts_df['alert_type'].value_counts().to_dict() if not alerts_df.empty else {},
            'alerts_by_machine': alerts_df['machine_id'].value_counts().to_dict() if not alerts_df.empty else {},
            'alerts_by_resource_type': alerts_df['resource_type'].value_counts().to_dict() if not alerts_df.empty else {},
            'processing_time_range': {
                'start': events_df['timestamp'].min().isoformat() if not events_df.empty else None,
                'end': events_df['timestamp'].max().isoformat() if not events_df.empty else None
            }
        }
        return stats
    
    def run_pipeline(self, events_file: str, rules_file: str, output_dir: str) -> Dict[str, Any]:
        """
        Run the complete processing pipeline
        """
        try:
            # Create output directory
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Load rules
            self.load_rules_from_json(rules_file)
            
            # Load events
            if events_file.endswith('.csv'):
                events_df = self.load_events_from_csv(events_file)
            else:
                events_df = self.load_events_from_json(events_file)
            
            # Process events
            alerts_df = self.process_events_batch(events_df)
            
            # Save results
            alerts_csv = f"{output_dir}/alerts.csv"
            alerts_json = f"{output_dir}/alerts.json"
            
            self.save_alerts_to_csv(alerts_df, alerts_csv)
            self.save_alerts_to_json(alerts_df, alerts_json)
            
            # Generate statistics
            stats = self.get_processing_statistics(events_df, alerts_df)
            
            # Save statistics
            stats_file = f"{output_dir}/processing_stats.json"
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2)
            
            logger.info("Pipeline completed successfully")
            return stats
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main entry point"""
    logging.basicConfig(level=logging.INFO)
    
    # Configuration
    config = {
        'batch_size': 1000,
        'enable_checkpoints': False
    }
    
    # Initialize processor
    processor = StreamingProcessor(config)
    
    # Run pipeline
    try:
        stats = processor.run_pipeline(
            events_file="sample_events.json",
            rules_file="sample_rules.json", 
            output_dir="output"
        )
        print("Processing completed successfully!")
        print(f"Statistics: {json.dumps(stats, indent=2)}")
        
    except Exception as e:
        print(f"Processing failed: {e}")


if __name__ == "__main__":
    main()