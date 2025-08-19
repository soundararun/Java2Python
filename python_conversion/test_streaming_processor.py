"""
Unit tests for the Pandas-based streaming processor using pytest
"""
import pytest
import pandas as pd
import json
import tempfile
import os
from datetime import datetime, timezone
from pathlib import Path

from models import Alert, Rule, AlertType
from rule_evaluator import PandasRuleEvaluator
from streaming_processor import StreamingProcessor


class TestPandasRuleEvaluator:
    """Test cases for PandasRuleEvaluator"""
    
    @pytest.fixture
    def sample_rules(self):
        """Sample rules for testing"""
        return [
            Rule(
                rule_id="R001",
                rule_name="High CPU",
                rule_description="CPU threshold exceeded",
                rule_message="High CPU usage",
                tool_name="Solarwinds",
                group_names=["MC"],
                device_names=["centos1"],
                rule_expr="cpu_time_idle < 10",
                resource_type="cpu",
                priority="HIGH",
                consecutive_limit=2,
                status="ACTIVE"
            ),
            Rule(
                rule_id="R002",
                rule_name="Memory Alert",
                rule_description="Memory usage high",
                rule_message="Memory threshold exceeded",
                tool_name="Solarwinds",
                group_names=["MC"],
                device_names=["centos1"],
                rule_expr="memory_usage > 90",
                resource_type="memory",
                priority="CRITICAL",
                consecutive_limit=1,
                status="ACTIVE"
            )
        ]
    
    @pytest.fixture
    def sample_events_df(self):
        """Sample events DataFrame for testing"""
        events_data = [
            {
                'machine_name': 'centos1',
                'timestamp': '2023-01-01T10:00:00Z',
                'tool_name': 'Solarwinds',
                'group_name': 'MC',
                'resource_type': 'cpu',
                'cpu_time_idle': 5.0,  # Should trigger rule
                'alert_message': 'CPU alert'
            },
            {
                'machine_name': 'centos1',
                'timestamp': '2023-01-01T10:01:00Z',
                'tool_name': 'Solarwinds',
                'group_name': 'MC',
                'resource_type': 'cpu',
                'cpu_time_idle': 3.0,  # Should trigger rule again
                'alert_message': 'CPU alert'
            },
            {
                'machine_name': 'centos1',
                'timestamp': '2023-01-01T10:02:00Z',
                'tool_name': 'Solarwinds',
                'group_name': 'MC',
                'resource_type': 'memory',
                'memory_usage': 95.0,  # Should trigger memory rule
                'alert_message': 'Memory alert'
            },
            {
                'machine_name': 'centos1',
                'timestamp': '2023-01-01T10:03:00Z',
                'tool_name': 'Solarwinds',
                'group_name': 'MC',
                'resource_type': 'cpu',
                'cpu_time_idle': 50.0,  # Should trigger reverse alert
                'alert_message': 'CPU normal'
            }
        ]
        df = pd.DataFrame(events_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    @pytest.fixture
    def evaluator(self, sample_rules):
        """Initialize evaluator with sample rules"""
        evaluator = PandasRuleEvaluator()
        evaluator.load_rules(sample_rules)
        return evaluator
    
    def test_load_rules(self, evaluator, sample_rules):
        """Test rule loading"""
        assert len(evaluator.rules_df) == 2
        assert evaluator.rules_df['rule_id'].tolist() == ['R001', 'R002']
        assert evaluator.rules_df['status'].tolist() == ['ACTIVE', 'ACTIVE']
    
    def test_filter_relevant_events(self, evaluator, sample_events_df):
        """Test event filtering"""
        rule = evaluator.rules_df.iloc[0]  # CPU rule
        relevant_events = evaluator._filter_relevant_events(sample_events_df, rule)
        
        # Should filter to CPU events for centos1 from MC group
        assert len(relevant_events) == 3  # 3 CPU events
        assert all(relevant_events['resource_type'] == 'cpu')
        assert all(relevant_events['machine_name'] == 'centos1')
    
    def test_extract_field_names(self, evaluator):
        """Test field name extraction from rule expressions"""
        fields = evaluator._extract_field_names("cpu_time_idle < 10")
        assert 'cpu_time_idle' in fields
        
        fields = evaluator._extract_field_names("memory_usage > 90 and cpu_usage < 50")
        assert 'memory_usage' in fields
        assert 'cpu_usage' in fields
    
    def test_state_management(self, evaluator):
        """Test state management functions"""
        state_key = "test_key"
        
        # Test initialization
        evaluator._initialize_state_if_absent(state_key)
        assert evaluator._state_exists(state_key)
        
        # Test consecutive count increment
        count = evaluator._increment_consecutive_count(state_key)
        assert count == 1
        
        count = evaluator._increment_consecutive_count(state_key)
        assert count == 2
        
        # Test positive alert status
        assert not evaluator._was_positive_alert_raised(state_key)
        evaluator._set_positive_alert_status(state_key)
        assert evaluator._was_positive_alert_raised(state_key)
        
        # Test clear state
        evaluator._clear_positive_alert_from_state(state_key)
        assert not evaluator._state_exists(state_key)
    
    def test_process_events_batch(self, evaluator, sample_events_df):
        """Test batch event processing"""
        alerts_df = evaluator.process_events_batch(sample_events_df)
        
        # Should generate alerts
        assert not alerts_df.empty
        assert len(alerts_df) >= 2  # At least CPU positive and memory positive
        
        # Check alert types
        alert_types = alerts_df['alert_type'].unique()
        assert 'POSITIVE' in alert_types
        
        # Check required fields
        required_fields = ['alert_key', 'machine_id', 'resource_type', 'alert_type']
        for field in required_fields:
            assert field in alerts_df.columns


class TestStreamingProcessor:
    """Test cases for StreamingProcessor"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def sample_rules_file(self, temp_dir):
        """Create sample rules JSON file"""
        rules_data = [
            {
                "id": "R001",
                "name": "CPU Test",
                "description": "CPU test rule",
                "tool_name": "Solarwinds",
                "group_name": ["MC"],
                "device_name": ["centos1"],
                "expression": "cpu_time_idle < 10",
                "resource_type": "cpu",
                "consecutive_limit": 2,
                "status": "ACTIVE"
            }
        ]
        
        rules_file = os.path.join(temp_dir, "test_rules.json")
        with open(rules_file, 'w') as f:
            json.dump(rules_data, f)
        
        return rules_file
    
    @pytest.fixture
    def sample_events_file(self, temp_dir):
        """Create sample events JSON file"""
        events_data = [
            {
                "machine_name": "centos1",
                "timestamp": "2023-01-01T10:00:00Z",
                "tool_name": "Solarwinds",
                "group_name": "MC",
                "resource_type": "cpu",
                "cpu_time_idle": 5.0,
                "alert_message": "CPU alert"
            },
            {
                "machine_name": "centos1",
                "timestamp": "2023-01-01T10:01:00Z",
                "tool_name": "Solarwinds",
                "group_name": "MC",
                "resource_type": "cpu",
                "cpu_time_idle": 3.0,
                "alert_message": "CPU alert"
            }
        ]
        
        events_file = os.path.join(temp_dir, "test_events.json")
        with open(events_file, 'w') as f:
            json.dump(events_data, f)
        
        return events_file
    
    def test_load_rules_from_json(self, sample_rules_file):
        """Test loading rules from JSON file"""
        processor = StreamingProcessor()
        processor.load_rules_from_json(sample_rules_file)
        
        assert len(processor.rules) == 1
        assert processor.rules[0].rule_id == "R001"
        assert processor.rules[0].rule_name == "CPU Test"
    
    def test_load_events_from_json(self, sample_events_file):
        """Test loading events from JSON file"""
        processor = StreamingProcessor()
        events_df = processor.load_events_from_json(sample_events_file)
        
        assert len(events_df) == 2
        assert 'machine_name' in events_df.columns
        assert 'cpu_time_idle' in events_df.columns
        assert events_df['timestamp'].dtype == 'datetime64[ns]'
    
    def test_process_events_batch(self, sample_rules_file, sample_events_file):
        """Test batch processing"""
        processor = StreamingProcessor()
        processor.load_rules_from_json(sample_rules_file)
        events_df = processor.load_events_from_json(sample_events_file)
        
        alerts_df = processor.process_events_batch(events_df)
        
        # Should generate at least one alert
        assert not alerts_df.empty
        assert 'alert_type' in alerts_df.columns
    
    def test_save_alerts_to_csv(self, temp_dir):
        """Test saving alerts to CSV"""
        processor = StreamingProcessor()
        
        # Create sample alerts DataFrame
        alerts_data = [{
            'alert_key': 'test_key',
            'machine_id': 'centos1',
            'alert_type': 'POSITIVE',
            'resource_type': 'cpu'
        }]
        alerts_df = pd.DataFrame(alerts_data)
        
        output_file = os.path.join(temp_dir, "test_alerts.csv")
        processor.save_alerts_to_csv(alerts_df, output_file)
        
        # Verify file was created and has correct content
        assert os.path.exists(output_file)
        saved_df = pd.read_csv(output_file)
        assert len(saved_df) == 1
        assert saved_df['alert_key'].iloc[0] == 'test_key'
    
    def test_get_processing_statistics(self):
        """Test statistics generation"""
        processor = StreamingProcessor()
        
        # Create sample DataFrames
        events_data = [
            {'machine_name': 'centos1', 'timestamp': '2023-01-01T10:00:00Z'},
            {'machine_name': 'centos2', 'timestamp': '2023-01-01T10:01:00Z'}
        ]
        events_df = pd.DataFrame(events_data)
        events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])
        
        alerts_data = [
            {'machine_id': 'centos1', 'alert_type': 'POSITIVE', 'resource_type': 'cpu'},
            {'machine_id': 'centos1', 'alert_type': 'REVERSE', 'resource_type': 'cpu'}
        ]
        alerts_df = pd.DataFrame(alerts_data)
        
        stats = processor.get_processing_statistics(events_df, alerts_df)
        
        assert stats['total_events_processed'] == 2
        assert stats['total_alerts_generated'] == 2
        assert 'POSITIVE' in stats['alert_types']
        assert 'REVERSE' in stats['alert_types']
        assert 'centos1' in stats['alerts_by_machine']
    
    def test_run_pipeline(self, sample_rules_file, sample_events_file, temp_dir):
        """Test complete pipeline execution"""
        processor = StreamingProcessor({'batch_size': 100})
        
        output_dir = os.path.join(temp_dir, "output")
        stats = processor.run_pipeline(sample_events_file, sample_rules_file, output_dir)
        
        # Check that output files were created
        assert os.path.exists(os.path.join(output_dir, "alerts.csv"))
        assert os.path.exists(os.path.join(output_dir, "alerts.json"))
        assert os.path.exists(os.path.join(output_dir, "processing_stats.json"))
        
        # Check statistics
        assert 'total_events_processed' in stats
        assert 'total_alerts_generated' in stats
        assert stats['total_events_processed'] == 2


class TestPerformanceOptimizations:
    """Test performance optimizations and edge cases"""
    
    def test_large_dataset_processing(self):
        """Test processing with large dataset"""
        # Create large dataset
        num_events = 10000
        events_data = []
        
        for i in range(num_events):
            events_data.append({
                'machine_name': f'machine_{i % 10}',
                'timestamp': f'2023-01-01T{10 + i // 3600:02d}:{(i % 3600) // 60:02d}:{i % 60:02d}Z',
                'tool_name': 'Solarwinds',
                'group_name': 'MC',
                'resource_type': 'cpu',
                'cpu_time_idle': 5.0 if i % 5 == 0 else 50.0  # Every 5th event triggers rule
            })
        
        events_df = pd.DataFrame(events_data)
        events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])
        
        # Create rule
        rules = [Rule(
            rule_id="R001",
            rule_name="CPU Test",
            rule_description="CPU test",
            rule_message="CPU alert",
            tool_name="Solarwinds",
            group_names=["MC"],
            device_names=[f'machine_{i}' for i in range(10)],
            rule_expr="cpu_time_idle < 10",
            resource_type="cpu",
            priority="HIGH",
            consecutive_limit=2,
            status="ACTIVE"
        )]
        
        # Process with batch size
        processor = StreamingProcessor({'batch_size': 1000})
        evaluator = PandasRuleEvaluator()
        evaluator.load_rules(rules)
        
        alerts_df = evaluator.process_events_batch(events_df)
        
        # Should handle large dataset efficiently
        assert isinstance(alerts_df, pd.DataFrame)
        print(f"Processed {len(events_df)} events, generated {len(alerts_df)} alerts")
    
    def test_missing_values_handling(self):
        """Test handling of missing values in data"""
        events_data = [
            {
                'machine_name': 'centos1',
                'timestamp': '2023-01-01T10:00:00Z',
                'tool_name': 'Solarwinds',
                'group_name': 'MC',
                'resource_type': 'cpu',
                'cpu_time_idle': None,  # Missing value
            },
            {
                'machine_name': 'centos1',
                'timestamp': '2023-01-01T10:01:00Z',
                'tool_name': 'Solarwinds',
                'group_name': None,  # Missing group
                'resource_type': 'cpu',
                'cpu_time_idle': 5.0,
            }
        ]
        
        events_df = pd.DataFrame(events_data)
        events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])
        
        rules = [Rule(
            rule_id="R001",
            rule_name="CPU Test",
            rule_description="CPU test",
            rule_message="CPU alert",
            tool_name="Solarwinds",
            group_names=["MC"],
            device_names=["centos1"],
            rule_expr="cpu_time_idle < 10",
            resource_type="cpu",
            priority="HIGH",
            consecutive_limit=1,
            status="ACTIVE"
        )]
        
        evaluator = PandasRuleEvaluator()
        evaluator.load_rules(rules)
        
        # Should handle missing values gracefully
        alerts_df = evaluator.process_events_batch(events_df)
        assert isinstance(alerts_df, pd.DataFrame)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])