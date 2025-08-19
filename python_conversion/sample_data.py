"""
Generate sample input datasets for testing the Python conversion
"""
import json
import pandas as pd
from datetime import datetime, timedelta
import random


def generate_sample_rules():
    """Generate sample rules JSON file"""
    rules = [
        {
            "id": "R001",
            "name": "High CPU Usage",
            "description": "CPU utilization exceeds threshold",
            "message": "High CPU utilization detected",
            "tool_name": "Solarwinds",
            "group_name": ["MC", "Production"],
            "device_name": ["centos1", "centos2", "server01"],
            "expression": "cpu_time_idle < 10",
            "resource_type": "cpu",
            "priority": "HIGH",
            "consecutive_limit": 3,
            "status": "ACTIVE"
        },
        {
            "id": "R002", 
            "name": "Memory Usage Alert",
            "description": "Memory usage is critically high",
            "message": "Memory threshold exceeded",
            "tool_name": "Solarwinds",
            "group_name": ["MC"],
            "device_name": ["centos1", "server01"],
            "expression": "memory_usage > 90",
            "resource_type": "memory",
            "priority": "CRITICAL",
            "consecutive_limit": 2,
            "status": "ACTIVE"
        },
        {
            "id": "R003",
            "name": "Disk Space Warning",
            "description": "Disk space running low",
            "message": "Low disk space detected",
            "tool_name": "Nagios",
            "group_name": ["Infrastructure"],
            "device_name": ["server01", "server02"],
            "expression": "disk_usage > 85",
            "resource_type": "disk",
            "priority": "MEDIUM",
            "consecutive_limit": 5,
            "status": "ACTIVE"
        }
    ]
    
    with open('sample_rules.json', 'w') as f:
        json.dump(rules, f, indent=2)
    
    return rules


def generate_sample_events(num_events=1000):
    """Generate sample events JSON file"""
    events = []
    machines = ["centos1", "centos2", "server01", "server02"]
    tools = ["Solarwinds", "Nagios"]
    groups = ["MC", "Production", "Infrastructure"]
    resource_types = ["cpu", "memory", "disk"]
    
    base_time = datetime.now() - timedelta(hours=2)
    
    for i in range(num_events):
        # Generate timestamp
        timestamp = base_time + timedelta(seconds=i * 30)  # Every 30 seconds
        
        machine = random.choice(machines)
        tool = random.choice(tools)
        group = random.choice(groups)
        resource_type = random.choice(resource_types)
        
        event = {
            "machine_name": machine,
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tool_name": tool,
            "group_name": group,
            "resource_type": resource_type,
            "alert_message": f"Monitoring alert for {resource_type}",
            "others": f"Additional info for {machine}"
        }
        
        # Add resource-specific metrics
        if resource_type == "cpu":
            # Generate CPU idle time - sometimes low to trigger alerts
            if random.random() < 0.3:  # 30% chance of low CPU idle (high usage)
                event["cpu_time_idle"] = random.uniform(2, 8)  # Low idle = high usage
            else:
                event["cpu_time_idle"] = random.uniform(15, 95)  # Normal idle
                
        elif resource_type == "memory":
            # Generate memory usage
            if random.random() < 0.2:  # 20% chance of high memory usage
                event["memory_usage"] = random.uniform(91, 98)
            else:
                event["memory_usage"] = random.uniform(30, 85)
                
        elif resource_type == "disk":
            # Generate disk usage
            if random.random() < 0.15:  # 15% chance of high disk usage
                event["disk_usage"] = random.uniform(86, 95)
            else:
                event["disk_usage"] = random.uniform(20, 80)
        
        events.append(event)
    
    with open('sample_events.json', 'w') as f:
        json.dump(events, f, indent=2)
    
    return events


def generate_sample_csv_events(num_events=1000):
    """Generate sample events as CSV file"""
    events = generate_sample_events(num_events)
    
    # Convert to DataFrame and save as CSV
    df = pd.json_normalize(events)
    df.to_csv('sample_events.csv', index=False)
    
    return df


def create_expected_output_examples():
    """Create examples of expected outputs for both Java and Python versions"""
    
    # Java conceptual output (what the original would produce)
    java_expected = {
        "sample_alert": {
            "alertId": "ruleId:R001|machineId:centos1|eventType:cpu|type:cep_alert|1640995200000",
            "deviceName": "centos1",
            "alertType": "cpu",
            "alertSeverity": "POSITIVE",
            "appName": "MC",
            "actual": "cpu_time_idle=5.2",
            "receivedTimeUTC": "2021-12-31T12:00:00Z",
            "generatedTimeUTC": "2021-12-31T12:00:05Z",
            "alertDescription": "CPU utilization exceeds threshold|High CPU utilization detected|{cpu_time_idle=5.2}",
            "toolName": "Solarwinds",
            "others": "Additional info for centos1"
        }
    }
    
    # Python/Pandas expected output
    python_expected = {
        "sample_alert": {
            "alert_key": "ruleId:R001|machineId:centos1|eventType:cpu|type:cep_alert|1640995200000",
            "machine_id": "centos1", 
            "resource_type": "cpu",
            "alert_type": "POSITIVE",
            "group": "MC",
            "actual_value": "5.2",
            "event_received_timestamp": "2021-12-31T12:00:00Z",
            "alert_generated_timestamp": "2021-12-31T12:00:05Z",
            "description": "CPU utilization exceeds threshold|High CPU utilization detected",
            "tool_name": "Solarwinds",
            "others": "Additional info for centos1",
            "rule_id": "R001",
            "consecutive_limit": 3
        }
    }
    
    # Save examples
    with open('expected_output_java.json', 'w') as f:
        json.dump(java_expected, f, indent=2)
        
    with open('expected_output_python.json', 'w') as f:
        json.dump(python_expected, f, indent=2)
    
    return java_expected, python_expected


if __name__ == "__main__":
    print("Generating sample data...")
    
    # Generate sample files
    rules = generate_sample_rules()
    events = generate_sample_events(500)  # Generate 500 events
    csv_events = generate_sample_csv_events(500)
    java_expected, python_expected = create_expected_output_examples()
    
    print(f"Generated:")
    print(f"- {len(rules)} sample rules -> sample_rules.json")
    print(f"- {len(events)} sample events -> sample_events.json")
    print(f"- {len(csv_events)} sample events -> sample_events.csv")
    print(f"- Expected output examples -> expected_output_*.json")
    
    print("\nSample rule:")
    print(json.dumps(rules[0], indent=2))
    
    print("\nSample event:")
    print(json.dumps(events[0], indent=2))