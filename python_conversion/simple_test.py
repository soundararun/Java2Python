"""
Simple test to demonstrate the conversion without pandas dependency issues
"""
import json
from datetime import datetime, timedelta
import random


def generate_simple_test_data():
    """Generate simple test data without pandas"""
    
    # Sample rules
    rules = [
        {
            "id": "R001",
            "name": "High CPU Usage",
            "description": "CPU utilization exceeds threshold",
            "message": "High CPU utilization detected",
            "tool_name": "Solarwinds",
            "group_name": ["MC", "Production"],
            "device_name": ["centos1", "centos2"],
            "expression": "cpu_time_idle < 10",
            "resource_type": "cpu",
            "priority": "HIGH",
            "consecutive_limit": 3,
            "status": "ACTIVE"
        }
    ]
    
    # Sample events
    events = []
    base_time = datetime.now() - timedelta(hours=1)
    
    for i in range(10):
        timestamp = base_time + timedelta(minutes=i * 5)
        
        # Create events that will trigger the rule
        cpu_idle = 5.0 if i < 5 else 50.0  # First 5 events trigger rule
        
        event = {
            "machine_name": "centos1",
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tool_name": "Solarwinds",
            "group_name": "MC",
            "resource_type": "cpu",
            "cpu_time_idle": cpu_idle,
            "alert_message": f"CPU monitoring event {i+1}"
        }
        events.append(event)
    
    return rules, events


def demonstrate_java_vs_python_logic():
    """Demonstrate the logic conversion from Java to Python"""
    
    rules, events = generate_simple_test_data()
    
    print("=== Java to Python Conversion Demonstration ===\n")
    
    print("Original Java Logic (Conceptual):")
    print("""
    // Java - Loop-based processing
    for (Entry<String, Rule> entry : rules.entrySet()) {
        Rule rule = entry.getValue();
        for (JSONObject event : events) {
            if (isRuleRelevant(rule, event)) {
                boolean isMatch = evaluateRuleExpr(rule, event);
                if (isMatch) {
                    incrementConsecutiveCount(stateKey);
                    if (consecutiveCount >= rule.getConsecutiveLimit()) {
                        generatePositiveAlert(event, rule);
                    }
                }
            }
        }
    }
    """)
    
    print("\nPython Pandas Logic (Vectorized):")
    print("""
    # Python - Vectorized processing
    for _, rule in active_rules.iterrows():
        relevant_events = events_df[
            (events_df['tool_name'] == rule['tool_name']) &
            (events_df['resource_type'] == rule['resource_type'])
        ]
        
        # Vectorized rule evaluation
        rule_matches = relevant_events.eval(rule['rule_expr'])
        matched_events = relevant_events[rule_matches]
        
        # Process all matches at once
        alerts_df = process_matches_vectorized(matched_events, rule)
    """)
    
    print("\n=== Sample Data Generated ===")
    print(f"\nRules ({len(rules)}):")
    print(json.dumps(rules[0], indent=2))
    
    print(f"\nEvents ({len(events)}):")
    for i, event in enumerate(events[:3]):
        print(f"Event {i+1}: {event['machine_name']} - CPU Idle: {event['cpu_time_idle']}%")
    print("...")
    
    print(f"\nExpected Behavior:")
    print("- First 5 events have cpu_time_idle < 10 (trigger rule)")
    print("- After 3 consecutive matches, generate POSITIVE alert")
    print("- When cpu_time_idle > 10, generate REVERSE alert")
    
    # Simple rule evaluation simulation
    consecutive_count = 0
    alerts_generated = []
    
    print(f"\n=== Processing Simulation ===")
    for i, event in enumerate(events):
        cpu_idle = event['cpu_time_idle']
        rule_match = cpu_idle < 10  # Rule: cpu_time_idle < 10
        
        if rule_match:
            consecutive_count += 1
            print(f"Event {i+1}: CPU={cpu_idle}% - MATCH (count: {consecutive_count})")
            
            if consecutive_count >= 3:  # consecutive_limit
                alert = {
                    "alert_type": "POSITIVE",
                    "machine_id": event['machine_name'],
                    "timestamp": event['timestamp'],
                    "description": "CPU utilization exceeds threshold"
                }
                alerts_generated.append(alert)
                print(f"  -> POSITIVE ALERT generated!")
                
        else:
            if consecutive_count >= 3:  # Was in alert state
                alert = {
                    "alert_type": "REVERSE", 
                    "machine_id": event['machine_name'],
                    "timestamp": event['timestamp'],
                    "description": "CPU utilization back to normal"
                }
                alerts_generated.append(alert)
                print(f"Event {i+1}: CPU={cpu_idle}% - NO MATCH -> REVERSE ALERT generated!")
            else:
                print(f"Event {i+1}: CPU={cpu_idle}% - NO MATCH")
            consecutive_count = 0
    
    print(f"\n=== Results ===")
    print(f"Total alerts generated: {len(alerts_generated)}")
    for i, alert in enumerate(alerts_generated):
        print(f"Alert {i+1}: {alert['alert_type']} - {alert['machine_id']} at {alert['timestamp']}")
    
    # Save sample files
    with open('demo_rules.json', 'w') as f:
        json.dump(rules, f, indent=2)
    
    with open('demo_events.json', 'w') as f:
        json.dump(events, f, indent=2)
    
    with open('demo_expected_alerts.json', 'w') as f:
        json.dump(alerts_generated, f, indent=2)
    
    print(f"\nSample files created:")
    print("- demo_rules.json")
    print("- demo_events.json") 
    print("- demo_expected_alerts.json")


if __name__ == "__main__":
    demonstrate_java_vs_python_logic()