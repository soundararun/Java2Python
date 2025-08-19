# Performance Optimization Guide

## Handling Missing Values

### 1. Data Validation
```python
# Check for missing critical fields
required_fields = ['machine_name', 'timestamp', 'tool_name', 'resource_type']
missing_mask = events_df[required_fields].isnull().any(axis=1)
valid_events = events_df[~missing_mask]

# Handle missing metric values
events_df['cpu_time_idle'] = events_df['cpu_time_idle'].fillna(0)
events_df['memory_usage'] = events_df['memory_usage'].fillna(events_df['memory_usage'].mean())
```

### 2. Robust Rule Evaluation
```python
# Use pandas.eval() for safe expression evaluation
def safe_evaluate_rule(df, expression):
    try:
        # Replace NaN values before evaluation
        df_clean = df.fillna(0)
        return df_clean.eval(expression)
    except Exception as e:
        logger.warning(f"Rule evaluation failed: {e}")
        return pd.Series([False] * len(df))
```

## Large Dataset Handling

### 1. Chunked Processing
```python
def process_large_dataset(events_df, chunk_size=10000):
    """Process large datasets in chunks to manage memory"""
    alerts_list = []
    
    for chunk_start in range(0, len(events_df), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(events_df))
        chunk = events_df.iloc[chunk_start:chunk_end]
        
        chunk_alerts = rule_evaluator.process_events_batch(chunk)
        if not chunk_alerts.empty:
            alerts_list.append(chunk_alerts)
            
        # Optional: Clear memory
        del chunk
        
    return pd.concat(alerts_list, ignore_index=True) if alerts_list else pd.DataFrame()
```

### 2. Memory Optimization
```python
# Use categorical data types for repeated strings
events_df['machine_name'] = events_df['machine_name'].astype('category')
events_df['tool_name'] = events_df['tool_name'].astype('category')
events_df['resource_type'] = events_df['resource_type'].astype('category')

# Use appropriate numeric types
events_df['cpu_time_idle'] = pd.to_numeric(events_df['cpu_time_idle'], downcast='float')
```

### 3. Parallel Processing
```python
import multiprocessing as mp
from functools import partial

def process_chunk_parallel(chunk, rules):
    evaluator = PandasRuleEvaluator()
    evaluator.load_rules(rules)
    return evaluator.process_events_batch(chunk)

def parallel_processing(events_df, rules, n_processes=None):
    if n_processes is None:
        n_processes = mp.cpu_count()
    
    # Split data into chunks
    chunks = np.array_split(events_df, n_processes)
    
    # Process in parallel
    with mp.Pool(n_processes) as pool:
        process_func = partial(process_chunk_parallel, rules=rules)
        results = pool.map(process_func, chunks)
    
    # Combine results
    return pd.concat([r for r in results if not r.empty], ignore_index=True)
```

## Performance Issues and Solutions

### 1. Slow Rule Evaluation
**Problem**: Complex rule expressions with many fields
**Solution**: 
- Pre-filter events by basic criteria (tool_name, resource_type)
- Use vectorized operations instead of apply()
- Cache compiled expressions

```python
import re
from functools import lru_cache

@lru_cache(maxsize=100)
def compile_rule_expression(expression):
    """Cache compiled rule expressions"""
    # Pre-process expression for pandas eval
    return expression.replace('&&', '&').replace('||', '|')

# Use vectorized filtering
def fast_rule_filter(events_df, rule):
    # Basic filters first (fastest)
    mask = (events_df['tool_name'] == rule['tool_name']) & \
           (events_df['resource_type'] == rule['resource_type'])
    
    if not mask.any():
        return pd.DataFrame()
    
    # Apply complex rule only to pre-filtered data
    filtered_events = events_df[mask]
    compiled_expr = compile_rule_expression(rule['rule_expr'])
    
    try:
        rule_matches = filtered_events.eval(compiled_expr)
        return filtered_events[rule_matches]
    except:
        return pd.DataFrame()
```

### 2. Memory Issues with Large State
**Problem**: State DataFrames grow too large
**Solution**: 
- Implement state cleanup for old entries
- Use more efficient data structures

```python
def cleanup_old_state(self, max_age_hours=24):
    """Remove old state entries to prevent memory bloat"""
    cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
    
    # Clean consecutive counts
    recent_mask = self.consecutive_counts['last_updated'] > cutoff_time
    self.consecutive_counts = self.consecutive_counts[recent_mask].reset_index(drop=True)
    
    # Clean positive alerts
    recent_mask = self.positive_alerts['last_updated'] > cutoff_time
    self.positive_alerts = self.positive_alerts[recent_mask].reset_index(drop=True)
```

### 3. I/O Performance
**Problem**: Slow file reading/writing
**Solution**:
- Use efficient file formats (Parquet instead of CSV)
- Implement streaming I/O for large files

```python
# Use Parquet for better performance
def save_alerts_parquet(alerts_df, filename):
    alerts_df.to_parquet(filename, compression='snappy')

def load_events_parquet(filename):
    return pd.read_parquet(filename)

# Streaming CSV reader for large files
def stream_process_csv(filename, chunk_size=10000):
    alerts_list = []
    
    for chunk in pd.read_csv(filename, chunksize=chunk_size):
        chunk_alerts = process_events_batch(chunk)
        if not chunk_alerts.empty:
            alerts_list.append(chunk_alerts)
    
    return pd.concat(alerts_list, ignore_index=True) if alerts_list else pd.DataFrame()
```

## Monitoring and Profiling

### 1. Performance Monitoring
```python
import time
import psutil
import logging

def monitor_performance(func):
    """Decorator to monitor function performance"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        logging.info(f"{func.__name__}: {end_time - start_time:.2f}s, "
                    f"Memory: {end_memory - start_memory:+.1f}MB")
        
        return result
    return wrapper

@monitor_performance
def process_events_batch(self, events_df):
    # Your processing logic here
    pass
```

### 2. Memory Profiling
```python
from memory_profiler import profile

@profile
def memory_intensive_function():
    # Function to profile
    pass

# Run with: python -m memory_profiler your_script.py
```

## Configuration Recommendations

### Production Settings
```python
PRODUCTION_CONFIG = {
    'batch_size': 5000,          # Balance memory vs processing speed
    'chunk_size': 10000,         # For large file processing
    'max_state_age_hours': 48,   # State cleanup interval
    'enable_parallel': True,     # Use multiprocessing
    'n_processes': 4,            # Number of parallel processes
    'memory_limit_mb': 2048,     # Memory usage limit
    'use_categorical': True,     # Optimize string columns
    'compression': 'snappy',     # For Parquet files
}
```

### Development Settings
```python
DEVELOPMENT_CONFIG = {
    'batch_size': 1000,
    'chunk_size': 5000,
    'max_state_age_hours': 24,
    'enable_parallel': False,    # Easier debugging
    'enable_profiling': True,    # Performance monitoring
    'log_level': 'DEBUG',
}
```