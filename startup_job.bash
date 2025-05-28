#!/bin/bash

# startup_job.bash - Run subscriber and transform jobs in sequence with logging
# This script coordinates the execution of data collection and transformation

set -e  # Exit on any error
set -o pipefail  # Exit on pipe failures

# Configuration
SCRIPT_DIR="/opt/busdata"
LOG_DIR="/opt/busdata/logs"
RAW_DATA_DIR="/opt/busdata/busdata/raw_data"  # Both subscriber writes and transform reads from here
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
MAIN_LOG="$LOG_DIR/startup_job_${TIMESTAMP}.log"

# Create necessary directories
mkdir -p "$LOG_DIR"
mkdir -p "$RAW_DATA_DIR"

# Logging function
log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$MAIN_LOG"
}

# Error handling function
handle_error() {
    local exit_code=$?
    local line_number=$1
    log "ERROR" "Script failed at line $line_number with exit code $exit_code"
    log "ERROR" "Startup job failed - check logs for details"
    exit $exit_code
}

# Set up error trap
trap 'handle_error $LINENO' ERR

# Start main execution
log "INFO" "=== Starting Bus Data Processing Pipeline ==="
log "INFO" "Script directory: $SCRIPT_DIR"
log "INFO" "Log directory: $LOG_DIR"
log "INFO" "Raw data directory: $RAW_DATA_DIR"
log "INFO" "Main log file: $MAIN_LOG"

# Change to script directory
cd "$SCRIPT_DIR"
log "INFO" "Changed to working directory: $(pwd)"

# Check if required files exist
log "INFO" "Checking for required files..."
required_files=("data_subscriber.py" "transform.py" "ids.txt")
for file in "${required_files[@]}"; do
    if [[ -f "$file" ]]; then
        log "INFO" "✓ Found: $file"
    else
        log "ERROR" "✗ Missing required file: $file"
        exit 1
    fi
done

# Step 1: Run data subscriber
log "INFO" "=== STEP 1: Starting Data Subscriber ==="
log "INFO" "Running data_subscriber.py..."

SUBSCRIBER_LOG="$LOG_DIR/subscriber_${TIMESTAMP}.log"
log "INFO" "Subscriber output will be logged to: $SUBSCRIBER_LOG"

if python3 data_subscriber.py 2>&1 | tee "$SUBSCRIBER_LOG"; then
    log "INFO" "✓ Data subscriber completed successfully"
else
    subscriber_exit_code=$?
    log "ERROR" "✗ Data subscriber failed with exit code: $subscriber_exit_code"
    log "ERROR" "Check subscriber log: $SUBSCRIBER_LOG"
    exit $subscriber_exit_code
fi

# Check if subscriber produced output
if [[ -n "$(find "$RAW_DATA_DIR" -name '*.json' -o -name '*.csv' 2>/dev/null)" ]]; then
    log "INFO" "✓ Subscriber generated output files"
    log "INFO" "Output files: $(find "$RAW_DATA_DIR" -type f | wc -l) files found"
else
    log "WARNING" "No output files found in $RAW_DATA_DIR after subscriber run"
fi

# Step 2: Run transform
log "INFO" "=== STEP 2: Starting Data Transform ==="
log "INFO" "Running transform.py..."

TRANSFORM_LOG="$LOG_DIR/transform_${TIMESTAMP}.log"
log "INFO" "Transform output will be logged to: $TRANSFORM_LOG"

if python3 transform.py 2>&1 | tee "$TRANSFORM_LOG"; then
    log "INFO" "✓ Data transform completed successfully"
else
    transform_exit_code=$?
    log "ERROR" "✗ Data transform failed with exit code: $transform_exit_code"
    log "ERROR" "Check transform log: $TRANSFORM_LOG"
    exit $transform_exit_code
fi

# Final status check
log "INFO" "=== PIPELINE COMPLETION STATUS ==="
total_output_files=$(find "$RAW_DATA_DIR" -type f 2>/dev/null | wc -l)
log "INFO" "Total output files: $total_output_files"

if [[ $total_output_files -gt 0 ]]; then
    log "INFO" "Output file details:"
    find "$RAW_DATA_DIR" -type f -exec ls -lh {} \; 2>/dev/null | while read -r line; do
        log "INFO" "  $line"
    done
fi

# Log disk usage
log "INFO" "Disk usage for raw data directory:"
if command -v du >/dev/null 2>&1; then
    du -sh "$RAW_DATA_DIR" 2>/dev/null | while read -r usage; do
        log "INFO" "  $usage"
    done
fi

# Success
log "INFO" "=== SUCCESS: Bus Data Processing Pipeline Completed ==="
log "INFO" "Total execution time: $SECONDS seconds"
log "INFO" "All logs available in: $LOG_DIR"
log "INFO" "Pipeline completed successfully at $(date)"

# Optional: Clean up old log files (keep last 10 runs)
log "INFO" "Cleaning up old log files..."
find "$LOG_DIR" -name "startup_job_*.log" -type f | sort | head -n -10 | xargs -r rm -f
find "$LOG_DIR" -name "subscriber_*.log" -type f | sort | head -n -10 | xargs -r rm -f
find "$LOG_DIR" -name "transform_*.log" -type f | sort | head -n -10 | xargs -r rm -f
log "INFO" "Log cleanup completed"

log "INFO" "Startup job finished successfully"
exit 0