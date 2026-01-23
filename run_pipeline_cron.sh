#!/bin/bash

# Cron-friendly wrapper script for document retrieval pipeline
# Usage in crontab:
#   0 */6 * * * /path/to/run_pipeline_cron.sh >> /path/to/logs/cron.log 2>&1

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="${SCRIPT_DIR}/venv"
PYTHON_PATH="${VENV_PATH}/bin/python"
PIPELINE_SCRIPT="${SCRIPT_DIR}/src/pipeline.py"
LOG_DIR="${SCRIPT_DIR}/logs"
STATE_DIR="${SCRIPT_DIR}/data/processed"

# Ensure log directory exists
mkdir -p "${LOG_DIR}"

# Log file with timestamp
LOG_FILE="${LOG_DIR}/cron_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

# Start logging
log "=========================================="
log "Document Retrieval Pipeline - Cron Run"
log "=========================================="
log "Start time: $(date)"
log ""

# Check if virtual environment exists
if [ ! -d "${VENV_PATH}" ]; then
    log "ERROR: Virtual environment not found at ${VENV_PATH}"
    log "Please run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements_doc_retrieval.txt"
    exit 1
fi

# Activate virtual environment
log "Activating virtual environment..."
source "${VENV_PATH}/bin/activate"

# Check if pipeline script exists
if [ ! -f "${PIPELINE_SCRIPT}" ]; then
    log "ERROR: Pipeline script not found at ${PIPELINE_SCRIPT}"
    exit 1
fi

# Check if .env file exists
if [ ! -f "${SCRIPT_DIR}/.env" ]; then
    log "WARNING: .env file not found. Make sure API keys are set as environment variables."
fi

# Run pipeline
log "Running pipeline..."
log "Command: ${PYTHON_PATH} ${PIPELINE_SCRIPT} --step all --incremental"
log ""

# Run with error handling
if "${PYTHON_PATH}" "${PIPELINE_SCRIPT}" --step all --incremental >> "${LOG_FILE}" 2>&1; then
    EXIT_CODE=0
    log ""
    log "✓ Pipeline completed successfully"
else
    EXIT_CODE=$?
    log ""
    log "✗ Pipeline failed with exit code ${EXIT_CODE}"
    log "Check ${LOG_FILE} for details"
fi

log ""
log "End time: $(date)"
log "=========================================="

# Optional: Send email notification on failure
if [ ${EXIT_CODE} -ne 0 ] && [ -n "${NOTIFICATION_EMAIL}" ]; then
    echo "Pipeline failed. Check ${LOG_FILE}" | mail -s "Document Retrieval Pipeline Failed" "${NOTIFICATION_EMAIL}"
fi

exit ${EXIT_CODE}

