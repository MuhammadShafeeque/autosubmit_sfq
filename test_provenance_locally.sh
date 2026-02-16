#!/bin/bash
#
# Local Provenance Testing Script
# Tests provenance tracking WITHOUT needing remote server or git push
#
# Usage: ./test_provenance_locally.sh [expid]
#

set -e  # Exit on error

EXPID="${1:-test_prov}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================"
echo "LOCAL PROVENANCE TEST"
echo "========================================"
echo "Testing experiment: $EXPID"
echo "Working directory: $SCRIPT_DIR"
echo ""

# Step 1: Check if autosubmit is installed
echo "[1/6] Checking autosubmit installation..."
if python3 -c "import autosubmit" 2>/dev/null; then
    echo "    ✓ Autosubmit is installed"
   echo "    ⓘ Using system installation (skipping reinstall to avoid PEP 668 issues)"
else
    echo "    ⚠️  Autosubmit not installed! Please install it first:"
    echo "       python3 -m pip install -e . --user"
    exit 1
fi
echo ""

# Step 2: Clean old experiment
echo "[2/6] Cleaning old experiment if exists..."
if autosubmit delete -f $EXPID >/dev/null 2>&1; then
    echo "    ✓ Deleted old experiment"
else
    echo "    ⓘ No existing experiment to delete"
fi
echo ""

# Step 3: Create experiment with VERBOSE logging
echo "[3/6] Creating experiment with INFO log level..."
echo "    Command: autosubmit create $EXPID -lc INFO"
echo ""

# Capture output
OUTPUT=$(autosubmit create $EXPID -lc INFO 2>&1)
echo "$OUTPUT"

echo ""
echo "[4/6] Analyzing output..."
echo ""

# Check for our diagnostic logs
if echo "$OUTPUT" | grep -q "\[PROV-DEBUG\]"; then
    echo "    ✅ Found [PROV-DEBUG] logs"
    echo "$OUTPUT" | grep "\[PROV-DEBUG\]" | head -5
else
    echo "    ❌ No [PROV-DEBUG] logs found"
fi
echo ""

if echo "$OUTPUT" | grep -q "\[PROV-TRACK\]"; then
    echo "    ✅ Found [PROV-TRACK] logs"
    echo "$OUTPUT" | grep "\[PROV-TRACK\]" | head -5
else
    echo "    ❌ No [PROV-TRACK] logs found"
fi
echo ""

if echo "$OUTPUT" | grep -q "\[PROV-LOAD\]"; then
    echo "    ✅ Found [PROV-LOAD] logs"
    echo "$OUTPUT" | grep "\[PROV-LOAD\]" | head -5
else
    echo "    ❌ No [PROV-LOAD] logs found"
fi
echo ""

# Step 4: Check experiment_data.yml
echo "[5/6] Checking experiment_data.yml..."

# Find the file
if [ -n "$AUTOSUBMIT_ROOT_DIR" ]; then
    EXPDIR="$AUTOSUBMIT_ROOT_DIR/$EXPID"
else
    EXPDIR="$HOME/autosubmit/$EXPID"
fi

METADATA_FILE="$EXPDIR/conf/metadata/experiment_data.yml"

if [ -f "$METADATA_FILE" ]; then
    echo "    ✓ File exists: $METADATA_FILE"
    echo ""
    
    if grep -q "^PROVENANCE:" "$METADATA_FILE"; then
        echo "    ✅ SUCCESS: PROVENANCE section found!"
        echo ""
        echo "    First 30 lines of PROVENANCE:"
        echo "    -----------------------------"
        sed -n '/^PROVENANCE:/,/^[A-Z]/p' "$METADATA_FILE" | head -30 | sed 's/^/    /'
        echo ""
        
        # Count tracked parameters
        NUM_TRACKED=$(grep -c 'file:' "$METADATA_FILE" || echo "0")
        echo "    📊 Stats: $NUM_TRACKED parameters tracked"
        
    else
        echo "    ❌ FAIL: No PROVENANCE section found"
        echo ""
        echo "    First 50 lines of file:"
        head -50 "$METADATA_FILE" | sed 's/^/    /'
    fi
else
    echo "    ❌ File not found: $METADATA_FILE"
    echo ""
    echo "    Checking if experiment directory exists..."
    if [ -d "$EXPDIR" ]; then
        echo "    ✓ Experiment directory exists"
        echo "    Contents:"
        find "$EXPDIR" -type f | head -20 | sed 's/^/      /'
    else
        echo "    ❌ Experiment directory not found: $EXPDIR"
    fi
fi

echo ""
echo "[6/6] Checking log files..."
if [ -d "$EXPDIR/tmp/ASLOGS" ]; then
    echo "    ✓ Log directory exists"
    LOG_FILES=$(find "$EXPDIR/tmp/ASLOGS" -name "*.log" 2>/dev/null || true)
    if [ -n "$LOG_FILES" ]; then
        echo "    Log files found:"
        echo "$LOG_FILES" | sed 's/^/      /'
        echo ""
        echo "    Checking for provenance logs in files..."
        grep -h "\[PROV" $LOG_FILES 2>/dev/null | head -10 | sed 's/^/      /' || echo "      (None found)"
    else
        echo "    ⚠️  No log files found in $EXPDIR/tmp/ASLOGS"
    fi
else
    echo "    ⚠️  Log directory doesn't exist: $EXPDIR/tmp/ASLOGS"
fi

echo ""
echo "========================================"
echo "TEST COMPLETE"
echo "========================================"
echo ""
echo "Next steps:"
echo "  - If PROVENANCE section exists: ✅ Feature working!"
echo "  - If no [PROV-DEBUG] logs: Check __init__() is being called"
echo "  - If no [PROV-TRACK] logs: Check _track_yaml_provenance() is being called"
echo "  - If no PROVENANCE section: Check save() logic"
echo ""
echo "To view full metadata file:"
echo "  cat $METADATA_FILE"
echo ""
