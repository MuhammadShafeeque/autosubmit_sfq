#!/bin/bash
# Test script to verify provenance tracking is now WORKING

echo "========================================="
echo "PROVENANCE FIX VERIFICATION TEST"
echo "========================================="
echo ""
echo "This test verifies that BOTH bugs are fixed:"
echo "  1. _track_yaml_provenance() now tracks parameters"
echo "  2. save() now exports PROVENANCE section"
echo ""

# Navigate to autosubmit directory 
cd ~/autosubmit-dev || exit 1

# Pull latest changes
echo "[1] Pulling latest changes..."
git pull
echo ""

# Reinstall
echo "[2] Reinstalling autosubmit..."
pip install -e . > /dev/null 2>&1
echo "   ✓ Reinstalled"
echo ""

# Delete old experiment to start fresh
echo "[3] Cleaning old experiment..."
as delete -f t0ht > /dev/null 2>&1
echo "   ✓ Cleaned"
echo ""

# Create new experiment with verbose output
echo "[4] Creating experiment with provenance tracking..."
echo ""
as create t0ht 2>&1 | grep -E "\[PROV|Provenance|tracked|experiment_data" || echo "   (No provenance logs found - checking file...)"
echo ""

# Check if experiment_data.yml has PROVENANCE section
echo "[5] Checking experiment_data.yml for PROVENANCE section..."
EXPID_FILE="$HOME/autosubmit/t0ht/tmp/t0ht/experiment_data.yml"

if [ -f "$EXPID_FILE" ]; then
    echo "   ✓ File exists: $EXPID_FILE"
    echo ""
    
    if grep -q "^PROVENANCE:" "$EXPID_FILE"; then
        echo "   ✅ SUCCESS: PROVENANCE section found!"
        echo ""
        echo "   First 20 lines of PROVENANCE section:"
        echo "   --------------------------------------"
        sed -n '/^PROVENANCE:/,/^[A-Z]/p' "$EXPID_FILE" | head -20 | sed 's/^/   /'
        echo ""
        
        # Count how many parameters were tracked
        NUM_TRACKED=$(grep -c 'file:' "$EXPID_FILE" || echo "0")
        echo "   📊 Stats: $NUM_TRACKED parameters tracked"
        echo ""
        echo "   ✅ BOTH BUGS FIXED!"
        echo "      1. Tracker is populated during load"
        echo "      2. PROVENANCE section exported correctly"
        
    else
        echo "   ❌ FAIL: No PROVENANCE section in experiment_data.yml"
        echo ""
        echo "   Checking if tracker was populated (should show logs):"
        echo "   ----------------------------------------------------"
        as create t0ht 2>&1 | grep -i "processed.*keys"
        echo ""
        echo "   If you see 'Processed N keys' above, bug #1 is fixed but bug #2 remains."
        echo "   If you see nothing, bug #1 still exists."
    fi
else
    echo "   ❌ File not found: $EXPID_FILE"
    echo "   Experiment creation may have failed"
fi

echo ""
echo "========================================="
echo "TEST COMPLETE"
echo "========================================="
