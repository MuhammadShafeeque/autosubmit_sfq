#!/bin/bash
# Diagnostic script to check what code is actually running

echo "========================================="
echo "Code Verification Diagnostic"
echo "========================================="
echo ""

cd ~/autosubmit-dev || exit 1

echo "Step 1: Check Git Status"
echo "-----------------------------------------"
git log --oneline -3
echo ""
echo "Current commit:"
git rev-parse HEAD
echo ""

echo "Step 2: Check __init__ tracker creation"
echo "-----------------------------------------"
echo "Looking for: self.provenance_tracker: ProvenanceTracker = ProvenanceTracker()"
grep -n "self.provenance_tracker.*ProvenanceTracker()" autosubmit/config/configcommon.py | head -3
echo ""

echo "Step 3: Check if reload() is simplified"
echo "-----------------------------------------"
echo "Looking for: 'Provenance tracking is always enabled'"
grep -n "Provenance tracking is always enabled" autosubmit/config/configcommon.py
echo ""

echo "Step 4: Check save() debug log"
echo "-----------------------------------------"
echo "Looking for: '[PROV-DEBUG] save() called'"
grep -n "\[PROV-DEBUG\] save()" autosubmit/config/configcommon.py
echo ""

echo "Step 5: Run quick test with full debug output"
echo "-----------------------------------------"
echo "Running: as create t0ht with FULL output (no grep)"
export AUTOSUBMIT_LOG_LEVEL=DEBUG
echo "First 50 lines of output:"
as create t0ht 2>&1 | head -100 | grep -E "PROV|Provenance|provenance|tracker"
echo ""

echo "========================================="
echo "Diagnostic Complete"
echo "========================================="
echo ""
echo "What to check:"
echo "1. Git commit should be 56b10cae or later"
echo "2. Should find 'self.provenance_tracker: ProvenanceTracker = ProvenanceTracker()' at line ~68"
echo "3. Should find 'Provenance tracking is always enabled' at line ~1917"
echo "4. Should find '[PROV-DEBUG] save()' logs"
echo "5. Should see [PROV-DEBUG] logs in as create output"
