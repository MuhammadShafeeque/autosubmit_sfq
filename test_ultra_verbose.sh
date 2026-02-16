#!/bin/bash
# Ultra-verbose debug test - show EVERYTHING

echo "========================================="
echo "Ultra-Verbose Provenance Debug"
echo "========================================="
echo ""

cd ~/autosubmit-dev || exit 1

echo "Step 1: Check git status"
git log --oneline -1
echo ""

echo "Step 2: Reinstall (verbose)"
pip uninstall -y autosubmit 2>&1 | tail -2
pip install -e . 2>&1 | tail -5
echo ""

echo "Step 3: Verify code has debug logs"
echo "Checking for '[PROV-DEBUG] __init__' in code:"
grep -n "\[PROV-DEBUG\] __init__" autosubmit/config/configcommon.py || echo "❌ NOT FOUND"
echo ""
echo "Checking for '[PROV-DEBUG] save()' in code:"
grep -n "\[PROV-DEBUG\] save()" autosubmit/config/configcommon.py || echo "❌ NOT FOUND"
echo ""
echo "Checking for '[PROV-DEBUG-CREATE]' in code:"
grep -n "\[PROV-DEBUG-CREATE\]" autosubmit/autosubmit.py || echo "❌ NOT FOUND"
echo ""

echo "Step 4: Run 'as create t0ht' and capture ALL output"
echo "========================================="
echo "Full output (first 100 lines, looking for PROV):"
as create t0ht 2>&1 | head -150 | grep -i "prov"
echo ""
echo "========================================="
echo ""

echo "Step 5: Run AGAIN and capture just provenance-related lines"
echo "Looking for: PROV-DEBUG, tracker, provenance, Provenance"
as create t0ht 2>&1 | grep -E "PROV|tracker|Provenance" | head -30
echo ""
echo "========================================="
