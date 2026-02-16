#!/bin/bash
# Deep debug test for tracker lifecycle

echo "========================================="
echo "Tracker Lifecycle Deep Debug Test"
echo "========================================="
echo ""

cd ~/autosubmit-dev || exit 1

echo "Pulling latest debug instrumentation..."
git pull
echo ""

echo "Reinstalling..."
pip uninstall -y autosubmit && pip install -e . > /dev/null 2>&1
echo "✅ Reinstalled"
echo ""

echo "Running 'as create t0ht' with DEBUG logging..."
echo "Looking for [PROV-DEBUG] markers..."
echo "-----------------------------------------"

export AUTOSUBMIT_LOG_LEVEL=DEBUG
as create t0ht 2>&1 | grep "\[PROV-DEBUG"

echo ""
echo "========================================="
echo "Analysis:"
echo "========================================="
echo ""
echo "Expected sequence:"
echo "1. [PROV-DEBUG] __init__() tracker created - tracker_is_none=False"
echo "2. [PROV-DEBUG] reload() called ... (multiple times)"
echo "3. [PROV-DEBUG] End of reload() - tracker_is_none=False, num_tracked=X"
echo "4. [PROV-DEBUG-CREATE] Before as_conf.save(): tracker_is_none=False"
echo "5. [PROV-DEBUG] save() called - tracker_is_none=False"
echo ""
echo "If any show tracker_is_none=True, that's where it breaks!"
