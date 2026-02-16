#!/bin/bash
# Final debug test with visible logs

echo "========================================="
echo "Provenance Tracker Debug - Final Test"
echo "========================================="
echo ""

cd ~/autosubmit-dev || exit 1

echo "Pulling latest changes..."
git pull
echo ""

echo "Reinstalling..."
pip uninstall -y autosubmit > /dev/null 2>&1
pip install -e . > /dev/null 2>&1
echo "✅ Reinstalled"
echo ""

echo "Running 'as create t0ht'..."
echo "Showing only [PROV-DEBUG] lines:"
echo "========================================="

as create t0ht 2>&1 | grep "\[PROV-DEBUG"

echo ""
echo "========================================="
echo "Analysis:"
echo "========================================="
echo ""
echo "You should see logs in this order:"
echo "1. __init__() tracker created"
echo "2. reload() called (1st time)"
echo "3. End of reload()"
echo "4. reload() called (2nd time)"  
echo "5. End of reload()"
echo "6. [PROV-DEBUG-CREATE] Before as_conf.save()"
echo "7. save() called"
echo ""
echo "If step 7 shows tracker_is_none=False BUT you still see"
echo "'Provenance tracker is None', then the tracker object exists"
echo "but evaluates to False (empty provenance_map)."
