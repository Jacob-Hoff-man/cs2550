#!/usr/bin/env bash
echo "run phase 1 round-robin"
python3 ./final-project/DbmsSimulator.py final-project/schema.json ran final-project/files/sample1.txt final-project/files/sample2.txt

echo "run phase 1 random"
python3 ./final-project/DbmsSimulator.py final-project/schema.json rr final-project/files/sample1.txt final-project/files/sample2.txt