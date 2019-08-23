#!/usr/bin/env sh

python3 -m pip install --no-cache-dir --user -r ./plugins/requirements.txt
cp ./plugins/*.py .
python3 -m app
