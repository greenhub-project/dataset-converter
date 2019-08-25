#!/usr/bin/env sh

python3 -m pip install --user --compile -r ./plugins/requirements.txt
cp ./plugins/*.py .
python3 -m app
