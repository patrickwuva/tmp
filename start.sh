#!/bin/bash
sudo apt update -y
sudo apt-get install -y python3.8-venv
python3 -m venv ve
source ve/bin/activate
pip install -r requirements.txt
