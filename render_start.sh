#!/bin/bash
python -m src.ingest data/metrics.csv data/metrics_new.csv
python app.py
