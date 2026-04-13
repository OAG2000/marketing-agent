#!/bin/bash
python -m src.ingest data/all_metrics.csv
python app.py