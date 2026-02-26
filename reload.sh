#!/bin/bash
pkill -f uvicorn 2>/dev/null
sleep 1
uvicorn main:app --reload
