#!/bin/sh
curl "http://localhost:8080/check?type=user&content=fuck&content=ok"
curl "http://localhost:8080/match?type=user&content=nice"
curl "http://localhost:8080/reload"
curl "http://localhost:8080/stats"
