#!/bin/bash
cd /app || exit 1
ln -s ./docs-exampls docs
go install -v ./cmd/mailroom && chmod +x ./mailroom || exit 1
./mailroom
