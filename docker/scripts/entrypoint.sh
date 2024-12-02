#!/bin/bash
cd /app || exit 1
go install -v ./cmd/mailroom && chmod +x /go/bin/mailroom || exit 1
/go/bin/mailroom