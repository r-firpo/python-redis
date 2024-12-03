#!/bin/sh

set -e # Exit early if any commands fail

exec python3 -m app.main "$@"
