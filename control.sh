#!/bin/bash

set -e

# shellcheck disable=SC2006
# shellcheck disable=SC2086

TEST_COVERAGE_THRESHOLD=70

function unittest() {
  go test -buildmode=pie -parallel=8 ./... -coverprofile coverage.out -covermode count
  go tool cover -func coverage.out
  echo "Quality Gate: checking test coverage is above threshold ..."
  echo "Threshold             : $TEST_COVERAGE_THRESHOLD%"
  totalCoverage=$(go tool cover -func=coverage.out | grep total | grep -Eo '[0-9]+\.[0-9]+')
  echo "Current test coverage : $totalCoverage %"
  if (($(echo "$totalCoverage $TEST_COVERAGE_THRESHOLD" | awk '{print ($1 > $2)}'))); then
    echo "OK"
  else
    echo "Current test coverage is below threshold. Please add more unit tests or adjust threshold to a lower value."
    echo "FAIL"
    exit 1
  fi
}

if [ "$1" == "test" ]; then
  unittest
fi
