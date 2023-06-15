# REQUIRE COMPONENTS
# https://github.com/mvdan/gofumpt => $go install mvdan.cc/gofumpt@latest
# https://github.com/incu6us/goimports-reviser => $go install github.com/incu6us/goimports-reviser/v3@v3.1.1
# https://github.com/golangci/golangci-lint => $go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.50.1

GO ?= go
SHELL := bash

.PHONY: help
help:
	@echo "Make Targets: "
	@echo " lint: Lint Go code"
	@echo " test: Run unit tests"

.PHONY: lint
lint:
	diff -u <(echo -n) <(gofumpt -w .)
	diff -u <(echo -n) <(goimports-reviser -project-name "github.com/chenjiandongx/grogudb" ./...)

.PHONY: test
test:
	${GO} test -parallel=8 ./...
