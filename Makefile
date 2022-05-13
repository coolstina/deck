GO := go

tidy:
	$(GO) mod tidy

vendor:
	$(GO) mod vendor

dependencies: tidy vendor

test: dependencies
	$(GO) test -race -cover ./...

.PHONY: \
	tidy \
	vendor \
	dependencies \
	test
