.PHONY: test smoke acceptance bench examples all

test:
	go test -race -timeout 60s ./...

smoke:
	go test -race -run TestSmoke -timeout 30s -v

acceptance:
	go test -race -run TestAcceptance -timeout 120s -v

bench:
	go test -bench=. -benchmem -timeout 120s -run=NONE

examples:
	@for d in examples/*/; do echo "=== $$d ===" && (cd $$d && go run .); done

all: test smoke acceptance bench examples
