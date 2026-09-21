.PHONY: build test vet lint run-fakes run demo docker

build:
	go build -o bin/crmbridge ./cmd/crmbridge
	go build -o bin/fakes ./cmd/fakes

test:
	go test ./... -race -cover

vet:
	go vet ./...

lint: vet
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

# two terminals: run-fakes, run
run-fakes: build
	./bin/fakes
run: build
	./bin/crmbridge -c examples/config.yaml

# scripted walk-through against the local stand-ins (needs curl and jq)
demo: build
	./scripts/demo.sh

docker:
	docker build -t crmbridge .
