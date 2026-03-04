.PHONY: build test run clean

build:
	go build -o bin/broker ./cmd/broker/

test:
	go test ./... -v -count=1

run:
	go run ./cmd/broker/

clean:
	rm -rf bin/
