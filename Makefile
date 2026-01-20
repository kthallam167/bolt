.PHONY: build test race vet fmt run bench clean

build:
	mkdir -p bin
	go build -o bin/bolt-server ./cmd/bolt-server
	go build -o bin/bolt-cli ./cmd/bolt-cli
	go build -o bin/bolt-bench ./cmd/bolt-bench

test:
	go test ./...

race:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

run: build
	./bin/bolt-server

bench: build
	./bin/bolt-bench -c 50 -n 500000 -mode mixed -pipeline 1

clean:
	rm -rf bin *.aof *.aof.rewrite.tmp
