all: test

test:
	@GOEXPERIMENT=synctest go test ./...
