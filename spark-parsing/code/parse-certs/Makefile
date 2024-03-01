GO_SOURCES = $(shell find . -name '*.go')

parse-certs: $(GO_SOURCES)
	go build -o $@

parse-certs-debian: $(GO_SOURCES)
	env GOOS=linux GOARCH=amd64 go build -o $@
