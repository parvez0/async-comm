TAG=$(shell cat .release | cut -d'=' -f2)
DEVELOPMENT_TAG=$(shell cat .development)
BUILD_PATH="$(shell go env GOPATH)/bin/wabacli"
CONFIG_PATH="$(HOME)/.async_comm"
.DEFAULT_GOAL := build

build: system-check
	@echo "starting build at $(BUILD_PATH) for tag $(DEVELOPMENT_TAG)"
	@cd cmd/ && GOOS_VAL=$(shell go env GOOS) GOARCH_VAL=$(shell go env GOARCH) go build -ldflags="-X main.BuildVersion=$(DEVELOPMENT_TAG)" -o $(BUILD_PATH) main.go
	@echo "build successful"

compose-up: docker-check docker-compose-check
	@docker-compose -f build/package/asynccommtest/docker-compose.yml  up

docker-build: docker-check
	@docker build -t parvez0/asynccomm -f build/package/asynccommtest/Dockerfile .

docker-check:
	@echo "verifying docker installation"
	@if [ -z "$(shell docker -v 2> /dev/null)" ]; then echo "docker is not installed or not running"; exit 1; fi

docker-compose-check:
	@echo "verifying docker-compose installation"
	@if [ -z "$(shell docker-compose -v 2> /dev/null)" ]; then echo "docker-compose is not installed"; exit 1; fi

install: system-check
	@if [ ! -f $(BUILD_PATH) ] ; then echo "binaries does not exits at $(BUILD_PATH)"; exit 1; fi;
	@if [[ "$(shell go env GOOS)" == "darwin" ]]; then echo "copying binaries to install path" && sudo cp "${BUILD_PATH}" /usr/local/bin/; fi;

run:
	@echo "starting application"
	@go run internal/app/asynccommtest/main.go

tag:
	@echo "creating tag $(TAG)"
	@git add .release README.md .goreleaser.yml cmd/ pkg/ config/ Makefile
	@git commit -m "Release $(TAG)"
	@git tag $(TAG)
	@git push origin $(TAG)

system-check:
	@echo "initializing system check"
	@if [ -z "$(shell go env GOOS)" ] || [ -z "$(shell go env GOARCH)" ] ;\
	 then \
   		echo "system info couldn't be determined" && exit 1 ; \
   	 else \
   	    echo "Go System: $(shell go env GOOS)" ; \
   	    echo "GO Arch: $(shell go env GOARCH)" ; \
   	    echo "system check passed" ;\
   	 fi ;

test:
	@echo "starting unit test"
	@if [ -d "$(CONFIG_PATH)" ]; then \
  		echo "copying latest config file" && cp config.yml "$(CONFIG_PATH)"; \
  	 else \
  	   echo "creating directory $(CONFIG_PATH)" && mkdir "$(CONFIG_PATH)" && cp config.yml "$(CONFIG_PATH)"; \
  	 fi
	@go test -timeout 15s -v ./...