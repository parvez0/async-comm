FROM golang:1.16.3 AS BUILD-ENV

WORKDIR /app

# Download dependencies
COPY ./go.mod ./go.sum ./
RUN go mod download

# Copy source
COPY ./ ./

# Build binary
# -o denotes where to put executable
RUN env GOOS=linux GOARCH=arm64 go build -o /go/bin/asynccomm ./internal/app/asynccommtest

# Production image
FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata

# Create Non Privileged user
#RUN useradd --uid 1001 app

# Run as Non Privileged user
#USER app

COPY --from=BUILD-ENV /go/bin/asynccomm /go/bin/asynccomm

CMD ["/go/bin/asynccomm"]
