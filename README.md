# async-comm

- [Features](#features)
- [Prerequisite](#prerequisite)
- [Installation](#installation)
- [Configuration](#configuration)
- [Examples](#examples)

## Features

It provides an async library for interacting with redis stream. It exposes following functions

```go
func NewAC(rdb *redis.Redis) (*AsyncComm, error)

func (ac *AsyncComm) Ack(q string, msgId ...string) error

func (ac *AsyncComm) Close()

func (ac *AsyncComm) CreateQ(q string, persistent bool) error

func (ac *AsyncComm) DeleteQ(q string) error

func (ac *AsyncComm) FlushQ(q string) error

func (ac *AsyncComm) GetQStats(opts redis.QStatusOptions) (redis.QStatus, error)

func (ac *AsyncComm) GroupExists(q string) bool

func (ac *AsyncComm) PendingMessages(q string) (map[string][]string, int, error)

func (ac *AsyncComm) Pull(q, consumer string, block time.Duration) ([]byte, string, error)

func (ac *AsyncComm) Push(q string, msg []byte) (string, error)

func (ac *AsyncComm) RegisterConsumer(ctx context.Context, cnsmr string, rTime, claimTime int, wg *sync.WaitGroup)

func (ac *AsyncComm) SetLogLevel(level string) error

```

## Prerequisite

You need Golang v1.16.x installed on your system, and a redis server >= v6.2.2 running 
to work with redis lib. This lib provides a testing application to start with.

To install golang you can follow the instructions here
```bash
https://golang.org/doc/install
```

To run redis on your local as a container run this command
```bash
$ docker run -d -p 6379:6379 redis:6.2.2
```

## Installation

To install async you need to clone this application to your local by running this command
```bash
$ git clone https://github.com/kiran-anand14/async-comm
```

After cloning the project you need to install the dependencies, for that run the below command
```bash
$ go mod download
```
By running this command it will download all the dependent libraries and stores it in your Gopath folder.
After that you can test application present in internal/app/asynccommtest
```bash
$ cd internal/app/asynccommtest
$ go run main.go
```
For running the application as docker container you can build the docker image using the following command
from your project root directory. Once the docker image is build you can run it using docker run command.
```bash
$ docker build -t <REPO_TAG> -f build/package/asynccommtest/Dockerfile .
$ docker run -d <REPO_TAG>
```
If you want you can run the complete stack using docker-compose file which is present in build/package/asynccommtest.
compose provides a stack of 1 redis server and 2 application server which are connected with config file config1.yml 
and config2.yml. 
```bash
$ cd build/package/asynccommtest
$ docker-compose up -d
```

##Configuration
This library uses Viper to manage configuration which can take the input from Json, yaml or toml files and can
dynamically parse environment variables into the config file. The basic structure of config is given below

```yaml
# redis connection information
redis:
  host:
  port: 6379
  username: ""
  password: ""

# act_app_logger configuration
app_logger:
  level: info
  full_timestamp: true
  output_file_path: ""

# ac library logger configuration
ac_logger:
  level: panic
  output_file_path: ""

# application information
application:
  app: App-A
  claim_time: 2000
  routines:
    - role: producer
      q: req-a-q #group req-q-consumer-group
      name: producerA
      message:
        format: "{{.App}}-{{.Producer}}-{{.Time}}"
        freq: 1000
    - role: consumer
      q: req-a-q
      name: consumerX
      processing_time: 1000
      refresh_time: 5000
```
If you want to provide a field in environment variable Keys should be conjunction with '.' separator. for example you can 
provide the redis host and port by exporting environment variables like this

```bash
$ export REDIS.HOST=localhost
$ export REDIS.PORT=6379
```

## Examples

Pushing a message:
```go
package main

import (
	"async-comm/pkg/asynccomm"
	"async-comm/pkg/redis"
	"context"
	"fmt"
)

func main()  {
	rdOpts := redis.Options{
		Addr: ":6370",
	}
	rDb := redis.NewRdb(context.TODO(), rdOpts)
	aclib, err := asynccomm.NewAC(rDb)
	if err != nil {
		panic(err)
    }
    
	res, err := aclib.Push("hq", []byte("hello world"))
	if err != nil {
		panic(err)
    }
    fmt.Println(res)
}

```

Fetching the Status of stream:

```go
package main

import (
	"async-comm/pkg/asynccomm"
	"async-comm/pkg/redis"
	"context"
	"fmt"
)

func main()  {
	rdOpts := redis.Options{
		Addr: ":6370",
	}
	rDb := redis.NewRdb(context.TODO(), rdOpts)
	aclib, err := asynccomm.NewAC(rDb)
	if err != nil {
		panic(err)
    }
    
    opts := redis.QStatusOptions{
    	Q :       "hq",  // redis stream name
    	Consumer: true,  // returns consumer related information 
    	Groups:   true,  // returns group related information
    }
    
	res, err := aclib.GetQStats(opts)
	if err != nil {
		panic(err)
    }
    fmt.Println(res)
}
```
Response QStatus:

```go
redis.QStatus{
	Info:redis.Info{
		Length:5,                // total messages in the q
		Acknowledged:5,          // acknowledge messages
		RadixTreeKeys:1, 
		RadixTreeNodes:2, 
		LastGeneratedID:"1620421718593-0", 
		Groups:1, 
		FirstEntry:redis.Message{
			ID:"1620421718588-0", 
			Values:map[string]interface {}{
				"message":"test_message_0_2021-05-08 02:38:38.562616 +0530 IST m=+0.002343927"
			}}, 
		LastEntry:redis.Message{
			ID:"1620421718593-0", Values:map[string]interface {}{
				"message":"test_message_4_2021-05-08 02:38:38.577363 +0530 IST m=+0.017090815"
			}}}, 
	Consumers:[]redis.Consumer{
		redis.Consumer{
			Name:"test_consumer1", 
			Pending:0, 
			Idle:6
		}, 
		redis.Consumer{
			Name:"test_consumer2", 
			Pending:0, 
			Idle:7
		}}, 
	Groups:[]redis.Group{
		redis.Group{
			Name:"test_queue-consumer-group", 
			Consumers:2, 
			Pending:0, 
			LastDeliveredID:"1620421718593-0"
		}
	}
}
```