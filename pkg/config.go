package pkg

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
	"sync"
)

// Config defines the structure of the config object
// it defines information about server details and
// postgres db details including the credentials
type Config struct {
	Server ServerConf `json:"server" mapstructure:"server"`
	Redis RedisConf `json:"redis" mapstructure:"redis"`
	Logger struct{
		Level string `json:"level" mapstructure:"level"`
		FullTimestamp bool `json:"full_timestamp" mapstructure:"full_timestamp"`
		OutputFilePath string `json:"output_file_path" mapstructure:"output_file_path"`
	}
}

type ServerConf struct {
	App      string `json:"app" mapstructure:"app"`
	Routines []Routine `json:"routines" mapstructure:"routines"`
}

type RedisConf struct {
	Host string `json:"host" mapstructure:"host"`
	Port string `json:"port" mapstructure:"port"`
	Username string `json:"username" mapstructure:"username"`
	Password string `json:"password" mapstructure:"password"`
}

type Routine struct {
	Role    string `json:"role" mapstructure:"role"`
	Q       string `json:"q" mapstructure:"q"`
	Name    string `json:"name" mapstructure:"name"`
	Group 	string `json:"group" mapstructure:"group"`
	Message struct {
		FormattedMsg string `json:"formatted_msg" mapstructure:"formatted_msg"`
		Format string `json:"format" mapstructure:"format"`
		Freq   string `json:"freq" mapstructure:"freq"`
	} `json:"message,omitempty" mapstructure:"message"`
	ProcessingTime int `json:"processing_time,omitempty" mapstructure:"processing_time"`
	RefreshTime    int `json:"refresh_time,omitempty" mapstructure:"refresh_time"`
}

var config *Config

// InitializeConfig makes use of viper library to initialize
// config from multiple sources such as json, yaml, toml and
// even environment variables, it returns a pointer to Config
func InitializeConfig() *Config {
	mutex := sync.Mutex{}
	if config != nil {
		return config
	}

	mutex.Lock()
	defer mutex.Unlock()

	// Set the file name of the configurations file
	viper.SetConfigName("config")

	// Set the path to look for the configurations file
	viper.AddConfigPath(".")
	viper.AddConfigPath("/opt/server")

	// Enable VIPER to read Environment Variables
	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("error reading config file - %s", err))
	}

	hostname, _ := os.Hostname()

	// Set undefined variables
	viper.SetDefault("server.app", hostname)
	viper.SetDefault("pkg.port", "5000")
	viper.SetDefault("db.host", "localhost")
	viper.SetDefault("db.port", "3306")
	viper.SetDefault("db.username", "")
	viper.SetDefault("logger.level", "info")
	viper.SetDefault("logger.full_timestamp", true)

	err := viper.Unmarshal(&config)
	if err != nil {
		panic(fmt.Sprintf("unable to decode config file : %v", err))
	}
	return config
}
