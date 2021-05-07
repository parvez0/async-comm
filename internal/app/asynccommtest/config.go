package main

import (
	"crypto/tls"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"path"
	"time"
)

// Config defines the structure of the config object
// it defines information about server details and
// postgres db details including the credentials
type Config struct {
	App      AppConf      `json:"application" mapstructure:"application"`
	Redis    RedisConf    `json:"redis" mapstructure:"redis"`
	AcLogger AcLoggerConf `json:"ac_logger" mapstructure:"ac_logger"`
	Logger   struct{
		Level string `json:"level" mapstructure:"level"`
		FullTimestamp bool `json:"full_timestamp" mapstructure:"full_timestamp"`
		OutputFilePath string `json:"output_file_path" mapstructure:"output_file_path"`
	} `json:"app_logger" mapstructure:"app_logger"`
}

type AppConf struct {
	App      string    `json:"app" mapstructure:"app"`
	ClaimTime int      `json:"claim_time" mapstructure:"claim_time"`
	Routines []Routine `json:"routines" mapstructure:"routines"`
}

type RedisConf struct {
	Host string `json:"host" mapstructure:"host"`
	Port string `json:"port" mapstructure:"port"`
	Username string `json:"username" mapstructure:"username"`
	Password string `json:"password" mapstructure:"password"`
	DB int `json:"db"`
	MaxRetries      int `json:"max_retries" mapstructure:"max_retries"`
	MinRetryBackoff time.Duration `json:"min_retry_backoff" mapstructure:"min_retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff" mapstructure:"max_retry_backoff"`
	PoolSize        int `json:"pool_size" mapstructure:"pool_size"`
	MinIdleConns    int `json:"min_idle_conns" mapstructure:"min_idle_conns"`
	IdleTimeout     time.Duration `json:"idle_timeout" mapstructure:"idle_timeout"`
	ReadTimeout     time.Duration `json:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `json:"write_timeout" mapstructure:"write_timeout"`
	TLSConfig 		*tls.Config `json:"tls_config" mapstructure:"tls_config"`
}

type AcLoggerConf struct {
	Level string `json:"level" mapstructure:"level"`
	OutputFilePath string `json:"output_file_path" mapstructure:"output_file_path"`
}

type Routine struct {
	Role    string `json:"role" mapstructure:"role"`
	Q       string `json:"q" mapstructure:"q"`
	Name    string `json:"name" mapstructure:"name"`
	Message struct {
		FormattedMsg string `json:"formatted_msg" mapstructure:"formatted_msg"`
		Format string `json:"format" mapstructure:"format"`
		Freq   int `json:"freq" mapstructure:"freq"`
	} `json:"message,omitempty" mapstructure:"message"`
	ProcessingTime int `json:"processing_time,omitempty" mapstructure:"processing_time"`
	RefreshTime    int `json:"refresh_time,omitempty" mapstructure:"refresh_time"`
}

var config *Config

// InitializeConfig makes use of viper library to initialize
// config from multiple sources such as json, yaml, toml and
// even environment variables, it returns a pointer to Config
func InitializeConfig() *Config {
	if config != nil {
		return config
	}

	// set the file name of the configurations file
	viper.SetConfigName("config")

	// getting home directory for config path
	homeDir, _ := os.UserHomeDir()

	// set the path to look for the configurations file
	viper.AddConfigPath(".")
	viper.AddConfigPath(path.Join(homeDir, ".async_comm"))
	viper.AddConfigPath("/etc/async-comm")
	viper.AddConfigPath(os.Getenv("CONFIG_PATH"))

	// enable VIPER to read Environment Variables
	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("error reading config file - %s\n", err)
		os.Exit(1)
	}

	hostname, _ := os.Hostname()

	// Set undefined variables
	viper.SetDefault("application.app", hostname)
	viper.SetDefault("application.claim_time", 5000)
	viper.SetDefault("db.host", "localhost")
	viper.SetDefault("db.port", "3306")
	viper.SetDefault("db.username", "")
	viper.SetDefault("app_logger.level", "info")
	viper.SetDefault("app_logger.full_timestamp", true)

	err := viper.Unmarshal(&config)
	if err != nil {
		panic(fmt.Sprintf("unable to decode config file : %v", err))
	}
	return config
}
