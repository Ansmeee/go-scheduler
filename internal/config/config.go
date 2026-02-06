package config

import (
	"github.com/spf13/viper"
)

var AppCfg Config

type Config struct {
	MySQLMaster DBConfig    `mapstructure:"mysql_master"`
	MySQLSlaver DBConfig    `mapstructure:"mysql_slaver"`
	RedisMaster RedisConfig `mapstructure:"redis_master"`
	RedisSlaver RedisConfig `mapstructure:"redis_slaver"`
}

type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	Db       int    `mapstructure:"db"`
}

type DBConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	MaxIdle  int    `mapstructure:"max_idle"`
	MaxConn  int    `mapstructure:"max_conn"`
}

func Load() error {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	if err := viper.Unmarshal(&AppCfg); err != nil {
		return err
	}

	return nil
}
