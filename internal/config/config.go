package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/joho/godotenv"
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
	if err := godotenv.Load(); err != nil {
		return err
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./configs")

	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	configFileContent, err := os.ReadFile(viper.ConfigFileUsed())
	if err != nil {
		return err
	}

	re := regexp.MustCompile(`\${([^}]+)}`)
	result := re.ReplaceAllStringFunc(string(configFileContent), func(match string) string {
		// 提取环境变量名称（去掉${}部分）
		envVar := match[2 : len(match)-1]
		// 获取环境变量值，如果不存在则保持原样
		if value := os.Getenv(envVar); value != "" {
			return value
		}
		return match
	})

	// 使用处理后的配置内容
	if err = viper.ReadConfig(strings.NewReader(result)); err != nil {
		return err
	}

	if err = viper.Unmarshal(&AppCfg); err != nil {
		return err
	}

	fmt.Println(AppCfg.MySQLMaster)
	return nil
}
