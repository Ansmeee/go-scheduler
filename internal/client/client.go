package client

import (
	"fmt"
	"go-scheduler/internal/config"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var MySQLMaster *gorm.DB
var MySQLSlaver *gorm.DB

var RedisMaster *redis.Client
var RedisSlaver *redis.Client

func Register() {
	registerMysqlClient()
	registerRedisClient()
}

func Close() {

}

func registerRedisClient() {
	RedisMaster = initRedisClient(config.AppCfg.RedisMaster)
	RedisSlaver = initRedisClient(config.AppCfg.RedisSlaver)
}

func initRedisClient(conf config.RedisConfig) *redis.Client {
	fmt.Println("init redis client")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		Password: conf.Password,
		DB:       conf.Db,
	})
	fmt.Println("init redis client success")
	return redisClient
}

func registerMysqlClient() {
	MySQLMaster = initMySQLClient(config.AppCfg.MySQLMaster)
	MySQLSlaver = initMySQLClient(config.AppCfg.MySQLSlaver)
}

func initMySQLClient(conf config.DBConfig) *gorm.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", conf.User, conf.Password, conf.Host, conf.Port, conf.Database)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database:" + err.Error())
		return nil
	}

	sqlDB, err := db.DB()
	if err != nil {
		panic("failed to connect database:" + err.Error())
		return nil
	}

	sqlDB.SetMaxOpenConns(conf.MaxConn)
	sqlDB.SetMaxIdleConns(conf.MaxIdle)
	sqlDB.SetConnMaxLifetime(time.Hour)
	sqlDB.SetConnMaxIdleTime(time.Hour)

	return db
}
