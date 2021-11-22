package model

import (
	"fmt"
	"time"

	"github.com/cloudreve/Cloudreve/v3/pkg/conf"
	"github.com/cloudreve/Cloudreve/v3/pkg/util"
	"github.com/jinzhu/gorm"

	"strings"

	mysql2 "github.com/cloudreve/Cloudreve/v3/mysql"
	_ "github.com/luyu6056/mysql"
)

// DB 数据库链接单例

var DB *DBStruct

// Init 初始化 MySQL 链接
func Init() {
	util.Log().Info("初始化数据库连接")

	var (
		db  *gorm.DB
		err error
	)
	DB = &DBStruct{}

	switch conf.DatabaseConfig.Type {
	case "mysql":
		ipport := fmt.Sprintf("%s:%d", conf.DatabaseConfig.Host, conf.DatabaseConfig.Port)
		if strings.Contains(conf.DatabaseConfig.Host, ".sock") && conf.DatabaseConfig.Port == 0 {
			ipport = conf.DatabaseConfig.Host
		}
		dsn := fmt.Sprintf("%s:%s@(%s)/%s?charset=%s&parseTime=True&loc=Local",
			conf.DatabaseConfig.User,
			conf.DatabaseConfig.Password,
			ipport,
			conf.DatabaseConfig.Name,
			conf.DatabaseConfig.Charset)
		db, err = gorm.Open(conf.DatabaseConfig.Type, dsn)
		if err != nil {
			util.Log().Panic("连接数据库不成功, %s", err)
		}
		DB.mysqlDb, err = mysql2.Open(dsn)
		if err != nil {
			util.Log().Panic("连接数据库不成功, %s", err)
		}
		err = DB.mysqlDb.Ping()
		if err != nil {
			util.Log().Panic("连接数据库不成功, %s", err)
		}
	default:
		util.Log().Panic("不支持数据库类型: %s", conf.DatabaseConfig.Type)
	}

	//db.SetLogger(util.Log())
	if err != nil {
		util.Log().Panic("连接数据库不成功, %s", err)
	}

	// 处理表前缀
	gorm.DefaultTableNameHandler = func(db *gorm.DB, defaultTableName string) string {
		return conf.DatabaseConfig.TablePrefix + defaultTableName
	}
	mysql2.Tablepre = []byte(conf.DatabaseConfig.TablePrefix)
	// Debug模式下，输出所有 SQL 日志
	if conf.SystemConfig.Debug {
		db.LogMode(true)
	} else {
		db.LogMode(false)
		mysql2.ISDEBUG = false
	}

	//设置连接池
	//空闲
	db.DB().SetMaxIdleConns(50)
	//打开
	db.DB().SetMaxOpenConns(100)
	//超时
	db.DB().SetConnMaxLifetime(time.Second * 30)

	//DB = db
	DB.gormDb = db

	//执行迁移
	migration()
	MakeFolderCache()
}
