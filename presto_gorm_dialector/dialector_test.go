package presto_gorm_dialector

import (
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"log"
	"os"
	"testing"
	"time"
)

func TestGormDialector(t *testing.T) {
	dsn := "http://user@ip:port?catalog=hive&schema=db"
	conn, err := gorm.Open(Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
		PrepareStmt: true,
		DisableAutomaticPing: false,
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{
				// 慢 SQL 阈值
				SlowThreshold: time.Second * 5,
				LogLevel:      logger.Info,
				//LogLevel: logger.Error,
				Colorful: true,
			},
		),
	})
	//
	if err != nil {
		log.Println(err)
		return
	}

	target := make([]map[string]interface{}, 0)
	db := conn.Table("list").Limit(10)
	if err := db.Find(&target).Error; err != nil{
		log.Println(err)
		return
	}

	log.Println(target)
}
