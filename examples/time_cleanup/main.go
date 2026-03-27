package main

import (
	"fmt"
	"log"
	"time"

	"x2-sharding-module/examples/internal/models"
	"x2-sharding-module/sharding"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	strategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)

	now := time.Now()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	configPath := "examples/time_cleanup/config.json"

	// 示例：先预创建最近 6 个月的表，便于演示清理。
	err = sharding.AutoMigrateTimeSharding(db, strategy, &models.Log{}, sharding.AutoMigrateOptions{
		SkipIfExists: true,
		TimeRange: &sharding.AutoMigrateTimeRange{
			StartTime: monthStart.AddDate(0, -6, 0),
			EndTime:   now,
		},
	})
	if err != nil {
		log.Fatal("Failed to auto migrate time sharding tables:", err)
	}

	beforeTables, err := db.Migrator().GetTables()
	if err != nil {
		log.Fatal("Failed to list tables before auto cleanup:", err)
	}
	fmt.Printf("Tables before auto cleanup trigger: %v\n", beforeTables)

	if err := sharding.RegisterShardingWithConfigFile(db, strategy, &models.Log{}, configPath); err != nil {
		log.Fatal("Failed to register sharding with config file:", err)
	}

	logEntry := &models.Log{
		CreatedAt: now,
		Message:   "write will trigger auto cleanup",
		Level:     "INFO",
	}
	if err := db.Create(logEntry).Error; err != nil {
		log.Fatal("Failed to create log with auto cleanup:", err)
	}

	tableNames, err := db.Migrator().GetTables()
	if err != nil {
		log.Fatal("Failed to list tables after auto cleanup:", err)
	}

	fmt.Printf("Auto cleanup config file: %s\n", configPath)
	fmt.Printf("Tables after auto cleanup trigger: %v\n", tableNames)
}
