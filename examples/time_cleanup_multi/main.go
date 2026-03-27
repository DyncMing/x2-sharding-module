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

	logsStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)
	metricsStrategy := sharding.NewTimeShardingStrategy("metrics", "CreatedAt", sharding.TimeShardingByDay)
	tracesStrategy := sharding.NewTimeShardingStrategy("traces", "CreatedAt", sharding.TimeShardingByHour)
	auditsStrategy := sharding.NewTimeShardingStrategy("audit_logs", "CreatedAt", sharding.TimeShardingByYear)

	configPath := "examples/time_cleanup_multi/config.json"
	registrations := []sharding.ConfigFileShardingRegistration{
		{Strategy: logsStrategy, Model: &models.Log{}},
		{Strategy: metricsStrategy, Model: &models.Metric{}},
		{Strategy: tracesStrategy, Model: &models.Trace{}},
		{Strategy: auditsStrategy, Model: &models.AuditLog{}},
	}

	if err := sharding.RegisterShardingsWithConfigFile(db, configPath, registrations); err != nil {
		log.Fatal("Failed to batch register time sharding strategies:", err)
	}

	now := time.Now()
	entries := []struct {
		label string
		data  interface{}
	}{
		{
			label: "logs(month)",
			data: &models.Log{
				CreatedAt: now,
				Message:   "monthly log",
				Level:     "INFO",
			},
		},
		{
			label: "metrics(day)",
			data: &models.Metric{
				CreatedAt: now,
				Message:   "daily metric",
			},
		},
		{
			label: "traces(hour)",
			data: &models.Trace{
				CreatedAt: now,
				Message:   "hourly trace",
			},
		},
		{
			label: "audit_logs(year)",
			data: &models.AuditLog{
				CreatedAt: now,
				Message:   "yearly audit",
			},
		},
		{
			label: "logs(previous month)",
			data: &models.Log{
				CreatedAt: now.AddDate(0, -1, 0),
				Message:   "older monthly log",
				Level:     "WARN",
			},
		},
	}

	for _, entry := range entries {
		if err := db.Create(entry.data).Error; err != nil {
			log.Fatalf("Failed to create %s: %v", entry.label, err)
		}
		fmt.Printf("Created %s\n", entry.label)
	}

	tables, err := db.Migrator().GetTables()
	if err != nil {
		log.Fatal("Failed to list tables:", err)
	}

	fmt.Printf("Config file: %s\n", configPath)
	fmt.Printf("Current tables: %v\n", tables)
	fmt.Println("Different time sharding strategies now share one config file and clean up independently by baseTable/unit/default policies.")
}
