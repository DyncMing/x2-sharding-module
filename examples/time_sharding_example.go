package main

import (
	"fmt"
	"log"
	"time"

	"x2-sharding-module/sharding"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Log 日志模型（基于 created_at 进行时间分表）
type Log struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
	Level     string    `gorm:"column:level"`
}

func main() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// 创建基于时间的分表策略（按月分表）
	timeStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)

	// 注册分表策略
	if err := sharding.RegisterSharding(db, timeStrategy); err != nil {
		log.Fatal("Failed to register sharding:", err)
	}

	// 创建日志（自动路由到对应月份的表）
	log1 := &Log{
		CreatedAt: time.Now(),
		Message:   "Test log message",
		Level:     "INFO",
	}

	tableName := timeStrategy.GetTableName("logs", log1.CreatedAt)
	fmt.Printf("Log will be inserted into table: %s\n", tableName)

	db.Table(tableName).Create(log1)

	// 查询指定时间范围的日志
	startTime := time.Now().AddDate(0, -2, 0) // 2个月前
	endTime := time.Now()

	tableNames := timeStrategy.GetAllTableNamesInRange("logs", startTime, endTime)
	fmt.Printf("Tables in range: %v\n", tableNames)

	// 跨表查询最近一个月的日志
	var recentLogs []Log
	err = sharding.CrossTableQuery(db, timeStrategy, &recentLogs, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("level = ?", "INFO").Order("created_at DESC")
	})
	if err != nil {
		log.Printf("Cross table query error: %v\n", err)
	} else {
		fmt.Printf("Found %d logs across tables\n", len(recentLogs))
	}

	// 跨表分页查询
	paginator, err := sharding.CrossTablePaginate(db, timeStrategy, &recentLogs, 1, 20, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("level = ?", "INFO").Order("created_at DESC")
	})
	if err != nil {
		log.Printf("Cross table paginate error: %v\n", err)
	} else {
		fmt.Printf("Page: %d, Total: %d, TotalPages: %d\n",
			paginator.Page, paginator.Total, paginator.TotalPages)
	}

	// 演示不同的时间分表单位
	fmt.Println("\n--- Different Time Sharding Units ---")

	yearStrategy := sharding.NewTimeShardingStrategy("logs_year", "CreatedAt", sharding.TimeShardingByYear)
	fmt.Printf("Year table: %s\n", yearStrategy.GetTableName("logs_year", time.Now()))

	dayStrategy := sharding.NewTimeShardingStrategy("logs_day", "CreatedAt", sharding.TimeShardingByDay)
	fmt.Printf("Day table: %s\n", dayStrategy.GetTableName("logs_day", time.Now()))

	hourStrategy := sharding.NewTimeShardingStrategy("logs_hour", "CreatedAt", sharding.TimeShardingByHour)
	fmt.Printf("Hour table: %s\n", hourStrategy.GetTableName("logs_hour", time.Now()))
}
