package main

import (
	"fmt"
	"log"
	"time"

	"x2-sharding-module/sharding"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// LogWithTime 使用 time.Time 类型的日志模型
type LogWithTime struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
}

// LogWithTimestamp 使用 int64 时间戳类型的日志模型
type LogWithTimestamp struct {
	ID        uint   `gorm:"primarykey"`
	CreatedAt int64  `gorm:"column:created_at;not null"` // Unix 时间戳（秒）
	Message   string `gorm:"column:message"`
}

// LogWithTimestampMs 使用 int64 毫秒时间戳类型的日志模型
type LogWithTimestampMs struct {
	ID        uint   `gorm:"primarykey"`
	CreatedAt int64  `gorm:"column:created_at;not null"` // Unix 时间戳（毫秒）
	Message   string `gorm:"column:message"`
}

// LogWithDate 使用 string 日期类型的日志模型
type LogWithDate struct {
	ID        uint   `gorm:"primarykey"`
	CreatedAt string `gorm:"column:created_at;not null"` // 日期字符串 YYYY-MM-DD
	Message   string `gorm:"column:message"`
}

func main() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	fmt.Println("=== 示例 1: time.Time 类型的时间分表 ===")
	// 使用默认的自动识别类型（会识别为 time.Time）
	timeStrategy1 := sharding.NewTimeShardingStrategy("logs_time", "CreatedAt", sharding.TimeShardingByMonth)
	
	// 或者明确指定类型
	timeStrategy1 = sharding.NewTimeShardingStrategyWithType(
		"logs_time",
		"CreatedAt",
		sharding.TimeShardingByMonth,
		sharding.TimeFieldTypeTime,
	)

	log1 := &LogWithTime{
		CreatedAt: time.Now(),
		Message:   "Test log with time.Time",
	}

	tableName := timeStrategy1.GetTableName("logs_time", log1.CreatedAt)
	fmt.Printf("Log with time.Time will be in table: %s\n", tableName)

	// 使用 int64 时间戳也能正确转换
	timestamp := time.Now().Unix()
	tableName = timeStrategy1.GetTableName("logs_time", timestamp)
	fmt.Printf("Log with int64 timestamp %d will be in table: %s\n", timestamp, tableName)

	// 使用字符串日期也能正确转换
	dateStr := "2024-01-15"
	tableName = timeStrategy1.GetTableName("logs_time", dateStr)
	fmt.Printf("Log with date string '%s' will be in table: %s\n", dateStr, tableName)

	fmt.Println("\n=== 示例 2: int64 时间戳（秒）类型的时间分表 ===")
	timestampStrategy := sharding.NewTimeShardingStrategyWithType(
		"logs_timestamp",
		"CreatedAt",
		sharding.TimeShardingByDay,
		sharding.TimeFieldTypeTimestamp,
	)

	log2 := &LogWithTimestamp{
		CreatedAt: time.Now().Unix(), // Unix 时间戳（秒）
		Message:   "Test log with timestamp",
	}

	tableName = timestampStrategy.GetTableName("logs_timestamp", log2.CreatedAt)
	fmt.Printf("Log with timestamp %d will be in table: %s\n", log2.CreatedAt, tableName)

	// 也可以直接传入 int64 值
	tableName = timestampStrategy.GetTableName("logs_timestamp", int64(1705305600)) // 2024-01-15
	fmt.Printf("Log with timestamp 1705305600 will be in table: %s\n", tableName)

	fmt.Println("\n=== 示例 3: int64 毫秒时间戳类型的时间分表 ===")
	timestampMsStrategy := sharding.NewTimeShardingStrategyWithType(
		"logs_timestamp_ms",
		"CreatedAt",
		sharding.TimeShardingByHour,
		sharding.TimeFieldTypeTimestampMs,
	)

	log3 := &LogWithTimestampMs{
		CreatedAt: time.Now().UnixMilli(), // Unix 时间戳（毫秒）
		Message:   "Test log with timestamp ms",
	}

	tableName = timestampMsStrategy.GetTableName("logs_timestamp_ms", log3.CreatedAt)
	fmt.Printf("Log with timestamp ms %d will be in table: %s\n", log3.CreatedAt, tableName)

	fmt.Println("\n=== 示例 4: string 日期类型的时间分表 ===")
	dateStrategy := sharding.NewTimeShardingStrategyWithType(
		"logs_date",
		"CreatedAt",
		sharding.TimeShardingByDay,
		sharding.TimeFieldTypeDate,
	)

	log4 := &LogWithDate{
		CreatedAt: "2024-01-15", // 日期字符串
		Message:   "Test log with date string",
	}

	tableName = dateStrategy.GetTableName("logs_date", log4.CreatedAt)
	fmt.Printf("Log with date string '%s' will be in table: %s\n", log4.CreatedAt, tableName)

	// 使用 time.Time 也能正确转换
	tableName = dateStrategy.GetTableName("logs_date", time.Now())
	fmt.Printf("Log with time.Time will be in table: %s\n", tableName)

	fmt.Println("\n=== 示例 5: 跨表查询（使用时间戳范围）===")
	var logs []LogWithTimestamp
	
	// 使用时间戳范围查询
	startTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	endTimestamp := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC).Unix()

	err = sharding.CrossTableQueryWithTimeRange(
		db,
		timestampStrategy,
		&logs,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Where("message LIKE ?", "%test%")
		},
		startTimestamp, // 使用 int64 时间戳
		endTimestamp,
	)

	if err != nil {
		log.Printf("Cross table query error: %v\n", err)
	} else {
		fmt.Printf("Found %d logs in timestamp range\n", len(logs))
	}

	fmt.Println("\n=== 示例 6: 跨表查询（使用日期字符串范围）===")
	var dateLogs []LogWithDate
	
	// 使用日期字符串范围查询
	err = sharding.CrossTableQueryWithTimeRange(
		db,
		dateStrategy,
		&dateLogs,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Where("message LIKE ?", "%test%")
		},
		"2024-01-01", // 使用字符串日期
		"2024-01-31",
	)

	if err != nil {
		log.Printf("Cross table query error: %v\n", err)
	} else {
		fmt.Printf("Found %d logs in date range\n", len(dateLogs))
	}

	fmt.Println("\n=== 示例 7: 使用 GetAllTableNamesInRangeWithValues ===")
	// 支持混合类型的时间范围
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTimestamp2 := int64(1706745599) // 2024-02-01 的 Unix 时间戳

	tableNames := timestampStrategy.GetAllTableNamesInRangeWithValues(
		"logs_timestamp",
		startTime,        // time.Time
		endTimestamp2,    // int64 时间戳
	)

	fmt.Printf("Tables in range: %v\n", tableNames)

	fmt.Println("\n=== 示例 8: 自动识别时间类型 ===")
	// 使用自动识别类型，系统会自动检测字段类型
	autoStrategy := sharding.NewTimeShardingStrategy(
		"logs_auto",
		"CreatedAt",
		sharding.TimeShardingByMonth,
	)

	// 测试不同的输入类型
	testValues := []interface{}{
		time.Now(),                    // time.Time
		time.Now().Unix(),            // int64 (秒)
		time.Now().UnixMilli(),       // int64 (毫秒)
		"2024-01-15",                 // 日期字符串
		"2024-01-15 12:00:00",       // 日期时间字符串
		int32(time.Now().Unix()),     // int32 时间戳
		uint64(time.Now().Unix()),    // uint64 时间戳
	}

	fmt.Println("Auto-detection test results:")
	for _, val := range testValues {
		tableName := autoStrategy.GetTableName("logs_auto", val)
		fmt.Printf("  Value %v -> Table: %s\n", val, tableName)
	}
}

