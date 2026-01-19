package main

import (
	"fmt"
	"log"
	"time"

	"x2-sharding-module/sharding"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// User 用户模型
type User struct {
	ID     uint   `gorm:"primarykey"`
	UserID int64  `gorm:"column:user_id;not null;index"`
	Name   string `gorm:"column:name"`
	Email  string `gorm:"column:email"`
}

// Log 日志模型
type Log struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `gorm:"column:created_at;not null;index"`
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

	fmt.Println("=== 示例 1: Hash 分表自动创建所有表 ===")
	// 创建 Hash 分表策略
	hashStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)

	// 方式 1: 使用 AutoMigrate 自动创建所有分表
	err = sharding.AutoMigrate(db, hashStrategy, &User{}, sharding.AutoMigrateOptions{
		SkipIfExists: true, // 如果表已存在则跳过
	})
	if err != nil {
		log.Printf("Auto migrate error: %v\n", err)
	} else {
		fmt.Println("All hash sharding tables created successfully!")
	}

	// 方式 2: 使用 CreateAllShardingTables（使用 SQL）
	createTableSQL := `
		CREATE TABLE users (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			name VARCHAR(255),
			email VARCHAR(255),
			INDEX idx_user_id (user_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	err = sharding.CreateAllShardingTables(db, hashStrategy, createTableSQL, true)
	if err != nil {
		log.Printf("Create all tables error: %v\n", err)
	}

	fmt.Println("\n=== 示例 2: 时间分表自动创建（指定时间范围）===")
	timeStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)

	// 创建指定时间范围的表（最近 3 个月）
	startTime := time.Now().AddDate(0, -3, 0)
	endTime := time.Now()

	err = sharding.AutoMigrate(db, timeStrategy, &Log{}, sharding.AutoMigrateOptions{
		SkipIfExists: true,
		TimeRange: &sharding.AutoMigrateTimeRange{
			StartTime: startTime,
			EndTime:   endTime,
		},
	})
	if err != nil {
		log.Printf("Auto migrate time sharding error: %v\n", err)
	} else {
		fmt.Printf("Time sharding tables from %s to %s created successfully!\n",
			startTime.Format("2006-01"), endTime.Format("2006-01"))
	}

	fmt.Println("\n=== 示例 3: 插入数据时自动创建表 ===")
	// 注册分表策略并启用自动创建表功能
	err = sharding.RegisterShardingWithAutoCreate(db, hashStrategy, &User{})
	if err != nil {
		log.Printf("Register sharding error: %v\n", err)
	}

	// 插入数据时，如果表不存在会自动创建
	user := &User{
		UserID: 123,
		Name:   "John",
		Email:  "john@example.com",
	}

	tableName := hashStrategy.GetTableName("users", 123)
	fmt.Printf("User will be inserted into table: %s\n", tableName)

	// 确保表存在
	err = sharding.EnsureTableExists(db, hashStrategy, 123, &User{})
	if err != nil {
		log.Printf("Ensure table exists error: %v\n", err)
	}

	err = db.Table(tableName).Create(user).Error
	if err != nil {
		log.Printf("Create user error: %v\n", err)
	} else {
		fmt.Println("User created successfully!")
	}

	fmt.Println("\n=== 示例 4: 时间分表自动创建（按需）===")
	// 注册时间分表策略并启用自动创建
	err = sharding.RegisterShardingWithAutoCreate(db, timeStrategy, &Log{})
	if err != nil {
		log.Printf("Register time sharding error: %v\n", err)
	}

	// 插入日志时，如果对应的月份表不存在会自动创建
	log1 := &Log{
		CreatedAt: time.Now(),
		Message:   "Test log",
		Level:     "INFO",
	}

	tableName = timeStrategy.GetTableName("logs", log1.CreatedAt)
	fmt.Printf("Log will be inserted into table: %s\n", tableName)

	// 确保表存在
	err = sharding.EnsureTableExists(db, timeStrategy, log1.CreatedAt, &Log{})
	if err != nil {
		log.Printf("Ensure table exists error: %v\n", err)
	}

	err = db.Table(tableName).Create(log1).Error
	if err != nil {
		log.Printf("Create log error: %v\n", err)
	} else {
		fmt.Println("Log created successfully!")
	}

	fmt.Println("\n=== 示例 5: 批量创建多个策略的分表 ===")
	strategies := []sharding.ShardingStrategy{
		hashStrategy,
		timeStrategy,
	}

	models := map[string]interface{}{
		"users": &User{},
		"logs":  &Log{},
	}

	// 批量自动迁移
	err = sharding.AutoMigrateAll(db, strategies, models, sharding.AutoMigrateOptions{
		SkipIfExists: true,
	})
	if err != nil {
		log.Printf("Auto migrate all error: %v\n", err)
	} else {
		fmt.Println("All strategies migrated successfully!")
	}

	fmt.Println("\n=== 示例 6: 范围分表自动创建 ===")
	rangeStrategy := sharding.NewRangeShardingStrategy("products", "ProductID", 10000, 5)

	err = sharding.AutoMigrate(db, rangeStrategy, &struct {
		ID        uint   `gorm:"primarykey"`
		ProductID int64  `gorm:"column:product_id;not null;index"`
		Name      string `gorm:"column:name"`
	}{}, sharding.AutoMigrateOptions{
		SkipIfExists: true,
	})
	if err != nil {
		log.Printf("Auto migrate range sharding error: %v\n", err)
	} else {
		fmt.Println("Range sharding tables created successfully!")
	}

	fmt.Println("\n=== 示例 7: 取模分表自动创建 ===")
	moduloStrategy := sharding.NewModuloShardingStrategy("orders", "OrderID", 4)

	err = sharding.AutoMigrate(db, moduloStrategy, &struct {
		ID      uint   `gorm:"primarykey"`
		OrderID int64  `gorm:"column:order_id;not null;index"`
		Amount  string `gorm:"column:amount"`
	}{}, sharding.AutoMigrateOptions{
		SkipIfExists: true,
	})
	if err != nil {
		log.Printf("Auto migrate modulo sharding error: %v\n", err)
	} else {
		fmt.Println("Modulo sharding tables created successfully!")
	}

	fmt.Println("\n所有示例执行完成！")
}
