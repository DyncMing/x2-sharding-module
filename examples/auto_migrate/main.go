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
	err = sharding.AutoMigrate(db, hashStrategy, &models.UserWithEmail{}, sharding.AutoMigrateOptions{
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

	err = sharding.AutoMigrate(db, timeStrategy, &models.Log{}, sharding.AutoMigrateOptions{
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
	err = sharding.RegisterShardingWithAutoCreate(db, hashStrategy, &models.UserWithEmail{})
	if err != nil {
		log.Printf("Register sharding error: %v\n", err)
	}

	// 插入数据时，如果表不存在会自动创建
	user := &models.UserWithEmail{
		UserID: 123,
		Name:   "John",
		Email:  "john@example.com",
	}

	tableName := hashStrategy.GetTableName("users", 123)
	fmt.Printf("User will be inserted into table: %s\n", tableName)
	err = db.Create(user).Error
	if err != nil {
		log.Printf("Create user error: %v\n", err)
	} else {
		fmt.Println("User created successfully!")
	}

	fmt.Println("\n=== 示例 4: 时间分表自动创建（按需）===")
	// 注册时间分表策略并启用自动创建
	err = sharding.RegisterShardingWithAutoCreate(db, timeStrategy, &models.Log{})
	if err != nil {
		log.Printf("Register time sharding error: %v\n", err)
	}

	// 插入日志时，如果对应的月份表不存在会自动创建
	log1 := &models.Log{
		CreatedAt: time.Now(),
		Message:   "Test log",
		Level:     "INFO",
	}

	tableName = timeStrategy.GetTableName("logs", log1.CreatedAt)
	fmt.Printf("Log will be inserted into table: %s\n", tableName)
	err = db.Create(log1).Error
	if err != nil {
		log.Printf("Create log error: %v\n", err)
	} else {
		fmt.Println("Log created successfully!")
	}

	fmt.Println("\n=== 示例 4.1: 使用显式封装按分表写入并自动建表 ===")
	log2 := &models.Log{
		CreatedAt: time.Now().AddDate(0, 1, 0),
		Message:   "Explicit sharded create",
		Level:     "WARN",
	}
	err = sharding.CreateShardedWithAutoCreate(db, timeStrategy, log2, &models.Log{})
	if err != nil {
		log.Printf("CreateShardedWithAutoCreate error: %v\n", err)
	} else {
		fmt.Printf("Explicit sharded create succeeded for table: %s\n", timeStrategy.GetTableName("logs", log2.CreatedAt))
	}

	fmt.Println("\n=== 示例 5: 批量创建多个策略的分表 ===")
	strategies := []sharding.ShardingStrategy{
		hashStrategy,
		timeStrategy,
	}

	exampleModels := map[string]interface{}{
		"users": &models.UserWithEmail{},
		"logs":  &models.Log{},
	}

	// 批量自动迁移
	err = sharding.AutoMigrateAll(db, strategies, exampleModels, sharding.AutoMigrateOptions{
		SkipIfExists: true,
	})
	if err != nil {
		log.Printf("Auto migrate all error: %v\n", err)
	} else {
		fmt.Println("All strategies migrated successfully!")
	}

	fmt.Println("\n=== 示例 6: 范围分表自动创建 ===")
	rangeStrategy := sharding.NewRangeShardingStrategy("products", "ProductID", 10000, 5)

	err = sharding.AutoMigrate(db, rangeStrategy, &models.RangeProduct{}, sharding.AutoMigrateOptions{
		SkipIfExists: true,
	})
	if err != nil {
		log.Printf("Auto migrate range sharding error: %v\n", err)
	} else {
		fmt.Println("Range sharding tables created successfully!")
	}

	fmt.Println("\n=== 示例 7: 取模分表自动创建 ===")
	moduloStrategy := sharding.NewModuloShardingStrategy("orders", "OrderID", 4)

	err = sharding.AutoMigrate(db, moduloStrategy, &models.ModuloOrder{}, sharding.AutoMigrateOptions{
		SkipIfExists: true,
	})
	if err != nil {
		log.Printf("Auto migrate modulo sharding error: %v\n", err)
	} else {
		fmt.Println("Modulo sharding tables created successfully!")
	}

	fmt.Println("\n所有示例执行完成！")
}
