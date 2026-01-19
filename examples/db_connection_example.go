package main

import (
	"fmt"
	"log"

	"x2-sharding-module/sharding"
	"gorm.io/gorm"
)

// User 用户模型
type User struct {
	ID     uint   `gorm:"primarykey"`
	UserID int64  `gorm:"column:user_id;not null"`
	Name   string `gorm:"column:name"`
	Email  string `gorm:"column:email"`
}

func main() {
	fmt.Println("=== 示例 1: 自动创建数据库连接 ===")
	
	// DSN 中包含数据库名（数据库可能不存在）
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	
	// 方式 1: 使用默认字符集和排序规则
	db, err := sharding.OpenMySQLWithAutoCreateDB(dsn, &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v\n", err)
	}
	
	fmt.Println("Database connection established successfully!")
	fmt.Println("If the database didn't exist, it has been created automatically.")
	
	// 使用数据库
	var users []User
	db.Find(&users)
	fmt.Printf("Found %d users\n", len(users))

	fmt.Println("\n=== 示例 2: 自定义字符集和排序规则 ===")
	
	dsn2 := "root:password@tcp(localhost:3306)/testdb2?charset=utf8mb4&parseTime=True&loc=Local"
	
	// 方式 2: 指定字符集和排序规则
	db2, err := sharding.OpenWithAutoCreateDB(
		dsn2,
		&gorm.Config{},
		"utf8mb4",           // 字符集
		"utf8mb4_unicode_ci", // 排序规则
	)
	if err != nil {
		log.Printf("Failed to connect: %v\n", err)
	} else {
		fmt.Println("Database created with custom charset and collation!")
		sqlDB, _ := db2.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}

	fmt.Println("\n=== 示例 3: 解析 DSN 字符串 ===")
	
	dsn3 := "root:password@tcp(localhost:3306)/mydb?charset=utf8mb4"
	dsnInfo, err := sharding.ParseDSN(dsn3)
	if err != nil {
		log.Printf("Failed to parse DSN: %v\n", err)
	} else {
		fmt.Printf("Username: %s\n", dsnInfo.Username)
		fmt.Printf("Protocol: %s\n", dsnInfo.Protocol)
		fmt.Printf("Address: %s\n", dsnInfo.Address)
		fmt.Printf("Database: %s\n", dsnInfo.Database)
		fmt.Printf("Params: %s\n", dsnInfo.Params)
	}

	fmt.Println("\n=== 示例 4: 从 DSN 中提取数据库名 ===")
	
	dbName, err := sharding.ExtractDatabaseFromDSN(dsn3)
	if err != nil {
		log.Printf("Failed to extract database name: %v\n", err)
	} else {
		fmt.Printf("Database name: %s\n", dbName)
	}

	fmt.Println("\n=== 示例 5: 替换 DSN 中的数据库名 ===")
	
	newDSN, err := sharding.ReplaceDatabaseInDSN(dsn3, "newdb")
	if err != nil {
		log.Printf("Failed to replace database: %v\n", err)
	} else {
		fmt.Printf("Original DSN: %s\n", dsn3)
		fmt.Printf("New DSN: %s\n", newDSN)
	}

	fmt.Println("\n=== 示例 6: 手动检查和创建数据库 ===")
	
	// 先连接到 MySQL 服务器（不指定数据库）
	serverDSN := "root:password@tcp(localhost:3306)/?charset=utf8mb4"
	serverDB, err := sharding.OpenMySQLWithAutoCreateDB(serverDSN, &gorm.Config{})
	if err != nil {
		log.Printf("Failed to connect to server: %v\n", err)
	} else {
		// 检查数据库是否存在
		exists, err := sharding.DatabaseExists(serverDB, "testdb3")
		if err != nil {
			log.Printf("Failed to check database: %v\n", err)
		} else {
			fmt.Printf("Database 'testdb3' exists: %v\n", exists)
			
			// 如果不存在，创建它
			if !exists {
				err = sharding.CreateDatabase(serverDB, "testdb3", "utf8mb4", "utf8mb4_unicode_ci")
				if err != nil {
					log.Printf("Failed to create database: %v\n", err)
				} else {
					fmt.Println("Database 'testdb3' created successfully!")
				}
			}
		}
		
		sqlDB, _ := serverDB.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}

	fmt.Println("\n=== 示例 7: 确保数据库存在 ===")
	
	serverDB2, err := sharding.OpenMySQLWithAutoCreateDB(serverDSN, &gorm.Config{})
	if err != nil {
		log.Printf("Failed to connect: %v\n", err)
	} else {
		err = sharding.EnsureDatabaseExists(serverDB2, "testdb4", "utf8mb4", "utf8mb4_unicode_ci")
		if err != nil {
			log.Printf("Failed to ensure database: %v\n", err)
		} else {
			fmt.Println("Database 'testdb4' is ensured to exist!")
		}
		
		sqlDB, _ := serverDB2.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}

	fmt.Println("\n所有示例执行完成！")
}

