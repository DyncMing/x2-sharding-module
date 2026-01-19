package main

import (
	"fmt"
	"log"

	"x2-sharding-module/sharding"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// User 用户模型（基于 user_id 进行 Hash 分表）
type User struct {
	ID     uint   `gorm:"primarykey"`
	UserID int64  `gorm:"column:user_id;not null"`
	Name   string `gorm:"column:name"`
	Email  string `gorm:"column:email"`
}

func main() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// 创建基于 Hash 的分表策略（4 张表）
	hashStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
	
	// 注册分表策略
	if err := sharding.RegisterSharding(db, hashStrategy); err != nil {
		log.Fatal("Failed to register sharding:", err)
	}

	// 创建用户（自动路由到对应的分表）
	user1 := &User{UserID: 123, Name: "John", Email: "john@example.com"}
	tableName := hashStrategy.GetTableName("users", 123)
	fmt.Printf("User 123 will be inserted into table: %s\n", tableName)
	
	db.Table(tableName).Create(user1)

	// 查询用户（需要知道在哪个表）
	tableName = hashStrategy.GetTableName("users", 123)
	var foundUser User
	db.Table(tableName).Where("user_id = ?", 123).First(&foundUser)
	fmt.Printf("Found user: %+v\n", foundUser)

	// 跨表查询所有用户
	var allUsers []User
	err = sharding.CrossTableQuery(db, hashStrategy, &allUsers, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("name LIKE ?", "%John%")
	})
	if err != nil {
		log.Printf("Cross table query error: %v\n", err)
	} else {
		fmt.Printf("Found %d users across all tables\n", len(allUsers))
	}

	// 跨表分页查询
	paginator, err := sharding.CrossTablePaginate(db, hashStrategy, &allUsers, 1, 10, func(tx *gorm.DB) *gorm.DB {
		return tx.Order("id DESC")
	})
	if err != nil {
		log.Printf("Cross table paginate error: %v\n", err)
	} else {
		fmt.Printf("Page: %d, Total: %d, TotalPages: %d\n", 
			paginator.Page, paginator.Total, paginator.TotalPages)
	}
}

