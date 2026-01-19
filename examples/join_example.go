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
	ID     uint   `gorm:"primarykey;column:id"`
	UserID int64  `gorm:"column:user_id;not null"`
	Name   string `gorm:"column:name"`
}

// Order 订单模型
type Order struct {
	ID      uint      `gorm:"primarykey;column:id"`
	UserID  int64     `gorm:"column:user_id;not null"`
	Amount  float64   `gorm:"column:amount"`
	Status  string    `gorm:"column:status"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func main() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// 创建基于 Hash 的分表策略
	userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
	orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)
	
	// 注册分表策略
	sharding.RegisterSharding(db, userStrategy)
	sharding.RegisterSharding(db, orderStrategy)

	// 演示跨表连接查询
	// 注意：由于两个表都是基于 UserID 分表的，所以 users_0 应该只连接 orders_0
	userID := int64(123)
	userTable := userStrategy.GetTableName("users", userID)
	orderTable := orderStrategy.GetTableName("orders", userID)

	fmt.Printf("User table: %s, Order table: %s\n", userTable, orderTable)

	// 执行连接查询
	type UserOrder struct {
		UserID   int64   `gorm:"column:user_id"`
		UserName string  `gorm:"column:name"`
		OrderID  uint    `gorm:"column:order_id"`
		Amount   float64 `gorm:"column:amount"`
		Status   string  `gorm:"column:status"`
	}

	var results []UserOrder
	
	// 直接使用 GORM 的 Join（同一分表内）
	err = db.Table(userTable).
		Select("users.user_id, users.name, orders.id as order_id, orders.amount, orders.status").
		Joins(fmt.Sprintf("LEFT JOIN %s ON users.user_id = orders.user_id", orderTable)).
		Where("users.user_id = ?", userID).
		Find(&results).Error

	if err != nil {
		log.Printf("Join query error: %v\n", err)
	} else {
		fmt.Printf("Found %d user-order pairs\n", len(results))
		for _, result := range results {
			fmt.Printf("User: %s, Order: %d, Amount: %.2f\n", 
				result.UserName, result.OrderID, result.Amount)
		}
	}

	// 使用跨表连接查询（支持跨不同分表）
	// 注意：这个功能适用于两个表使用不同分表策略的情况
	var allResults []map[string]interface{}
	err = sharding.CrossTableJoin(
		db,
		userStrategy,
		orderStrategy,
		sharding.LeftJoin,
		"users.user_id = orders.user_id",
		&allResults,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("users.user_id, users.name, orders.id as order_id, orders.amount").
				Where("users.user_id = ?", userID)
		},
	)

	if err != nil {
		log.Printf("Cross table join error: %v\n", err)
	} else {
		fmt.Printf("Cross table join found %d results\n", len(allResults))
	}
}

