package main

import (
	"fmt"
	"log"

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
	ID      uint    `gorm:"primarykey;column:id"`
	UserID  int64   `gorm:"column:user_id;not null"`
	OrderID int64   `gorm:"column:order_id;not null"`
	Amount  float64 `gorm:"column:amount"`
}

// Payment 支付模型
type Payment struct {
	ID      uint    `gorm:"primarykey;column:id"`
	OrderID int64   `gorm:"column:order_id;not null"`
	Amount  float64 `gorm:"column:amount"`
	Status  string  `gorm:"column:status"`
}

// UserOrderPayment 连接查询结果
type UserOrderPayment struct {
	UserName string  `gorm:"column:user_name"`
	OrderID  int64   `gorm:"column:order_id"`
	Amount   float64 `gorm:"column:amount"`
	Status   string  `gorm:"column:payment_status"`
}

func main() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// 创建分表策略
	userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
	orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)
	paymentStrategy := sharding.NewHashShardingStrategy("payments", "OrderID", 4)

	// 注册分表策略
	sharding.RegisterSharding(db, userStrategy)
	sharding.RegisterSharding(db, orderStrategy)
	sharding.RegisterSharding(db, paymentStrategy)

	// 示例 1: 三表连接查询（用户-订单-支付）
	fmt.Println("=== 三表连接查询示例 ===")
	var results []map[string]interface{}

	config := sharding.MultiJoinConfig{
		MainTable: sharding.JoinInfo{
			Strategy:    userStrategy,
			JoinType:    sharding.InnerJoin,
			OnCondition: "1=1", // 主表不需要 ON 条件
		},
		JoinTables: []sharding.JoinInfo{
			{
				Strategy:    orderStrategy,
				JoinType:    sharding.LeftJoin,
				OnCondition: "users.user_id = orders.user_id",
			},
			{
				Strategy:    paymentStrategy,
				JoinType:    sharding.LeftJoin,
				OnCondition: "orders.order_id = payments.order_id",
			},
		},
	}

	err = sharding.CrossTableMultiJoin(db, config, &results, func(tx *gorm.DB) *gorm.DB {
		return tx.Select("users.name as user_name, orders.order_id, payments.amount, payments.status as payment_status").
			Where("users.user_id = ?", 123)
	})

	if err != nil {
		log.Printf("Multi-join query error: %v\n", err)
	} else {
		fmt.Printf("Found %d results\n", len(results))
		for _, result := range results {
			fmt.Printf("Result: %+v\n", result)
		}
	}

	// 示例 2: 优化的多表连接（根据连接键值只查询相关表）
	fmt.Println("\n=== 优化的三表连接查询示例 ===")
	var optimizedResults []UserOrderPayment

	joinKeys := map[string]interface{}{
		"user_id":  123,
		"order_id": 456,
	}

	err = sharding.CrossTableMultiJoinOptimized(db, config, joinKeys, &optimizedResults, func(tx *gorm.DB) *gorm.DB {
		return tx.Select("users.name as user_name, orders.order_id, payments.amount, payments.status as payment_status").
			Where("users.user_id = ?", 123)
	})

	if err != nil {
		log.Printf("Optimized multi-join query error: %v\n", err)
	} else {
		fmt.Printf("Found %d results\n", len(optimizedResults))
		for _, result := range optimizedResults {
			fmt.Printf("User: %s, Order: %d, Amount: %.2f, Status: %s\n",
				result.UserName, result.OrderID, result.Amount, result.Status)
		}
	}

	// 示例 3: 四表连接查询（添加商品表）
	fmt.Println("\n=== 四表连接查询示例 ===")
	productStrategy := sharding.NewHashShardingStrategy("products", "ProductID", 4)

	config4 := sharding.MultiJoinConfig{
		MainTable: sharding.JoinInfo{
			Strategy:    userStrategy,
			JoinType:    sharding.InnerJoin,
			OnCondition: "1=1",
		},
		JoinTables: []sharding.JoinInfo{
			{
				Strategy:    orderStrategy,
				JoinType:    sharding.LeftJoin,
				OnCondition: "users.user_id = orders.user_id",
			},
			{
				Strategy:    paymentStrategy,
				JoinType:    sharding.LeftJoin,
				OnCondition: "orders.order_id = payments.order_id",
			},
			{
				Strategy:    productStrategy,
				JoinType:    sharding.LeftJoin,
				OnCondition: "orders.product_id = products.product_id",
				Alias:       "p",
			},
		},
	}

	var fourTableResults []map[string]interface{}
	err = sharding.CrossTableMultiJoin(db, config4, &fourTableResults, func(tx *gorm.DB) *gorm.DB {
		return tx.Select("users.name, orders.order_id, payments.amount, p.name as product_name").
			Where("users.user_id = ?", 123)
	})

	if err != nil {
		log.Printf("Four-table join query error: %v\n", err)
	} else {
		fmt.Printf("Found %d results\n", len(fourTableResults))
	}
}
