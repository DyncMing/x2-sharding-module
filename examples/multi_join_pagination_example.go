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

// UserOrderPaymentResult 查询结果
type UserOrderPaymentResult struct {
	UserID    int64   `gorm:"column:user_id"`
	UserName  string  `gorm:"column:user_name"`
	OrderID   int64   `gorm:"column:order_id"`
	OrderAmount float64 `gorm:"column:order_amount"`
	PaymentAmount float64 `gorm:"column:payment_amount"`
	Status    string  `gorm:"column:payment_status"`
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

	fmt.Println("=== 示例 1: 三表连接查询分页 ===")
	
	// 配置三表连接
	config := sharding.MultiJoinConfig{
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
		},
	}

	var results []map[string]interface{}
	
	// 执行分页查询
	// 注意：在 queryBuilder 中应该使用表别名（基础表名），如 users.user_id
	// 因为我们已经为表设置了别名，别名就是基础表名
	paginator, err := sharding.CrossTableMultiJoinPaginate(
		db,
		config,
		&results,
		1,      // 第1页
		10,     // 每页10条
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("users.user_id, users.name as user_name, orders.order_id, orders.amount as order_amount, payments.amount as payment_amount, payments.status as payment_status").
				Where("users.user_id > ?", 0).
				Order("users.user_id DESC")
		},
	)

	if err != nil {
		log.Printf("Multi-join paginate error: %v\n", err)
	} else {
		fmt.Printf("Page: %d, PageSize: %d, Total: %d, TotalPages: %d\n",
			paginator.Page, paginator.PageSize, paginator.Total, paginator.TotalPages)
		fmt.Printf("Found %d results on page %d\n", len(results), paginator.Page)
	}

	fmt.Println("\n=== 示例 2: 优化的多表连接分页（已知连接键）===")
	
	// 如果已知连接键值，可以使用优化版本
	joinKeys := map[string]interface{}{
		"user_id":  123,
		"order_id": 456,
	}

	var optimizedResults []UserOrderPaymentResult
	
	optimizedPaginator, err := sharding.CrossTableMultiJoinPaginateOptimized(
		db,
		config,
		joinKeys,
		&optimizedResults,
		1,
		10,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("users.user_id, users.name as user_name, orders.order_id, orders.amount as order_amount, payments.amount as payment_amount, payments.status as payment_status").
				Where("users.user_id = ?", 123)
		},
	)

	if err != nil {
		log.Printf("Optimized multi-join paginate error: %v\n", err)
	} else {
		fmt.Printf("Page: %d, Total: %d, TotalPages: %d\n",
			optimizedPaginator.Page, optimizedPaginator.Total, optimizedPaginator.TotalPages)
		fmt.Printf("Found %d results\n", len(optimizedResults))
		for _, result := range optimizedResults {
			fmt.Printf("  User: %s, Order: %d, Amount: %.2f\n",
				result.UserName, result.OrderID, result.OrderAmount)
		}
	}

	fmt.Println("\n=== 示例 3: 多表连接查询计数 ===")
	
	totalCount, err := sharding.CrossTableMultiJoinCount(
		db,
		config,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Where("users.user_id > ?", 0)
		},
	)

	if err != nil {
		log.Printf("Multi-join count error: %v\n", err)
	} else {
		fmt.Printf("Total count: %d\n", totalCount)
	}

	fmt.Println("\n=== 示例 4: 时间分表的多表连接分页查询 ===")
	
	// 创建时间分表策略
	logStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)
	eventStrategy := sharding.NewTimeShardingStrategy("events", "CreatedAt", sharding.TimeShardingByMonth)

	timeConfig := sharding.MultiJoinConfig{
		MainTable: sharding.JoinInfo{
			Strategy:    logStrategy,
			JoinType:    sharding.InnerJoin,
			OnCondition: "1=1",
		},
		JoinTables: []sharding.JoinInfo{
			{
				Strategy:    eventStrategy,
				JoinType:    sharding.LeftJoin,
				OnCondition: "logs.log_id = events.log_id",
			},
		},
	}

	// 指定时间范围
	startTime := time.Now().AddDate(0, -2, 0) // 2个月前
	endTime := time.Now()

	var timeResults []map[string]interface{}
	
	timePaginator, err := sharding.CrossTableMultiJoinPaginateWithTimeRange(
		db,
		timeConfig,
		&timeResults,
		1,
		20,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("logs.*, events.*").Order("logs.created_at DESC")
		},
		startTime,
		endTime,
	)

	if err != nil {
		log.Printf("Time range multi-join paginate error: %v\n", err)
	} else {
		fmt.Printf("Page: %d, Total: %d, Time Range: %s to %s\n",
			timePaginator.Page, timePaginator.Total,
			startTime.Format("2006-01"), endTime.Format("2006-01"))
	}

	fmt.Println("\n=== 示例 5: 使用时间戳进行多表连接分页 ===")
	
	// 使用时间戳作为时间范围
	startTimestamp := time.Now().AddDate(0, -1, 0).Unix() // 1个月前的时间戳
	endTimestamp := time.Now().Unix()

	countWithTimestamp, err := sharding.CrossTableMultiJoinCountWithTimeRange(
		db,
		timeConfig,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Where("logs.level = ?", "INFO")
		},
		startTimestamp,
		endTimestamp,
	)

	if err != nil {
		log.Printf("Count with timestamp error: %v\n", err)
	} else {
		fmt.Printf("Total count with timestamp range: %d\n", countWithTimestamp)
	}

	fmt.Println("\n所有示例执行完成！")
}

