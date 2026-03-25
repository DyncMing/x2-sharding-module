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

// UserEventRow 混合 Hash + Time 分表连接分页的结果结构。
type UserEventRow struct {
	UserID    int64  `gorm:"column:user_id"`
	UserName  string `gorm:"column:user_name"`
	EventID   int64  `gorm:"column:event_id"`
	EventName string `gorm:"column:event_name"`
}

func main() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// 主表 users：Hash 分表；子表 events：Time 分表（按月）
	userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
	eventStrategy := sharding.NewTimeShardingStrategy("events", "CreatedAt", sharding.TimeShardingByMonth)

	if err := sharding.RegisterSharding(db, userStrategy); err != nil {
		log.Fatalf("Failed to register user sharding: %v", err)
	}
	if err := sharding.RegisterSharding(db, eventStrategy); err != nil {
		log.Fatalf("Failed to register event sharding: %v", err)
	}

	fmt.Println("=== Hash 主表 + Time 子表 的混合分页查询示例 ===")

	users := []models.UserBasic{
		{UserID: 1001, Name: "alice"},
		{UserID: 1002, Name: "bob"},
		{UserID: 1003, Name: "carol"},
	}
	for _, user := range users {
		if err := upsertUser(db, userStrategy, user); err != nil {
			log.Fatalf("Failed to seed user %d: %v", user.UserID, err)
		}
	}

	jan := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	mar := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)

	events := []models.Event{
		{EventID: 9001, UserID: 1001, Name: "login", CreatedAt: jan},
		{EventID: 9002, UserID: 1002, Name: "purchase", CreatedAt: feb},
		{EventID: 9003, UserID: 1003, Name: "logout", CreatedAt: mar},
	}
	for _, event := range events {
		if err := upsertEvent(db, eventStrategy, event); err != nil {
			log.Fatalf("Failed to seed event %d: %v", event.EventID, err)
		}
	}

	fmt.Printf("Seeded users into hash tables like: %s\n", userStrategy.GetTableName("users", users[0].UserID))
	fmt.Printf("Seeded events into time tables like: %s\n", eventStrategy.GetTableName("events", jan))

	// 演示分页：page=1, pageSize=1，只返回范围内的第一条列表数据。
	// 注意：这里故意把结束时间放前面，开始时间放后面，库内部会自动纠正时间范围。
	var rows []UserEventRow
	paginator, err := sharding.CrossTableJoinPaginateWithTimeRange(
		db,
		userStrategy,
		eventStrategy,
		sharding.InnerJoin,
		"users.user_id = events.user_id",
		&rows,
		1,
		1,
		func(tx *gorm.DB) *gorm.DB {
			return tx.
				Select("users.user_id, users.name AS user_name, events.event_id, events.name AS event_name").
				Order("users.user_id ASC")
		},
		time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		log.Fatalf("Mixed join paginate error: %v", err)
	}

	fmt.Printf("Page=%d PageSize=%d Total=%d TotalPages=%d\n", paginator.Page, paginator.PageSize, paginator.Total, paginator.TotalPages)
	fmt.Println("Current page rows:")
	for _, row := range rows {
		fmt.Printf("  user_id=%d user_name=%s event_id=%d event_name=%s\n", row.UserID, row.UserName, row.EventID, row.EventName)
	}

	fmt.Println("说明：")
	fmt.Println("1. users 使用 Hash 分表")
	fmt.Println("2. events 使用 Time 分表")
	fmt.Println("3. 传入时间范围时，只会裁剪 Time 分表这一侧")
	fmt.Println("4. queryBuilder 中使用基础表名别名 users / events，而不是实际分表名")
}

func upsertUser(db *gorm.DB, strategy *sharding.HashShardingStrategy, user models.UserBasic) error {
	if err := sharding.EnsureTableExists(db, strategy, user.UserID, &models.UserBasic{}); err != nil {
		return err
	}

	tableName := strategy.GetTableName("users", user.UserID)
	if err := db.Table(tableName).Where("user_id = ?", user.UserID).Delete(&models.UserBasic{}).Error; err != nil {
		return err
	}
	return db.Table(tableName).Create(&user).Error
}

func upsertEvent(db *gorm.DB, strategy *sharding.TimeShardingStrategy, event models.Event) error {
	if err := sharding.EnsureTableExists(db, strategy, event.CreatedAt, &models.Event{}); err != nil {
		return err
	}

	tableName := strategy.GetTableName("events", event.CreatedAt)
	if err := db.Table(tableName).Where("event_id = ?", event.EventID).Delete(&models.Event{}).Error; err != nil {
		return err
	}
	return db.Table(tableName).Create(&event).Error
}
