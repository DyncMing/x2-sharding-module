package main

import (
	"fmt"
	"log"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"x2-sharding-module/examples/internal/models"
	"x2-sharding-module/sharding"
)

type legacyHashUser struct {
	ID     uint   `gorm:"primarykey;column:id"`
	UserID int64  `gorm:"column:user_id;not null;index"`
	Name   string `gorm:"column:name"`
}

func main() {
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	hashStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)

	userID1 := int64(123)
	userID2 := findUserIDInDifferentShard(hashStrategy, userID1)
	userID3 := findUserIDInDifferentShard(hashStrategy, userID2, hashStrategy.GetTableName("users", userID1))
	table1 := hashStrategy.GetTableName("users", userID1)
	table2 := hashStrategy.GetTableName("users", userID2)
	table3 := hashStrategy.GetTableName("users", userID3)

	fmt.Println("=== 示例 1: 准备旧结构 Hash 分表 ===")
	for _, tableName := range []string{table1, table2, table3} {
		if err := db.Table(tableName).AutoMigrate(&legacyHashUser{}); err != nil {
			log.Fatalf("failed to prepare legacy shard %s: %v", tableName, err)
		}
		printColumnState(db, tableName, "email", "准备旧结构后")
	}

	fmt.Println("\n=== 示例 2: 单次写入前同步目标分表结构 ===")
	user1 := &models.UserWithEmail{UserID: userID1, Name: "Alice", Email: "alice@example.com"}
	printColumnState(db, table1, "email", "写入前")
	if err := sharding.CreateShardedWithSchemaSync(db, hashStrategy, user1, &models.UserWithEmail{}); err != nil {
		log.Fatalf("CreateShardedWithSchemaSync failed: %v", err)
	}
	printColumnState(db, table1, "email", "CreateShardedWithSchemaSync 后")
	fmt.Printf("Inserted user %d into %s\n", userID1, table1)

	fmt.Println("\n=== 示例 3: 使用注册回调自动同步已存在分表结构 ===")
	if err := sharding.RegisterShardingWithAutoCreate(db, hashStrategy, &models.UserWithEmail{}); err != nil {
		log.Fatalf("RegisterShardingWithAutoCreate failed: %v", err)
	}

	user2 := &models.UserWithEmail{UserID: userID2, Name: "Bob", Email: "bob@example.com"}
	printColumnState(db, table2, "email", "db.Create 前")
	if err := db.Create(user2).Error; err != nil {
		log.Fatalf("db.Create failed: %v", err)
	}
	printColumnState(db, table2, "email", "db.Create 后")
	fmt.Printf("Inserted user %d into %s via db.Create\n", userID2, table2)

	fmt.Println("\n=== 示例 4: 批量同步数据库中已存在的 Hash 分表结构 ===")
	printColumnState(db, table3, "email", "批量同步前")
	if err := sharding.AutoMigrateExistingTables(db, hashStrategy, &models.UserWithEmail{}); err != nil {
		log.Fatalf("AutoMigrateExistingTables failed: %v", err)
	}
	printColumnState(db, table3, "email", "批量同步后")
	fmt.Printf("All existing hash-sharded tables have been schema-synced successfully! Example upgraded shard: %s\n", table3)
}

func findUserIDInDifferentShard(strategy *sharding.HashShardingStrategy, baseUserID int64, excludedTables ...string) int64 {
	baseTable := strategy.GetTableName("users", baseUserID)
	excluded := make(map[string]struct{}, len(excludedTables)+1)
	excluded[baseTable] = struct{}{}
	for _, tableName := range excludedTables {
		excluded[tableName] = struct{}{}
	}

	for candidate := baseUserID + 1; candidate < baseUserID+1024; candidate++ {
		candidateTable := strategy.GetTableName("users", candidate)
		if _, skip := excluded[candidateTable]; skip {
			continue
		}
		if candidateTable != baseTable {
			return candidate
		}
	}
	return baseUserID + 1
}

func printColumnState(db *gorm.DB, tableName, columnName, stage string) {
	state := "❌ 不存在"
	if db.Migrator().HasColumn(tableName, columnName) {
		state = "✅ 已存在"
	}
	fmt.Printf("[%s] %s.%s => %s\n", stage, tableName, columnName, state)
}
