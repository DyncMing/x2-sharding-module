package sharding

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

type schemaSyncHashUserV1 struct {
	ID     uint   `gorm:"primarykey;column:id"`
	UserID int64  `gorm:"column:user_id;not null"`
	Name   string `gorm:"column:name"`
}

type schemaSyncHashUserV2 struct {
	ID     uint   `gorm:"primarykey;column:id"`
	UserID int64  `gorm:"column:user_id;not null"`
	Name   string `gorm:"column:name"`
	Email  string `gorm:"column:email"`
}

type schemaSyncTimeLogV1 struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
}

type schemaSyncTimeLogV2 struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
	Level     string    `gorm:"column:level"`
}

func TestAutoMigrateExistingTables_TimeShardingAddsNewColumn(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	for _, tableName := range []string{"logs_202601", "logs_202602"} {
		if err := db.Table(tableName).AutoMigrate(&schemaSyncTimeLogV1{}); err != nil {
			t.Fatalf("failed to prepare %s: %v", tableName, err)
		}
		if db.Migrator().HasColumn(tableName, "level") {
			t.Fatalf("did not expect column level to exist in %s before migration", tableName)
		}
	}

	if err := AutoMigrateExistingTables(db, strategy, &schemaSyncTimeLogV2{}); err != nil {
		t.Fatalf("AutoMigrateExistingTables returned error: %v", err)
	}

	for _, tableName := range []string{"logs_202601", "logs_202602"} {
		if !db.Migrator().HasColumn(tableName, "level") {
			t.Fatalf("expected column level to be added to %s", tableName)
		}
	}
}

func TestRegisterShardingWithAutoCreate_AutoUpdatesExistingShardSchema(t *testing.T) {
	db := openTestDB(t)
	strategy := NewHashShardingStrategy("users", "UserID", 4)
	userID := int64(123)
	tableName := strategy.GetTableName("users", userID)

	if err := db.Table(tableName).AutoMigrate(&schemaSyncHashUserV1{}); err != nil {
		t.Fatalf("failed to prepare existing shard table: %v", err)
	}
	if db.Migrator().HasColumn(tableName, "email") {
		t.Fatalf("did not expect column email before schema sync")
	}

	if err := RegisterShardingWithAutoCreate(db, strategy, &schemaSyncHashUserV2{}); err != nil {
		t.Fatalf("RegisterShardingWithAutoCreate returned error: %v", err)
	}

	user := &schemaSyncHashUserV2{UserID: userID, Name: "alice", Email: "alice@example.com"}
	if err := db.Create(user).Error; err != nil {
		t.Fatalf("db.Create returned error: %v", err)
	}

	if !db.Migrator().HasColumn(tableName, "email") {
		t.Fatalf("expected column email to be auto-migrated for %s", tableName)
	}

	var count int64
	if err := db.Table(tableName).Where("email = ?", user.Email).Count(&count).Error; err != nil {
		t.Fatalf("failed to query inserted row from %s: %v", tableName, err)
	}
	if count != 1 {
		t.Fatalf("expected 1 inserted row in %s, got %d", tableName, count)
	}
}

func TestRegisterShardingWithConfigFile_AutoUpdateSchema(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	targetTime := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	tableName := strategy.GetTableName("logs", targetTime)

	if err := db.Table(tableName).AutoMigrate(&schemaSyncTimeLogV1{}); err != nil {
		t.Fatalf("failed to prepare existing shard table: %v", err)
	}
	if db.Migrator().HasColumn(tableName, "level") {
		t.Fatalf("did not expect column level before schema sync")
	}

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "schema-sync-config.json")
	configContent := `{
		"autoCreateTable": true,
		"autoUpdateSchema": true
	}`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	if err := RegisterShardingWithConfigFile(db, strategy, &schemaSyncTimeLogV2{}, configPath); err != nil {
		t.Fatalf("RegisterShardingWithConfigFile returned error: %v", err)
	}

	entry := &schemaSyncTimeLogV2{CreatedAt: targetTime, Message: "hello", Level: "INFO"}
	if err := db.Create(entry).Error; err != nil {
		t.Fatalf("db.Create returned error: %v", err)
	}

	if !db.Migrator().HasColumn(tableName, "level") {
		t.Fatalf("expected column level to be auto-migrated for %s", tableName)
	}
}

func TestEnsureTableSchema_AutoMigratesExistingShard(t *testing.T) {
	db := openTestDB(t)
	strategy := NewHashShardingStrategy("users", "UserID", 4)
	userID := int64(777)
	tableName := strategy.GetTableName("users", userID)

	if err := db.Table(tableName).AutoMigrate(&schemaSyncHashUserV1{}); err != nil {
		t.Fatalf("failed to prepare existing shard table: %v", err)
	}

	if err := EnsureTableSchema(db, strategy, userID, &schemaSyncHashUserV2{}); err != nil {
		t.Fatalf("EnsureTableSchema returned error: %v", err)
	}

	if !db.Migrator().HasColumn(tableName, "email") {
		t.Fatalf("expected column email to be added to %s", tableName)
	}
}
