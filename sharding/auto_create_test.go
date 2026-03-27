package sharding

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

type autoCreateTimeLog struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
}

type autoCreateHashUser struct {
	ID     uint   `gorm:"primarykey;column:id"`
	UserID int64  `gorm:"column:user_id;not null"`
	Name   string `gorm:"column:name"`
}

type autoCreateMetric struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
}

type autoCreateTrace struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
}

type autoCreateAudit struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	Message   string    `gorm:"column:message"`
}

func TestRegisterShardingWithAutoCreate_TimeSharding_CreateCreatesTable(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	if err := RegisterShardingWithAutoCreate(db, strategy, &autoCreateTimeLog{}); err != nil {
		t.Fatalf("RegisterShardingWithAutoCreate returned error: %v", err)
	}

	entry := &autoCreateTimeLog{
		CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC),
		Message:   "hello",
	}
	if err := db.Create(entry).Error; err != nil {
		t.Fatalf("db.Create returned error: %v", err)
	}

	tableName := strategy.GetTableName("logs", entry.CreatedAt)
	if !db.Migrator().HasTable(tableName) {
		t.Fatalf("expected sharded table %s to be auto-created", tableName)
	}

	var count int64
	if err := db.Table(tableName).Where("message = ?", "hello").Count(&count).Error; err != nil {
		t.Fatalf("failed to query inserted row from %s: %v", tableName, err)
	}
	if count != 1 {
		t.Fatalf("expected 1 inserted row in %s, got %d", tableName, count)
	}
}

func TestRegisterShardingWithAutoCreate_TimeSharding_TableCallUsesShardedTable(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	if err := RegisterShardingWithAutoCreate(db, strategy, &autoCreateTimeLog{}); err != nil {
		t.Fatalf("RegisterShardingWithAutoCreate returned error: %v", err)
	}

	entry := &autoCreateTimeLog{
		CreatedAt: time.Date(2026, 4, 2, 8, 30, 0, 0, time.UTC),
		Message:   "from-table-call",
	}
	if err := db.Table("logs").Create(entry).Error; err != nil {
		t.Fatalf("db.Table(base).Create returned error: %v", err)
	}

	baseTable := "logs"
	shardedTable := strategy.GetTableName("logs", entry.CreatedAt)
	if db.Migrator().HasTable(baseTable) {
		t.Fatalf("did not expect base table %s to be created", baseTable)
	}
	if !db.Migrator().HasTable(shardedTable) {
		t.Fatalf("expected sharded table %s to be auto-created", shardedTable)
	}

	var count int64
	if err := db.Table(shardedTable).Where("message = ?", entry.Message).Count(&count).Error; err != nil {
		t.Fatalf("failed to query sharded table %s: %v", shardedTable, err)
	}
	if count != 1 {
		t.Fatalf("expected 1 inserted row in %s, got %d", shardedTable, count)
	}
}

func TestRegisterShardingWithAutoCreate_HashSharding_ModelMismatchStillWorks(t *testing.T) {
	db := openTestDB(t)
	strategy := NewHashShardingStrategy("users", "UserID", 4)
	if err := RegisterShardingWithAutoCreate(db, strategy, &autoCreateHashUser{}); err != nil {
		t.Fatalf("RegisterShardingWithAutoCreate returned error: %v", err)
	}

	user := &autoCreateHashUser{UserID: 123, Name: "alice"}
	if err := db.Create(user).Error; err != nil {
		t.Fatalf("db.Create returned error: %v", err)
	}

	tableName := strategy.GetTableName("users", user.UserID)
	if !db.Migrator().HasTable(tableName) {
		t.Fatalf("expected hash table %s to be auto-created", tableName)
	}

	var count int64
	if err := db.Table(tableName).Where("name = ?", user.Name).Count(&count).Error; err != nil {
		t.Fatalf("failed to query inserted row from %s: %v", tableName, err)
	}
	if count != 1 {
		t.Fatalf("expected 1 inserted row in %s, got %d", tableName, count)
	}
}

func TestCreateShardedWithAutoCreate_TimeSharding(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	entry := &autoCreateTimeLog{
		CreatedAt: time.Date(2026, 5, 9, 9, 15, 0, 0, time.UTC),
		Message:   "explicit-create",
	}
	if err := CreateShardedWithAutoCreate(db, strategy, entry, &autoCreateTimeLog{}); err != nil {
		t.Fatalf("CreateShardedWithAutoCreate returned error: %v", err)
	}

	tableName := strategy.GetTableName("logs", entry.CreatedAt)
	if !db.Migrator().HasTable(tableName) {
		t.Fatalf("expected sharded table %s to be auto-created", tableName)
	}

	var count int64
	if err := db.Table(tableName).Where("message = ?", entry.Message).Count(&count).Error; err != nil {
		t.Fatalf("failed to query inserted row from %s: %v", tableName, err)
	}
	if count != 1 {
		t.Fatalf("expected 1 inserted row in %s, got %d", tableName, count)
	}
}

func TestRegisterShardingWithOptions_TimeShardingAutoCleanup(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	createCleanupTestTables(t, db, "logs_202601", "logs_202602")

	if err := RegisterShardingWithOptions(db, strategy, RegisterShardingOptions{
		Model:           &autoCreateTimeLog{},
		AutoCreateTable: true,
		TimeSharding: &TimeShardingRegisterOptions{
			AutoCleanup: &TimeShardingAutoCleanupOptions{
				Enabled:     true,
				RetainCount: 1,
			},
		},
	}); err != nil {
		t.Fatalf("RegisterShardingWithOptions returned error: %v", err)
	}

	entry := &autoCreateTimeLog{
		CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC),
		Message:   "trigger-cleanup",
	}
	if err := db.Create(entry).Error; err != nil {
		t.Fatalf("db.Create returned error: %v", err)
	}

	if db.Migrator().HasTable("logs_202601") || db.Migrator().HasTable("logs_202602") {
		t.Fatalf("expected old time-sharding tables to be auto-cleaned")
	}
	if !db.Migrator().HasTable("logs_202603") {
		t.Fatalf("expected current shard table logs_202603 to exist")
	}
}

func TestRegisterShardingWithOptions_TimeShardingAutoCleanupHonorsMinInterval(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	if err := RegisterShardingWithOptions(db, strategy, RegisterShardingOptions{
		Model:           &autoCreateTimeLog{},
		AutoCreateTable: true,
		TimeSharding: &TimeShardingRegisterOptions{
			AutoCleanup: &TimeShardingAutoCleanupOptions{
				Enabled:     true,
				RetainCount: 1,
				MinInterval: time.Hour,
			},
		},
	}); err != nil {
		t.Fatalf("RegisterShardingWithOptions returned error: %v", err)
	}

	first := &autoCreateTimeLog{CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC), Message: "first"}
	if err := db.Create(first).Error; err != nil {
		t.Fatalf("first create returned error: %v", err)
	}

	createCleanupTestTables(t, db, "logs_202602")
	second := &autoCreateTimeLog{CreatedAt: time.Date(2026, 3, 27, 11, 0, 0, 0, time.UTC), Message: "second"}
	if err := db.Create(second).Error; err != nil {
		t.Fatalf("second create returned error: %v", err)
	}

	if !db.Migrator().HasTable("logs_202602") {
		t.Fatalf("expected old table logs_202602 to remain because auto-cleanup was throttled")
	}
}

func TestRegisterShardingWithConfigFile_TimeShardingAutoCleanup(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	createCleanupTestTables(t, db, "logs_202601", "logs_202602")

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "sharding-config.json")
	configContent := `{
		"autoCreateTable": true,
		"timeSharding": {
			"autoCleanup": {
				"enabled": true,
				"retainCount": 1,
				"minInterval": "0s"
			}
		}
	}`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	if err := RegisterShardingWithConfigFile(db, strategy, &autoCreateTimeLog{}, configPath); err != nil {
		t.Fatalf("RegisterShardingWithConfigFile returned error: %v", err)
	}

	entry := &autoCreateTimeLog{CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC), Message: "from-config-file"}
	if err := db.Create(entry).Error; err != nil {
		t.Fatalf("db.Create returned error: %v", err)
	}

	if db.Migrator().HasTable("logs_202601") || db.Migrator().HasTable("logs_202602") {
		t.Fatalf("expected old tables to be auto-cleaned when config file enables it")
	}
}

func TestRegisterShardingWithConfigFile_MultiTimeStrategiesAutoCleanupPolicies(t *testing.T) {
	db := openTestDB(t)
	monthStrategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	dayStrategy := NewTimeShardingStrategy("metrics", "CreatedAt", TimeShardingByDay)
	hourStrategy := NewTimeShardingStrategy("traces", "CreatedAt", TimeShardingByHour)
	yearStrategy := NewTimeShardingStrategy("audits", "CreatedAt", TimeShardingByYear)

	createCleanupTestTables(t, db,
		"logs_202601", "logs_202602",
		"metrics_20260107", "metrics_20260108",
		"traces_2026032708", "traces_2026032709",
		"audits_2024", "audits_2025",
	)

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "multi-sharding-config.json")
	configContent := `{
		"autoCreateTable": true,
		"timeSharding": {
			"autoCleanupPolicies": {
				"default": {
					"enabled": true,
					"retainCount": 1,
					"minInterval": "0s"
				},
				"byUnit": {
					"day": {
						"enabled": true,
						"retainCount": 2,
						"minInterval": "0s"
					},
					"hour": {
						"enabled": true,
						"retainCount": 2,
						"minInterval": "0s"
					}
				},
				"byBaseTable": {
					"logs": {
						"enabled": true,
						"retainCount": 1,
						"minInterval": "0s"
					}
				}
			}
		}
	}`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	for _, item := range []struct {
		strategy *TimeShardingStrategy
		model    interface{}
	}{
		{monthStrategy, &autoCreateTimeLog{}},
		{dayStrategy, &autoCreateMetric{}},
		{hourStrategy, &autoCreateTrace{}},
		{yearStrategy, &autoCreateAudit{}},
	} {
		if err := RegisterShardingWithConfigFile(db, item.strategy, item.model, configPath); err != nil {
			t.Fatalf("RegisterShardingWithConfigFile returned error for %s: %v", item.strategy.GetBaseTableName(), err)
		}
	}

	if err := db.Create(&autoCreateTimeLog{CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC), Message: "logs-entry"}).Error; err != nil {
		t.Fatalf("db.Create returned error for logs-entry: %v", err)
	}
	if err := db.Create(&autoCreateMetric{CreatedAt: time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC), Message: "metrics-entry"}).Error; err != nil {
		t.Fatalf("db.Create returned error for metrics-entry: %v", err)
	}
	if err := db.Create(&autoCreateTrace{CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC), Message: "traces-entry"}).Error; err != nil {
		t.Fatalf("db.Create returned error for traces-entry: %v", err)
	}
	if err := db.Create(&autoCreateAudit{CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Message: "audits-entry"}).Error; err != nil {
		t.Fatalf("db.Create returned error for audits-entry: %v", err)
	}

	if db.Migrator().HasTable("logs_202601") || db.Migrator().HasTable("logs_202602") {
		t.Fatalf("expected month-strategy old tables to be cleaned by base-table policy")
	}
	if db.Migrator().HasTable("metrics_20260107") || db.Migrator().HasTable("metrics_20260108") {
		t.Fatalf("expected day-strategy old tables to be cleaned according to byUnit policy")
	}
	if db.Migrator().HasTable("audits_2024") || db.Migrator().HasTable("audits_2025") {
		t.Fatalf("expected year-strategy old tables to be cleaned by default policy")
	}
	if db.Migrator().HasTable("traces_2026032708") {
		t.Fatalf("expected oldest hour-strategy table to be cleaned by hour unit policy")
	}
	if !db.Migrator().HasTable("traces_2026032709") {
		t.Fatalf("expected more recent hour-strategy table to be retained")
	}
}

func TestLoadRegisterShardingOptionsForStrategyFromJSON_InvalidUnitPolicy(t *testing.T) {
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "invalid-unit-config.json")
	configContent := `{
		"timeSharding": {
			"autoCleanupPolicies": {
				"byUnit": {
					"weekly": {
						"enabled": true,
						"retainCount": 2
					}
				}
			}
		}
	}`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	if _, err := LoadRegisterShardingOptionsForStrategyFromJSON(configPath, strategy, &autoCreateTimeLog{}); err == nil {
		t.Fatalf("expected invalid byUnit policy to return an error")
	}
}

func TestRegisterShardingsWithConfigFile_BatchRegistration(t *testing.T) {
	db := openTestDB(t)
	monthStrategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	dayStrategy := NewTimeShardingStrategy("metrics", "CreatedAt", TimeShardingByDay)
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "batch-config.json")
	configContent := `{
		"autoCreateTable": true,
		"timeSharding": {
			"autoCleanupPolicies": {
				"byBaseTable": {
					"logs": {"enabled": true, "retainCount": 1, "minInterval": "0s"},
					"metrics": {"enabled": true, "retainCount": 2, "minInterval": "0s"}
				}
			}
		}
	}`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	createCleanupTestTables(t, db, "logs_202601", "metrics_20260107", "metrics_20260109")

	if err := RegisterShardingsWithConfigFile(db, configPath, []ConfigFileShardingRegistration{
		{Strategy: monthStrategy, Model: &autoCreateTimeLog{}},
		{Strategy: dayStrategy, Model: &autoCreateMetric{}},
	}); err != nil {
		t.Fatalf("RegisterShardingsWithConfigFile returned error: %v", err)
	}

	if err := db.Create(&autoCreateTimeLog{CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC), Message: "logs-batch"}).Error; err != nil {
		t.Fatalf("db.Create returned error for logs batch item: %v", err)
	}
	if err := db.Create(&autoCreateMetric{CreatedAt: time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC), Message: "metrics-batch"}).Error; err != nil {
		t.Fatalf("db.Create returned error for metrics batch item: %v", err)
	}

	if db.Migrator().HasTable("logs_202601") {
		t.Fatalf("expected logs old shard to be cleaned after batch registration")
	}
	if db.Migrator().HasTable("metrics_20260107") {
		t.Fatalf("expected metrics oldest shard to be cleaned because retainCount=2 for day strategy")
	}
	if !db.Migrator().HasTable("metrics_20260109") {
		t.Fatalf("expected more recent metrics shard to remain because retainCount=2 for day strategy")
	}
}

func TestShardingHelper_RegisterStrategiesWithConfigFile(t *testing.T) {
	db := openTestDB(t)
	helper := NewShardingHelper(db)
	monthStrategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	yearStrategy := NewTimeShardingStrategy("audits", "CreatedAt", TimeShardingByYear)
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "helper-batch-config.json")
	configContent := `{
		"autoCreateTable": true,
		"timeSharding": {
			"autoCleanupPolicies": {
				"default": {"enabled": true, "retainCount": 1, "minInterval": "0s"}
			}
		}
	}`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	createCleanupTestTables(t, db, "logs_202601", "audits_2025")

	if err := helper.RegisterStrategiesWithConfigFile(configPath, []ConfigFileShardingRegistration{
		{Strategy: monthStrategy, Model: &autoCreateTimeLog{}},
		{Strategy: yearStrategy, Model: &autoCreateAudit{}},
	}); err != nil {
		t.Fatalf("RegisterStrategiesWithConfigFile returned error: %v", err)
	}

	if err := db.Create(&autoCreateTimeLog{CreatedAt: time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC), Message: "logs-helper"}).Error; err != nil {
		t.Fatalf("db.Create returned error for helper logs item: %v", err)
	}
	if err := db.Create(&autoCreateAudit{CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Message: "audit-helper"}).Error; err != nil {
		t.Fatalf("db.Create returned error for helper audit item: %v", err)
	}

	if db.Migrator().HasTable("logs_202601") || db.Migrator().HasTable("audits_2025") {
		t.Fatalf("expected helper batch registration to apply default auto-cleanup policy")
	}

	if _, ok := helper.GetStrategy("logs"); !ok {
		t.Fatalf("expected helper to cache logs strategy")
	}
	if _, ok := helper.GetStrategy("audits"); !ok {
		t.Fatalf("expected helper to cache audits strategy")
	}
}

func TestRegisterShardingsWithConfigFile_RejectsDuplicateBaseTable(t *testing.T) {
	db := openTestDB(t)
	strategy1 := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	strategy2 := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByDay)
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "dup-config.json")
	if err := os.WriteFile(configPath, []byte(`{"autoCreateTable":true}`), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	err := RegisterShardingsWithConfigFile(db, configPath, []ConfigFileShardingRegistration{
		{Strategy: strategy1, Model: &autoCreateTimeLog{}},
		{Strategy: strategy2, Model: &autoCreateTimeLog{}},
	})
	if err == nil {
		t.Fatalf("expected duplicate base table batch registration to fail")
	}
}
