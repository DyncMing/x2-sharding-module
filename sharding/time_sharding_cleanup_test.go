package sharding

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTimeShardingStrategy_GetTableTimeRangeAndIsTableExpired(t *testing.T) {
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	startTime, endTime, err := strategy.GetTableTimeRange("logs_202602")
	if err != nil {
		t.Fatalf("GetTableTimeRange returned error: %v", err)
	}
	if startTime.Year() != 2026 || startTime.Month() != time.February || startTime.Day() != 1 {
		t.Fatalf("unexpected start time: %v", startTime)
	}
	if endTime.Year() != 2026 || endTime.Month() != time.March || endTime.Day() != 1 {
		t.Fatalf("unexpected end time: %v", endTime)
	}

	expired, err := strategy.IsTableExpired("logs_202602", time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("IsTableExpired returned error: %v", err)
	}
	if !expired {
		t.Fatalf("expected logs_202602 to be expired at the exact boundary")
	}

	expired, err = strategy.IsTableExpired("logs_202602", time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC))
	if err != nil {
		t.Fatalf("IsTableExpired returned error: %v", err)
	}
	if expired {
		t.Fatalf("expected logs_202602 to remain active before its end boundary")
	}

	if _, _, err := strategy.GetTableTimeRange("orders_202602"); err == nil {
		t.Fatalf("expected non-matching base table name to return an error")
	}
}

func TestCleanupExpiredTimeTables_DryRun(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	createCleanupTestTables(t, db,
		"logs_202512",
		"logs_202601",
		"logs_202602",
		"logs_202603",
		"logs_invalid",
		"other_202601",
	)

	result, err := cleanupExpiredTimeTables(db, strategy, cleanupExpiredTimeTablesOptions{
		ExpireBefore: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		DryRun:       true,
	})
	if err != nil {
		t.Fatalf("cleanupExpiredTimeTables returned error: %v", err)
	}

	assertSortedStringsEqual(t, result.MatchedTables, []string{"logs_202512", "logs_202601", "logs_202602", "logs_202603"})
	assertSortedStringsEqual(t, result.SkippedTables, []string{"logs_invalid"})
	assertSortedStringsEqual(t, result.ExpiredTables, []string{"logs_202512", "logs_202601", "logs_202602"})
	assertSortedStringsEqual(t, result.DroppedTables, nil)

	for _, tableName := range []string{"logs_202512", "logs_202601", "logs_202602", "logs_202603", "logs_invalid", "other_202601"} {
		if !db.Migrator().HasTable(tableName) {
			t.Fatalf("expected table %s to remain after dry-run cleanup", tableName)
		}
	}
}

func TestCleanupExpiredTimeTables_DropTables(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	createCleanupTestTables(t, db,
		"logs_202511",
		"logs_202512",
		"logs_202601",
		"logs_202603",
		"logs_invalid",
		"metrics_202601",
	)

	result, err := cleanupExpiredTimeTables(db, strategy, cleanupExpiredTimeTablesOptions{
		ExpireBefore: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("cleanupExpiredTimeTables returned error: %v", err)
	}

	assertSortedStringsEqual(t, result.ExpiredTables, []string{"logs_202511", "logs_202512", "logs_202601"})
	assertSortedStringsEqual(t, result.DroppedTables, []string{"logs_202511", "logs_202512", "logs_202601"})

	for _, tableName := range []string{"logs_202511", "logs_202512", "logs_202601"} {
		if db.Migrator().HasTable(tableName) {
			t.Fatalf("expected expired table %s to be dropped", tableName)
		}
	}
	for _, tableName := range []string{"logs_202603", "logs_invalid", "metrics_202601"} {
		if !db.Migrator().HasTable(tableName) {
			t.Fatalf("expected non-expired/unrelated table %s to remain", tableName)
		}
	}
}

func TestCleanupTimeTablesRetainingRecent_Monthly(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	createCleanupTestTables(t, db,
		"logs_202511",
		"logs_202512",
		"logs_202601",
		"logs_202602",
		"logs_202603",
		"logs_202604",
		"logs_invalid",
	)

	result, err := CleanupTimeTablesRetainingRecent(db, strategy, CleanupRetainRecentTimeTablesOptions{
		RetainCount:   3,
		ReferenceTime: time.Date(2026, 3, 27, 12, 30, 0, 0, time.UTC),
		DryRun:        true,
	})
	if err != nil {
		t.Fatalf("CleanupTimeTablesRetainingRecent returned error: %v", err)
	}

	assertSortedStringsEqual(t, result.ExpiredTables, []string{"logs_202511", "logs_202512"})
	assertSortedStringsEqual(t, result.DroppedTables, nil)
	assertSortedStringsEqual(t, result.SkippedTables, []string{"logs_invalid"})
}

func TestCleanupTimeTablesRetainingRecent_DaySharding(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByDay)

	createCleanupTestTables(t, db,
		"logs_20260107",
		"logs_20260108",
		"logs_20260109",
		"logs_20260110",
		"logs_20260111",
	)

	result, err := CleanupTimeTablesRetainingRecent(db, strategy, CleanupRetainRecentTimeTablesOptions{
		RetainCount:   2,
		ReferenceTime: time.Date(2026, 1, 10, 8, 45, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("CleanupTimeTablesRetainingRecent returned error: %v", err)
	}

	assertSortedStringsEqual(t, result.ExpiredTables, []string{"logs_20260107", "logs_20260108"})
	assertSortedStringsEqual(t, result.DroppedTables, []string{"logs_20260107", "logs_20260108"})

	for _, tableName := range []string{"logs_20260109", "logs_20260110", "logs_20260111"} {
		if !db.Migrator().HasTable(tableName) {
			t.Fatalf("expected retained/future table %s to remain", tableName)
		}
	}
}

func TestCleanupTimeTablesRetainingRecent_InvalidRetainCount(t *testing.T) {
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)

	_, err := CleanupTimeTablesRetainingRecent(openTestDB(t), strategy, CleanupRetainRecentTimeTablesOptions{
		RetainCount: 0,
	})
	if err == nil {
		t.Fatalf("expected invalid retain count to return an error")
	}
}

func TestCleanupTimeTablesRetainingRecent_YearSharding(t *testing.T) {
	db := openTestDB(t)
	strategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByYear)

	createCleanupTestTables(t, db,
		"logs_2022",
		"logs_2023",
		"logs_2024",
		"logs_2025",
	)

	result, err := CleanupTimeTablesRetainingRecent(db, strategy, CleanupRetainRecentTimeTablesOptions{
		RetainCount:   2,
		ReferenceTime: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		DryRun:        true,
	})
	if err != nil {
		t.Fatalf("CleanupTimeTablesRetainingRecent returned error: %v", err)
	}

	assertSortedStringsEqual(t, result.ExpiredTables, []string{"logs_2022"})
}

func createCleanupTestTables(t *testing.T, db *gorm.DB, tableNames ...string) {
	t.Helper()
	for _, tableName := range tableNames {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT)", tableName))
	}
}

func assertSortedStringsEqual(t *testing.T, got, want []string) {
	t.Helper()
	gotCopy := append([]string(nil), got...)
	wantCopy := append([]string(nil), want...)
	sort.Strings(gotCopy)
	sort.Strings(wantCopy)
	if !reflect.DeepEqual(gotCopy, wantCopy) {
		t.Fatalf("unexpected string slice\nwant: %v\ngot:  %v", wantCopy, gotCopy)
	}
}
