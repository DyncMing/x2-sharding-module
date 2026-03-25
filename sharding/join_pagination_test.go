package sharding

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type joinPaginationRow struct {
	UserID    int64  `gorm:"column:user_id"`
	UserName  string `gorm:"column:user_name"`
	OrderID   int64  `gorm:"column:order_id"`
	LogID     int64  `gorm:"column:log_id"`
	EventID   int64  `gorm:"column:event_id"`
	EventName string `gorm:"column:event_name"`
}

func TestCrossTableJoinPaginate_HashSharding(t *testing.T) {
	db := openTestDB(t)
	userStrategy := NewHashShardingStrategy("users", "UserID", 2)
	orderStrategy := NewHashShardingStrategy("orders", "UserID", 2)

	createHashJoinTables(t, db, userStrategy, orderStrategy)

	userIDs := collectValuesForTable(userStrategy, "users", "users_0", 3)
	for idx, userID := range userIDs {
		userTable := userStrategy.GetTableName("users", userID)
		orderTable := orderStrategy.GetTableName("orders", userID)

		mustExec(t, db, fmt.Sprintf("INSERT INTO %s (user_id, name) VALUES (?, ?)", userTable), userID, fmt.Sprintf("user-%d", idx+1))
		mustExec(t, db, fmt.Sprintf("INSERT INTO %s (user_id, order_id, amount) VALUES (?, ?, ?)", orderTable), userID, int64(1000+idx), float64(10*(idx+1)))
	}

	var results []joinPaginationRow
	paginator, err := CrossTableJoinPaginate(
		db,
		userStrategy,
		orderStrategy,
		LeftJoin,
		"users.user_id = orders.user_id",
		&results,
		1,
		2,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("users.user_id, users.name as user_name, orders.order_id").Order("users.user_id ASC")
		},
	)
	if err != nil {
		t.Fatalf("CrossTableJoinPaginate returned error: %v", err)
	}

	if paginator.Total != 3 {
		t.Fatalf("expected total=3, got %d", paginator.Total)
	}
	if paginator.TotalPages != 2 {
		t.Fatalf("expected totalPages=2, got %d", paginator.TotalPages)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 paginated rows, got %d", len(results))
	}
	if results[0].UserID != userIDs[0] || results[1].UserID != userIDs[1] {
		t.Fatalf("unexpected page data: %+v", results)
	}
	if paginator.Data != &results {
		t.Fatalf("expected paginator data to reference result slice pointer")
	}
}

func TestCrossTableJoinPaginateWithTimeRange_TimeSharding(t *testing.T) {
	db := openTestDB(t)
	logStrategy := NewTimeShardingStrategy("logs", "CreatedAt", TimeShardingByMonth)
	eventStrategy := NewTimeShardingStrategy("events", "CreatedAt", TimeShardingByMonth)

	jan := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	mar := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)

	for _, ts := range []time.Time{jan, feb, mar} {
		logTable := logStrategy.GetTableName("logs", ts)
		eventTable := eventStrategy.GetTableName("events", ts)
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, log_id INTEGER, created_at DATETIME)", logTable))
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER, log_id INTEGER, created_at DATETIME, name TEXT)", eventTable))
	}

	insertTimeJoinData(t, db, logStrategy, eventStrategy, jan, 101, 1001, "jan-event")
	insertTimeJoinData(t, db, logStrategy, eventStrategy, feb, 102, 1002, "feb-event")
	insertTimeJoinData(t, db, logStrategy, eventStrategy, mar, 103, 1003, "mar-event")

	var results []joinPaginationRow
	paginator, err := CrossTableJoinPaginateWithTimeRange(
		db,
		logStrategy,
		eventStrategy,
		LeftJoin,
		"logs.log_id = events.log_id",
		&results,
		1,
		10,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("logs.log_id, events.event_id, events.name as event_name")
		},
		time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("CrossTableJoinPaginateWithTimeRange returned error: %v", err)
	}

	if paginator.Total != 2 {
		t.Fatalf("expected total=2 within range, got %d", paginator.Total)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows in range, got %d", len(results))
	}
	for _, row := range results {
		if row.LogID == 103 || row.EventID == 1003 {
			t.Fatalf("unexpected out-of-range row returned: %+v", row)
		}
	}
}

func TestCrossTableJoinPaginateWithTimeRange_MixedHashAndTimeSharding(t *testing.T) {
	db := openTestDB(t)
	userStrategy := NewHashShardingStrategy("users", "UserID", 2)
	eventStrategy := NewTimeShardingStrategy("events", "CreatedAt", TimeShardingByMonth)

	createMixedJoinTables(t, db, userStrategy, eventStrategy, []time.Time{
		time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
	})

	userIDs := collectValuesForTable(userStrategy, "users", "users_0", 3)
	for idx, userID := range userIDs {
		userTable := userStrategy.GetTableName("users", userID)
		mustExec(t, db, fmt.Sprintf("INSERT INTO %s (user_id, name) VALUES (?, ?)", userTable), userID, fmt.Sprintf("user-%d", idx+1))
	}

	jan := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	mar := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	insertMixedTimeJoinData(t, db, eventStrategy, jan, userIDs[0], 2001, "jan-event")
	insertMixedTimeJoinData(t, db, eventStrategy, feb, userIDs[1], 2002, "feb-event")
	insertMixedTimeJoinData(t, db, eventStrategy, mar, userIDs[2], 2003, "mar-event")

	var results []joinPaginationRow
	paginator, err := CrossTableJoinPaginateWithTimeRange(
		db,
		userStrategy,
		eventStrategy,
		InnerJoin,
		"users.user_id = events.user_id",
		&results,
		1,
		10,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("users.user_id, users.name as user_name, events.event_id, events.name as event_name").Order("users.user_id ASC")
		},
		time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("CrossTableJoinPaginateWithTimeRange mixed strategies returned error: %v", err)
	}

	if paginator.Total != 2 {
		t.Fatalf("expected total=2 within mixed-strategy range, got %d", paginator.Total)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 mixed-strategy rows, got %d", len(results))
	}
	for _, row := range results {
		if row.EventID == 2003 {
			t.Fatalf("unexpected out-of-range mixed-strategy row returned: %+v", row)
		}
	}
}

func TestGetTableNameByKey_UsesMatchingStrategyKey(t *testing.T) {
	userStrategy := NewHashShardingStrategy("users", "UserID", 4)
	paymentStrategy := NewHashShardingStrategy("payments", "OrderID", 4)
	joinKeys := map[string]interface{}{
		"user_id":  int64(123),
		"order_id": int64(456),
	}

	userTable := getTableNameByKey(userStrategy, "users", joinKeys)
	paymentTable := getTableNameByKey(paymentStrategy, "payments", joinKeys)

	if expected := userStrategy.GetTableName("users", int64(123)); userTable != expected {
		t.Fatalf("expected user table %s, got %s", expected, userTable)
	}
	if expected := paymentStrategy.GetTableName("payments", int64(456)); paymentTable != expected {
		t.Fatalf("expected payment table %s, got %s", expected, paymentTable)
	}
}

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	return db
}

func createHashJoinTables(t *testing.T, db *gorm.DB, userStrategy, orderStrategy *HashShardingStrategy) {
	t.Helper()
	for _, tableName := range userStrategy.GetAllTableNames("users") {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, name TEXT)", tableName))
	}
	for _, tableName := range orderStrategy.GetAllTableNames("orders") {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, order_id INTEGER, amount REAL)", tableName))
	}
}

func createMixedJoinTables(t *testing.T, db *gorm.DB, userStrategy *HashShardingStrategy, eventStrategy *TimeShardingStrategy, timestamps []time.Time) {
	t.Helper()
	for _, tableName := range userStrategy.GetAllTableNames("users") {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, name TEXT)", tableName))
	}
	for _, ts := range timestamps {
		eventTable := eventStrategy.GetTableName("events", ts)
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER, user_id INTEGER, created_at DATETIME, name TEXT)", eventTable))
	}
}

func insertTimeJoinData(t *testing.T, db *gorm.DB, logStrategy, eventStrategy *TimeShardingStrategy, ts time.Time, logID, eventID int64, name string) {
	t.Helper()
	logTable := logStrategy.GetTableName("logs", ts)
	eventTable := eventStrategy.GetTableName("events", ts)
	mustExec(t, db, fmt.Sprintf("INSERT INTO %s (log_id, created_at) VALUES (?, ?)", logTable), logID, ts)
	mustExec(t, db, fmt.Sprintf("INSERT INTO %s (event_id, log_id, created_at, name) VALUES (?, ?, ?, ?)", eventTable), eventID, logID, ts, name)
}

func insertMixedTimeJoinData(t *testing.T, db *gorm.DB, eventStrategy *TimeShardingStrategy, ts time.Time, userID, eventID int64, name string) {
	t.Helper()
	eventTable := eventStrategy.GetTableName("events", ts)
	mustExec(t, db, fmt.Sprintf("INSERT INTO %s (event_id, user_id, created_at, name) VALUES (?, ?, ?, ?)", eventTable), eventID, userID, ts, name)
}

func mustExec(t *testing.T, db *gorm.DB, sql string, args ...interface{}) {
	t.Helper()
	if err := db.Exec(sql, args...).Error; err != nil {
		t.Fatalf("exec failed for %q: %v", sql, err)
	}
}

func collectValuesForTable(strategy *HashShardingStrategy, baseTableName, expectedTable string, count int) []int64 {
	values := make([]int64, 0, count)
	for candidate := int64(1); len(values) < count; candidate++ {
		if strategy.GetTableName(baseTableName, candidate) == expectedTable {
			values = append(values, candidate)
		}
	}
	return values
}
