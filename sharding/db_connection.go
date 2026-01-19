package sharding

import (
	"fmt"
	"regexp"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// DSNInfo DSN 信息结构
type DSNInfo struct {
	Username string
	Password string
	Protocol string // tcp, unix
	Address  string // host:port 或 socket 路径
	Database string
	Params   string // 查询参数部分
}

// ParseDSN 解析 MySQL DSN 字符串
// 支持格式: 
//   - user:password@tcp(localhost:3306)/dbname?charset=utf8mb4
//   - user:password@unix(/path/to/socket)/dbname?charset=utf8mb4
//   - user:password@/dbname?charset=utf8mb4 (Unix socket)
//   - user:password@tcp(localhost:3306)/?charset=utf8mb4 (无数据库)
func ParseDSN(dsn string) (*DSNInfo, error) {
	info := &DSNInfo{
		Protocol: "tcp",
	}

	// 移除前后空白
	dsn = strings.TrimSpace(dsn)

	// 匹配格式: user:password@protocol(address)/database?params
	// 例如: root:password@tcp(localhost:3306)/testdb?charset=utf8mb4
	re := regexp.MustCompile(`^([^:]+)(?::([^@]+))?@(tcp|unix)\(([^)]+)\)/([^?]*)(?:\?(.+))?$`)
	matches := re.FindStringSubmatch(dsn)

	if len(matches) >= 5 {
		info.Username = matches[1]
		if len(matches) > 2 && matches[2] != "" {
			info.Password = matches[2]
		}
		if len(matches) > 3 && matches[3] != "" {
			info.Protocol = matches[3]
		}
		if len(matches) > 4 && matches[4] != "" {
			info.Address = matches[4]
		}
		if len(matches) > 5 {
			info.Database = matches[5]
		}
		if len(matches) > 6 && matches[6] != "" {
			info.Params = matches[6]
		}
		return info, nil
	}

	// 尝试解析格式: user:password@/database?params (Unix socket 默认)
	re2 := regexp.MustCompile(`^([^:]+)(?::([^@]+))?@/([^?]*)(?:\?(.+))?$`)
	matches2 := re2.FindStringSubmatch(dsn)
	if len(matches2) >= 3 {
		info.Username = matches2[1]
		if len(matches2) > 2 && matches2[2] != "" {
			info.Password = matches2[2]
		}
		if len(matches2) > 3 {
			info.Database = matches2[3]
		}
		if len(matches2) > 4 && matches2[4] != "" {
			info.Params = matches2[4]
		}
		info.Protocol = "unix"
		return info, nil
	}

	return nil, fmt.Errorf("invalid DSN format: %s", dsn)
}

// BuildDSN 构建 DSN 字符串
func (d *DSNInfo) BuildDSN() string {
	var dsn strings.Builder
	
	dsn.WriteString(d.Username)
	if d.Password != "" {
		dsn.WriteString(":")
		dsn.WriteString(d.Password)
	}
	dsn.WriteString("@")
	
	if d.Protocol == "unix" {
		dsn.WriteString("unix(")
		if d.Address != "" {
			dsn.WriteString(d.Address)
		}
		dsn.WriteString(")")
	} else {
		dsn.WriteString("tcp(")
		if d.Address == "" {
			dsn.WriteString("localhost:3306")
		} else {
			dsn.WriteString(d.Address)
		}
		dsn.WriteString(")")
	}
	
	dsn.WriteString("/")
	if d.Database != "" {
		dsn.WriteString(d.Database)
	}
	
	if d.Params != "" {
		dsn.WriteString("?")
		dsn.WriteString(d.Params)
	}
	
	return dsn.String()
}

// BuildDSNWithoutDatabase 构建不带数据库名的 DSN（用于连接到 MySQL 服务器）
func (d *DSNInfo) BuildDSNWithoutDatabase() string {
	info := *d
	info.Database = ""
	return info.BuildDSN()
}

// OpenWithAutoCreateDB 打开数据库连接，如果数据库不存在则自动创建
// dsn: MySQL DSN 字符串
// config: GORM 配置（可为 nil）
// charset: 数据库字符集（默认为 utf8mb4）
// collation: 数据库排序规则（默认为 utf8mb4_unicode_ci）
func OpenWithAutoCreateDB(dsn string, config *gorm.Config, charset, collation string) (*gorm.DB, error) {
	// 解析 DSN
	dsnInfo, err := ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	if dsnInfo.Database == "" {
		// 如果没有指定数据库，直接连接
		return gorm.Open(mysql.Open(dsn), config)
	}

	// 先连接到 MySQL 服务器（不指定数据库）
	serverDSN := dsnInfo.BuildDSNWithoutDatabase()
	serverDB, err := gorm.Open(mysql.Open(serverDSN), config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL server: %w", err)
	}

	// 检查数据库是否存在
	var exists bool
	checkQuery := "SELECT EXISTS(SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?)"
	err = serverDB.Raw(checkQuery, dsnInfo.Database).Scan(&exists).Error
	if err != nil {
		return nil, fmt.Errorf("failed to check if database exists: %w", err)
	}

	// 如果数据库不存在，创建它
	if !exists {
		if charset == "" {
			charset = "utf8mb4"
		}
		if collation == "" {
			collation = "utf8mb4_unicode_ci"
		}

		// 使用反引号保护数据库名（防止 SQL 注入）
		dbName := quoteIdentifier(dsnInfo.Database)
		createQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET %s COLLATE %s", 
			dbName, charset, collation)

		err = serverDB.Exec(createQuery).Error
		if err != nil {
			return nil, fmt.Errorf("failed to create database: %w", err)
		}
	}

	// 关闭服务器连接
	sqlDB, err := serverDB.DB()
	if err == nil && sqlDB != nil {
		sqlDB.Close()
	}

	// 连接到指定的数据库
	db, err := gorm.Open(mysql.Open(dsn), config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return db, nil
}

// OpenMySQLWithAutoCreateDB 打开 MySQL 数据库连接，如果数据库不存在则自动创建（简化版本）
// 使用默认字符集 utf8mb4 和排序规则 utf8mb4_unicode_ci
func OpenMySQLWithAutoCreateDB(dsn string, config *gorm.Config) (*gorm.DB, error) {
	return OpenWithAutoCreateDB(dsn, config, "utf8mb4", "utf8mb4_unicode_ci")
}

// DatabaseExists 检查数据库是否存在
func DatabaseExists(db *gorm.DB, databaseName string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?)"
	err := db.Raw(query, databaseName).Scan(&exists).Error
	return exists, err
}

// CreateDatabase 创建数据库
func CreateDatabase(db *gorm.DB, databaseName string, charset, collation string) error {
	if charset == "" {
		charset = "utf8mb4"
	}
	if collation == "" {
		collation = "utf8mb4_unicode_ci"
	}

	dbName := quoteIdentifier(databaseName)
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET %s COLLATE %s",
		dbName, charset, collation)

	return db.Exec(query).Error
}

// EnsureDatabaseExists 确保数据库存在，如果不存在则创建
func EnsureDatabaseExists(db *gorm.DB, databaseName string, charset, collation string) error {
	exists, err := DatabaseExists(db, databaseName)
	if err != nil {
		return err
	}

	if !exists {
		return CreateDatabase(db, databaseName, charset, collation)
	}

	return nil
}

// quoteIdentifier 使用反引号引用标识符（防止 SQL 注入）
func quoteIdentifier(ident string) string {
	// 移除反引号，然后重新添加
	ident = strings.ReplaceAll(ident, "`", "")
	return "`" + ident + "`"
}

// ExtractDatabaseFromDSN 从 DSN 中提取数据库名
func ExtractDatabaseFromDSN(dsn string) (string, error) {
	dsnInfo, err := ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	return dsnInfo.Database, nil
}

// ReplaceDatabaseInDSN 替换 DSN 中的数据库名
func ReplaceDatabaseInDSN(dsn string, newDatabase string) (string, error) {
	dsnInfo, err := ParseDSN(dsn)
	if err != nil {
		return "", err
	}

	dsnInfo.Database = newDatabase
	return dsnInfo.BuildDSN(), nil
}

