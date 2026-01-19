package main

import (
	"fmt"
	"log"
	"strings"

	"x2-sharding-module/sharding"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Product 商品模型（使用自定义分表策略）
type Product struct {
	ID        uint   `gorm:"primarykey;column:id"`
	ProductID int64  `gorm:"column:product_id;not null"`
	Name      string `gorm:"column:name"`
	Category  string `gorm:"column:category"`
}

func main() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// 示例 1: 使用自定义分表策略（按类别分表）
	fmt.Println("=== 自定义分表策略：按类别分表 ===")

	categoryShardingFunc := func(baseTableName string, shardingValue interface{}) string {
		// 根据类别名称的第一个字母分表
		category, ok := shardingValue.(string)
		if !ok {
			return baseTableName
		}

		if len(category) == 0 {
			return baseTableName
		}

		// 取第一个字母作为分表后缀
		firstLetter := strings.ToUpper(string(category[0]))
		// A-M 在表 0，N-Z 在表 1
		if firstLetter >= "A" && firstLetter <= "M" {
			return fmt.Sprintf("%s_0", baseTableName)
		}
		return fmt.Sprintf("%s_1", baseTableName)
	}

	categoryValueFunc := func(value interface{}) (interface{}, error) {
		return sharding.ExtractValue(value, "Category")
	}

	getAllTablesFunc := func(baseTableName string) []string {
		return []string{
			fmt.Sprintf("%s_0", baseTableName),
			fmt.Sprintf("%s_1", baseTableName),
		}
	}

	categoryStrategy := sharding.NewCustomShardingStrategy(
		"products",
		"Category",
		categoryShardingFunc,
		categoryValueFunc,
		getAllTablesFunc,
	)

	sharding.RegisterSharding(db, categoryStrategy)

	// 插入数据
	product1 := &Product{ProductID: 1, Name: "Apple iPhone", Category: "Electronics"}
	tableName := categoryStrategy.GetTableName("products", "Electronics")
	fmt.Printf("Product with category 'Electronics' will be in table: %s\n", tableName)

	db.Table(tableName).Create(product1)

	product2 := &Product{ProductID: 2, Name: "Nike Shoes", Category: "Sports"}
	tableName = categoryStrategy.GetTableName("products", "Sports")
	fmt.Printf("Product with category 'Sports' will be in table: %s\n", tableName)

	db.Table(tableName).Create(product2)

	// 跨表查询
	var allProducts []Product
	err = sharding.CrossTableQuery(db, categoryStrategy, &allProducts, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("name LIKE ?", "%iPhone%")
	})

	if err != nil {
		log.Printf("Cross table query error: %v\n", err)
	} else {
		fmt.Printf("Found %d products across all tables\n", len(allProducts))
	}

	// 示例 2: 使用范围分表策略
	fmt.Println("\n=== 范围分表策略 ===")

	rangeStrategy := sharding.NewRangeShardingStrategy("products", "ProductID", 10000, 10)
	// 每个分表存储 10000 个产品，共 10 张表

	productID := int64(15000)
	tableName = rangeStrategy.GetTableName("products", productID)
	fmt.Printf("Product with ID %d will be in table: %s\n", productID, tableName)

	// 获取所有表名
	allTableNames := rangeStrategy.GetAllTableNames("products")
	fmt.Printf("All range sharding tables: %v\n", allTableNames)

	// 示例 3: 使用取模分表策略
	fmt.Println("\n=== 取模分表策略 ===")

	moduloStrategy := sharding.NewModuloShardingStrategy("products", "ProductID", 4)
	// 根据 ProductID % 4 分表

	productID = int64(123)
	tableName = moduloStrategy.GetTableName("products", productID)
	fmt.Printf("Product with ID %d will be in table: %s (123 %% 4 = %d)\n",
		productID, tableName, 123%4)

	allTableNames = moduloStrategy.GetAllTableNames("products")
	fmt.Printf("All modulo sharding tables: %v\n", allTableNames)

	// 示例 4: 更复杂的自定义分表（按地区+日期）
	fmt.Println("\n=== 复杂自定义分表：按地区+日期 ===")

	regionDateShardingFunc := func(baseTableName string, shardingValue interface{}) string {
		// shardingValue 应该是 "region:date" 格式
		valueStr, ok := shardingValue.(string)
		if !ok {
			return baseTableName
		}

		parts := strings.Split(valueStr, ":")
		if len(parts) != 2 {
			return baseTableName
		}

		region := parts[0]
		dateStr := parts[1]

		// 简化处理：取日期前6位（年月）和地区代码
		datePrefix := ""
		if len(dateStr) >= 6 {
			datePrefix = dateStr[:6] // YYYYMM
		}

		// 使用地区和日期组合作为表名
		return fmt.Sprintf("%s_%s_%s", baseTableName, region, datePrefix)
	}

	regionDateValueFunc := func(value interface{}) (interface{}, error) {
		// 从结构体中提取地区和日期字段
		region, err1 := sharding.ExtractValue(value, "Region")
		date, err2 := sharding.ExtractValue(value, "CreatedAt")

		if err1 != nil || err2 != nil {
			return "", fmt.Errorf("failed to extract region or date")
		}

		// 格式化日期
		dateStr := fmt.Sprintf("%v", date)
		if len(dateStr) >= 10 {
			dateStr = strings.ReplaceAll(dateStr[:10], "-", "") // YYYY-MM-DD -> YYYYMMDD
		}

		return fmt.Sprintf("%v:%s", region, dateStr), nil
	}

	regionDateGetAllFunc := func(baseTableName string) []string {
		// 这里简化处理，实际使用时应该根据业务需求返回所有可能的表名
		return []string{
			fmt.Sprintf("%s_CN_202401", baseTableName),
			fmt.Sprintf("%s_US_202401", baseTableName),
		}
	}

	regionDateStrategy := sharding.NewCustomShardingStrategy(
		"orders",
		"",
		regionDateShardingFunc,
		regionDateValueFunc,
		regionDateGetAllFunc,
	)

	shardingValue := "CN:20240115"
	tableName = regionDateStrategy.GetTableName("orders", shardingValue)
	fmt.Printf("Order with region:date '%s' will be in table: %s\n", shardingValue, tableName)

	// 示例 5: 使用自定义函数进行更灵活的分表
	fmt.Println("\n=== 灵活自定义分表函数 ===")

	flexibleShardingFunc := func(baseTableName string, shardingValue interface{}) string {
		// 根据值的类型和内容动态决定分表规则
		switch v := shardingValue.(type) {
		case int64:
			// 数字类型：根据数值范围分表
			if v < 1000000 {
				return fmt.Sprintf("%s_small", baseTableName)
			} else if v < 10000000 {
				return fmt.Sprintf("%s_medium", baseTableName)
			} else {
				return fmt.Sprintf("%s_large", baseTableName)
			}
		case string:
			// 字符串类型：根据长度分表
			if len(v) < 10 {
				return fmt.Sprintf("%s_short", baseTableName)
			} else {
				return fmt.Sprintf("%s_long", baseTableName)
			}
		default:
			return baseTableName
		}
	}

	flexibleStrategy := sharding.NewCustomShardingStrategy(
		"items",
		"Value",
		flexibleShardingFunc,
		nil, // 使用默认值提取
		func(baseTableName string) []string {
			return []string{
				fmt.Sprintf("%s_small", baseTableName),
				fmt.Sprintf("%s_medium", baseTableName),
				fmt.Sprintf("%s_large", baseTableName),
				fmt.Sprintf("%s_short", baseTableName),
				fmt.Sprintf("%s_long", baseTableName),
			}
		},
	)

	testValues := []interface{}{int64(500000), int64(5000000), int64(20000000), "short", "very long string"}
	for _, val := range testValues {
		tableName = flexibleStrategy.GetTableName("items", val)
		fmt.Printf("Value %v -> Table: %s\n", val, tableName)
	}
}
