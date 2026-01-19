package sharding

import (
	"reflect"
	"strconv"
	"time"
)

// TimeShardingUnit 时间分表单位
type TimeShardingUnit int

const (
	TimeShardingByYear   TimeShardingUnit = iota // 按年分表
	TimeShardingByMonth                          // 按月分表
	TimeShardingByDay                            // 按日分表
	TimeShardingByHour                           // 按小时分表
	TimeShardingByMinute                         // 按分钟分表
)

// TimeFieldType 时间字段类型
type TimeFieldType int

const (
	TimeFieldTypeAuto        TimeFieldType = iota // 自动识别（默认）
	TimeFieldTypeTime                             // time.Time 类型
	TimeFieldTypeTimestamp                        // int64/uint64 Unix 时间戳（秒）
	TimeFieldTypeTimestampMs                      // int64 Unix 时间戳（毫秒）
	TimeFieldTypeDate                             // string 日期格式 (YYYY-MM-DD)
	TimeFieldTypeDateTime                         // string 日期时间格式 (YYYY-MM-DD HH:MM:SS)
)

// TimeShardingStrategy 基于时间的分表策略
type TimeShardingStrategy struct {
	baseTableName string
	timeField     string           // 时间字段名（如 "created_at"）
	unit          TimeShardingUnit // 分表单位
	timeFormat    string           // 时间格式字符串
	fieldType     TimeFieldType    // 时间字段类型
}

// NewTimeShardingStrategy 创建时间分表策略
// baseTableName: 基础表名（如 "logs"）
// timeField: 时间字段名（如 "created_at"）
// unit: 分表单位（年/月/日/小时/分钟）
func NewTimeShardingStrategy(baseTableName, timeField string, unit TimeShardingUnit) *TimeShardingStrategy {
	return NewTimeShardingStrategyWithType(baseTableName, timeField, unit, TimeFieldTypeAuto)
}

// NewTimeShardingStrategyWithType 创建时间分表策略（指定时间字段类型）
// baseTableName: 基础表名（如 "logs"）
// timeField: 时间字段名（如 "created_at"）
// unit: 分表单位（年/月/日/小时/分钟）
// fieldType: 时间字段类型（自动识别/Time/时间戳/日期等）
func NewTimeShardingStrategyWithType(baseTableName, timeField string, unit TimeShardingUnit, fieldType TimeFieldType) *TimeShardingStrategy {
	strategy := &TimeShardingStrategy{
		baseTableName: baseTableName,
		timeField:     timeField,
		unit:          unit,
		fieldType:     fieldType,
	}
	strategy.timeFormat = strategy.getTimeFormat(unit)
	return strategy
}

// GetTableName 根据时间值获取实际表名
func (s *TimeShardingStrategy) GetTableName(baseTableName string, shardingValue interface{}) string {
	t := s.convertToTime(shardingValue)
	return FormatTimeTableName(baseTableName, t, s.timeFormat)
}

// GetAllTableNames 获取所有分表名称（需要指定时间范围）
// 注意：时间分表是动态的，此方法需要时间范围参数
func (s *TimeShardingStrategy) GetAllTableNames(baseTableName string) []string {
	// 时间分表是动态的，无法预先获取所有表名
	// 此方法主要用于接口实现，实际使用时应使用 GetAllTableNamesInRange
	return []string{baseTableName}
}

// GetAllTableNamesInRange 获取指定时间范围内的所有表名
func (s *TimeShardingStrategy) GetAllTableNamesInRange(baseTableName string, startTime, endTime time.Time) []string {
	tableNames := make([]string, 0)
	currentTime := startTime

	for currentTime.Before(endTime) || currentTime.Equal(endTime) {
		tableName := FormatTimeTableName(baseTableName, currentTime, s.timeFormat)
		tableNames = append(tableNames, tableName)

		// 移动到下一个时间单位
		switch s.unit {
		case TimeShardingByYear:
			currentTime = currentTime.AddDate(1, 0, 0)
		case TimeShardingByMonth:
			currentTime = currentTime.AddDate(0, 1, 0)
		case TimeShardingByDay:
			currentTime = currentTime.AddDate(0, 0, 1)
		case TimeShardingByHour:
			currentTime = currentTime.Add(time.Hour)
		case TimeShardingByMinute:
			currentTime = currentTime.Add(time.Minute)
		}
	}

	// 去重
	uniqueNames := make(map[string]bool)
	result := make([]string, 0)
	for _, name := range tableNames {
		if !uniqueNames[name] {
			uniqueNames[name] = true
			result = append(result, name)
		}
	}

	return result
}

// GetShardingValue 从模型对象中提取时间字段值
func (s *TimeShardingStrategy) GetShardingValue(value interface{}) (interface{}, error) {
	timeValue, err := ExtractValue(value, s.timeField)
	if err != nil {
		return nil, err
	}

	// 如果已经指定了字段类型，使用指定的类型转换
	if s.fieldType != TimeFieldTypeAuto {
		return s.convertByType(timeValue, s.fieldType), nil
	}

	// 自动识别类型并转换
	return s.convertToTime(timeValue), nil
}

// convertToTime 将各种类型的时间值转换为 time.Time
func (s *TimeShardingStrategy) convertToTime(value interface{}) time.Time {
	if value == nil {
		return time.Now()
	}

	// 如果已经是指定类型，直接使用
	if s.fieldType != TimeFieldTypeAuto {
		result := s.convertByType(value, s.fieldType)
		if t, ok := result.(time.Time); ok {
			return t
		}
		return time.Now()
	}

	// 自动识别类型
	switch v := value.(type) {
	case time.Time:
		return v

	case int:
		// Unix 时间戳（秒）
		return time.Unix(int64(v), 0)
	case int32:
		return time.Unix(int64(v), 0)
	case int64:
		// 判断是秒还是毫秒（通常 > 1e10 的是毫秒）
		if v > 1e10 {
			return time.Unix(v/1000, (v%1000)*1e6)
		}
		return time.Unix(v, 0)

	case uint:
		return time.Unix(int64(v), 0)
	case uint32:
		return time.Unix(int64(v), 0)
	case uint64:
		// 判断是秒还是毫秒
		if v > 1e10 {
			return time.Unix(int64(v/1000), int64((v%1000)*1e6))
		}
		return time.Unix(int64(v), 0)

	case string:
		return s.parseStringTime(v)

	case *time.Time:
		if v != nil {
			return *v
		}
		return time.Now()

	default:
		// 尝试通过反射获取值
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				return time.Now()
			}
			rv = rv.Elem()
		}

		// 尝试作为整数时间戳
		if rv.CanInt() {
			timestamp := rv.Int()
			if timestamp > 1e10 {
				return time.Unix(timestamp/1000, (timestamp%1000)*1e6)
			}
			return time.Unix(timestamp, 0)
		}

		// 尝试作为无符号整数时间戳
		if rv.CanUint() {
			timestamp := rv.Uint()
			if timestamp > 1e10 {
				return time.Unix(int64(timestamp/1000), int64((timestamp%1000)*1e6))
			}
			return time.Unix(int64(timestamp), 0)
		}

		// 尝试转换为字符串再解析
		if rv.CanInterface() {
			if str, ok := rv.Interface().(string); ok {
				return s.parseStringTime(str)
			}
		}

		return time.Now()
	}
}

// convertByType 根据指定的类型转换时间值
func (s *TimeShardingStrategy) convertByType(value interface{}, fieldType TimeFieldType) interface{} {
	if value == nil {
		return time.Now()
	}

	switch fieldType {
	case TimeFieldTypeTime:
		if t, ok := value.(time.Time); ok {
			return t
		}
		if t, ok := value.(*time.Time); ok && t != nil {
			return *t
		}
		return time.Now()

	case TimeFieldTypeTimestamp:
		// Unix 时间戳（秒）
		return s.convertToTimestamp(value, false)

	case TimeFieldTypeTimestampMs:
		// Unix 时间戳（毫秒）
		return s.convertToTimestamp(value, true)

	case TimeFieldTypeDate:
		// 日期字符串 YYYY-MM-DD
		if str, ok := value.(string); ok {
			return str
		}
		t := s.convertToTime(value)
		return t.Format("2006-01-02")

	case TimeFieldTypeDateTime:
		// 日期时间字符串 YYYY-MM-DD HH:MM:SS
		if str, ok := value.(string); ok {
			return str
		}
		t := s.convertToTime(value)
		return t.Format("2006-01-02 15:04:05")

	default:
		return s.convertToTime(value)
	}
}

// convertToTimestamp 转换为时间戳
func (s *TimeShardingStrategy) convertToTimestamp(value interface{}, isMillisecond bool) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	case uint32:
		return int64(v)
	case time.Time:
		if isMillisecond {
			return v.UnixMilli()
		}
		return v.Unix()
	case *time.Time:
		if v != nil {
			if isMillisecond {
				return v.UnixMilli()
			}
			return v.Unix()
		}
	case string:
		// 尝试解析字符串
		t := s.parseStringTime(v)
		if isMillisecond {
			return t.UnixMilli()
		}
		return t.Unix()
	}
	return 0
}

// parseStringTime 解析字符串时间
func (s *TimeShardingStrategy) parseStringTime(str string) time.Time {
	if str == "" {
		return time.Now()
	}

	// 尝试多种时间格式
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, str); err == nil {
			return t
		}
	}

	// 尝试作为 Unix 时间戳字符串解析
	if timestamp, err := strconv.ParseInt(str, 10, 64); err == nil {
		// 判断是秒还是毫秒
		if timestamp > 1e10 {
			return time.Unix(timestamp/1000, (timestamp%1000)*1e6)
		}
		return time.Unix(timestamp, 0)
	}

	return time.Now()
}

// GetBaseTableName 获取基础表名
func (s *TimeShardingStrategy) GetBaseTableName() string {
	return s.baseTableName
}

// getTimeFormat 根据分表单位获取时间格式
func (s *TimeShardingStrategy) getTimeFormat(unit TimeShardingUnit) string {
	switch unit {
	case TimeShardingByYear:
		return "2006"
	case TimeShardingByMonth:
		return "200601"
	case TimeShardingByDay:
		return "20060102"
	case TimeShardingByHour:
		return "2006010215"
	case TimeShardingByMinute:
		return "200601021504"
	default:
		return "200601"
	}
}

// GetTimeFieldType 获取时间字段类型
func (s *TimeShardingStrategy) GetTimeFieldType() TimeFieldType {
	return s.fieldType
}

// ParseTimeRange 从查询条件中解析时间范围（辅助函数，用于跨表查询优化）
// 支持从 int64 时间戳、time.Time 或字符串时间中提取时间范围
func (s *TimeShardingStrategy) ParseTimeRange(startValue, endValue interface{}) (time.Time, time.Time, error) {
	startTime := s.convertToTime(startValue)
	endTime := s.convertToTime(endValue)

	if startTime.After(endTime) {
		return endTime, startTime, nil // 自动交换
	}

	return startTime, endTime, nil
}

// GetAllTableNamesInRangeWithValues 使用时间值（支持多种类型）获取表名范围
func (s *TimeShardingStrategy) GetAllTableNamesInRangeWithValues(baseTableName string, startValue, endValue interface{}) []string {
	startTime := s.convertToTime(startValue)
	endTime := s.convertToTime(endValue)

	if startTime.After(endTime) {
		startTime, endTime = endTime, startTime
	}

	return s.GetAllTableNamesInRange(baseTableName, startTime, endTime)
}
