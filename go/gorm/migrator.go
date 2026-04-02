package pyrosql

import (
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// PyroMigrator extends the base GORM migrator with PyroSQL-specific behavior.
type PyroMigrator struct {
	migrator.Migrator
}

// AutoMigrate implements gorm.Migrator by creating or altering tables to match models.
func (m PyroMigrator) AutoMigrate(values ...interface{}) error {
	for _, value := range values {
		tx := m.DB.Session(&gorm.Session{})
		if !m.HasTable(value) {
			if err := m.CreateTable(value); err != nil {
				return err
			}
		} else {
			if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
				columnTypes, err := m.ColumnTypes(value)
				if err != nil {
					return err
				}

				existingColumns := make(map[string]gorm.ColumnType)
				for _, ct := range columnTypes {
					existingColumns[ct.Name()] = ct
				}

				for _, dbName := range stmt.Schema.DBNames {
					field := stmt.Schema.FieldsByDBName[dbName]
					if _, ok := existingColumns[dbName]; !ok {
						if err := m.AddColumn(value, dbName); err != nil {
							return err
						}
					} else {
						// Column exists -- check if we need to alter it
						_ = field // future: type comparison
					}
				}

				for _, rel := range stmt.Schema.Relationships.Relations {
					if rel.Field.IgnoreMigration {
						continue
					}
					if constraint := rel.ParseConstraint(); constraint != nil &&
						constraint.Schema == stmt.Schema &&
						!tx.Migrator().HasConstraint(value, constraint.Name) {
						if err := tx.Migrator().CreateConstraint(value, constraint.Name); err != nil {
							return err
						}
					}
				}

				for _, idx := range stmt.Schema.ParseIndexes() {
					if !tx.Migrator().HasIndex(value, idx.Name) {
						if err := tx.Migrator().CreateIndex(value, idx.Name); err != nil {
							return err
						}
					}
				}

				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateTable creates a table for the given model.
func (m PyroMigrator) CreateTable(values ...interface{}) error {
	for _, value := range values {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			var (
				createTableSQL          = "CREATE TABLE ? ("
				args                    = []interface{}{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				if !field.IgnoreMigration {
					createTableSQL += "? ?"
					hasPrimaryKeyInDataType = hasPrimaryKeyInDataType || isSerialType(m.DB.Migrator().(PyroMigrator).DataTypeOf(field))
					args = append(args,
						clause.Column{Name: dbName},
						m.fullDataTypeOf(field),
					)
					createTableSQL += ","
				}
			}

			// Primary key constraint (skip if using SERIAL which implies PK)
			if !hasPrimaryKeyInDataType {
				createTableSQL += m.buildPrimaryKeyClause(stmt.Schema)
			}

			createTableSQL = strings.TrimSuffix(createTableSQL, ",")
			createTableSQL += ")"

			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
				createTableSQL += fmt.Sprintf(" %s", tableOption)
			}

			return tx.Exec(createTableSQL, args...).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// DataTypeOf returns the SQL type for a schema field.
func (m PyroMigrator) DataTypeOf(field *schema.Field) string {
	return m.Dialector.DataTypeOf(field)
}

func (m PyroMigrator) fullDataTypeOf(field *schema.Field) clause.Expr {
	expr := clause.Expr{SQL: m.DataTypeOf(field)}

	if isSerialType(expr.SQL) {
		expr.SQL += " PRIMARY KEY"
		return expr
	}

	if field.NotNull {
		expr.SQL += " NOT NULL"
	}

	if field.Unique {
		expr.SQL += " UNIQUE"
	}

	if field.HasDefaultValue && field.DefaultValueInterface != nil {
		expr.SQL += " DEFAULT ?"
		expr.Vars = append(expr.Vars, field.DefaultValueInterface)
	}

	return expr
}

func (m PyroMigrator) buildPrimaryKeyClause(s *schema.Schema) string {
	var primaryKeys []string
	for _, field := range s.PrimaryFields {
		if isSerialType(m.DataTypeOf(field)) {
			continue
		}
		primaryKeys = append(primaryKeys, fmt.Sprintf(`"%s"`, field.DBName))
	}
	if len(primaryKeys) > 0 {
		return fmt.Sprintf("PRIMARY KEY (%s),", strings.Join(primaryKeys, ","))
	}
	return ""
}

func isSerialType(dataType string) bool {
	upper := strings.ToUpper(dataType)
	return strings.Contains(upper, "SERIAL")
}

// HasTable checks if a table exists by querying PyroSQL system tables.
func (m PyroMigrator) HasTable(value interface{}) bool {
	var count int64
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM pyrosql_tables WHERE table_name = $1",
			stmt.Table,
		).Row().Scan(&count)
	})
	if err != nil {
		// If system table query fails, try an alternative approach
		return m.hasTableFallback(value)
	}
	return count > 0
}

func (m PyroMigrator) hasTableFallback(value interface{}) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public'",
			stmt.Table,
		).Row().Scan(&count)
	})
	return count > 0
}

// HasColumn checks if a column exists in a table.
func (m PyroMigrator) HasColumn(value interface{}, field string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
			stmt.Table, field,
		).Row().Scan(&count)
	})
	return count > 0
}

// HasIndex checks if an index exists.
func (m PyroMigrator) HasIndex(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}
		return m.DB.Raw(
			"SELECT COUNT(*) FROM information_schema.statistics WHERE table_name = $1 AND index_name = $2",
			stmt.Table, name,
		).Row().Scan(&count)
	})
	return count > 0
}

// HasConstraint checks if a constraint exists.
func (m PyroMigrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_name = $1 AND constraint_name = $2",
			stmt.Table, name,
		).Row().Scan(&count)
	})
	return count > 0
}

// ColumnTypes returns column type information for a table.
func (m PyroMigrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	columnTypes := make([]gorm.ColumnType, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		rows, err := m.DB.Raw(
			"SELECT column_name, data_type, is_nullable, column_default, character_maximum_length "+
				"FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position",
			stmt.Table,
		).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var (
				columnName string
				dataType   string
				nullable   string
				dflt       *string
				maxLen     *int64
			)
			if err := rows.Scan(&columnName, &dataType, &nullable, &dflt, &maxLen); err != nil {
				return err
			}

			ct := &pyroColumnType{
				name:     columnName,
				dataType: dataType,
				nullable: nullable == "YES",
			}
			if dflt != nil {
				ct.defaultValue = *dflt
				ct.hasDefault = true
			}
			if maxLen != nil {
				ct.length = *maxLen
				ct.hasLength = true
			}
			columnTypes = append(columnTypes, ct)
		}
		return rows.Err()
	})
	return columnTypes, err
}

// AddColumn adds a column to an existing table.
func (m PyroMigrator) AddColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		field := stmt.Schema.FieldsByDBName[name]
		if field == nil {
			return fmt.Errorf("pyrosql migrator: field %s not found", name)
		}
		return m.DB.Exec(
			"ALTER TABLE ? ADD COLUMN ? ?",
			m.CurrentTable(stmt),
			clause.Column{Name: field.DBName},
			m.fullDataTypeOf(field),
		).Error
	})
}

// DropColumn drops a column from a table.
func (m PyroMigrator) DropColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec(
			"ALTER TABLE ? DROP COLUMN ?",
			m.CurrentTable(stmt),
			clause.Column{Name: name},
		).Error
	})
}

// AlterColumn alters a column type.
func (m PyroMigrator) AlterColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		field := stmt.Schema.FieldsByDBName[name]
		if field == nil {
			return fmt.Errorf("pyrosql migrator: field %s not found", name)
		}
		return m.DB.Exec(
			"ALTER TABLE ? ALTER COLUMN ? TYPE ?",
			m.CurrentTable(stmt),
			clause.Column{Name: field.DBName},
			clause.Expr{SQL: m.DataTypeOf(field)},
		).Error
	})
}

// RenameColumn renames a column.
func (m PyroMigrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec(
			"ALTER TABLE ? RENAME COLUMN ? TO ?",
			m.CurrentTable(stmt),
			clause.Column{Name: oldName},
			clause.Column{Name: newName},
		).Error
	})
}

// CreateIndex creates an index on a table.
func (m PyroMigrator) CreateIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		idx := stmt.Schema.LookIndex(name)
		if idx == nil {
			return fmt.Errorf("pyrosql migrator: index %s not found", name)
		}

		createIndexSQL := "CREATE "
		if idx.Class == "UNIQUE" {
			createIndexSQL += "UNIQUE "
		}
		createIndexSQL += "INDEX ? ON ?("

		var columns []string
		for _, field := range idx.Fields {
			col := fmt.Sprintf(`"%s"`, field.DBName)
			if field.Sort != "" {
				col += " " + string(field.Sort)
			}
			columns = append(columns, col)
		}
		createIndexSQL += strings.Join(columns, ",") + ")"

		return m.DB.Exec(
			createIndexSQL,
			clause.Column{Name: idx.Name},
			m.CurrentTable(stmt),
		).Error
	})
}

// DropIndex drops an index.
func (m PyroMigrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}
		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}).Error
	})
}

// RenameIndex renames an index.
func (m PyroMigrator) RenameIndex(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec(
			"ALTER INDEX ? RENAME TO ?",
			clause.Column{Name: oldName},
			clause.Column{Name: newName},
		).Error
	})
}

// DropTable drops one or more tables.
func (m PyroMigrator) DropTable(values ...interface{}) error {
	for _, value := range values {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", m.CurrentTable(stmt)).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// RenameTable renames a table.
func (m PyroMigrator) RenameTable(oldName, newName interface{}) error {
	oldTable := clause.Table{Name: fmt.Sprint(oldName)}
	newTable := clause.Table{Name: fmt.Sprint(newName)}
	return m.DB.Exec("ALTER TABLE ? RENAME TO ?", oldTable, newTable).Error
}

// CreateConstraint creates a constraint.
func (m PyroMigrator) CreateConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, chk := m.lookupConstraint(stmt.Schema, name)
		if chk != nil {
			return m.DB.Exec(
				"ALTER TABLE ? ADD CONSTRAINT ? CHECK (?)",
				m.CurrentTable(stmt),
				clause.Column{Name: chk.Name},
				clause.Expr{SQL: chk.Constraint},
			).Error
		}
		if constraint != nil {
			var foreignKeys, references []string
			for _, field := range constraint.ForeignKeys {
				foreignKeys = append(foreignKeys, fmt.Sprintf(`"%s"`, field.DBName))
			}
			for _, field := range constraint.References {
				references = append(references, fmt.Sprintf(`"%s"`, field.DBName))
			}
			sql := fmt.Sprintf(
				"ALTER TABLE ? ADD CONSTRAINT ? FOREIGN KEY (%s) REFERENCES ?(%s)",
				strings.Join(foreignKeys, ","),
				strings.Join(references, ","),
			)
			args := []interface{}{
				m.CurrentTable(stmt),
				clause.Column{Name: constraint.Name},
				clause.Table{Name: constraint.ReferenceSchema.Table},
			}
			if constraint.OnDelete != "" {
				sql += " ON DELETE " + constraint.OnDelete
			}
			if constraint.OnUpdate != "" {
				sql += " ON UPDATE " + constraint.OnUpdate
			}
			return m.DB.Exec(sql, args...).Error
		}
		return nil
	})
}

// DropConstraint drops a constraint.
func (m PyroMigrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec(
			"ALTER TABLE ? DROP CONSTRAINT ?",
			m.CurrentTable(stmt),
			clause.Column{Name: name},
		).Error
	})
}

// CurrentTable returns the current table clause for a statement.
func (m PyroMigrator) CurrentTable(stmt *gorm.Statement) interface{} {
	return clause.Table{Name: stmt.Table}
}

// lookupConstraint finds a constraint or check by name in the schema.
func (m PyroMigrator) lookupConstraint(s *schema.Schema, name string) (*schema.Constraint, *schema.CheckConstraint) {
	if s.CreateClauses != nil {
		// Iterate relationships to find foreign key constraints
		for _, rel := range s.Relationships.Relations {
			if c := rel.ParseConstraint(); c != nil && c.Name == name {
				return c, nil
			}
		}
	}
	// Check for check constraints
	for _, chk := range s.ParseCheckConstraints() {
		if chk.Name == name {
			c := chk
			return nil, &c
		}
	}
	// Also search relationships if CreateClauses was nil
	for _, rel := range s.Relationships.Relations {
		if c := rel.ParseConstraint(); c != nil && c.Name == name {
			return c, nil
		}
	}
	return nil, nil
}

// pyroColumnType implements gorm.ColumnType.
type pyroColumnType struct {
	name         string
	dataType     string
	nullable     bool
	hasDefault   bool
	defaultValue string
	hasLength    bool
	length       int64
}

func (c *pyroColumnType) Name() string                                   { return c.name }
func (c *pyroColumnType) DatabaseTypeName() string                       { return c.dataType }
func (c *pyroColumnType) Length() (int64, bool)                          { return c.length, c.hasLength }
func (c *pyroColumnType) Nullable() (bool, bool)                        { return c.nullable, true }
func (c *pyroColumnType) DecimalSize() (int64, int64, bool)             { return 0, 0, false }
func (c *pyroColumnType) ScanType() reflect.Type                        { return reflect.TypeOf("") }
func (c *pyroColumnType) DefaultValue() (string, bool)                  { return c.defaultValue, c.hasDefault }
func (c *pyroColumnType) DefaultValueValue() (string, bool)             { return c.defaultValue, c.hasDefault }
func (c *pyroColumnType) PrimaryKey() (bool, bool)                      { return false, false }
func (c *pyroColumnType) AutoIncrement() (bool, bool)                   { return false, false }
func (c *pyroColumnType) Comment() (string, bool)                       { return "", false }
func (c *pyroColumnType) Unique() (bool, bool)                          { return false, false }
func (c *pyroColumnType) ColumnType() (string, bool)                    { return c.dataType, true }
