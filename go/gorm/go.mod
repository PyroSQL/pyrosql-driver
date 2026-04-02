module github.com/pyrosql/pyrosql-driver/go/gorm

go 1.21

require (
	github.com/pyrosql/pyrosql-driver/go v1.0.0
	gorm.io/gorm v1.25.12
)

require github.com/jinzhu/inflection v1.0.0 // indirect

require (
	github.com/jinzhu/now v1.1.5 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/pyrosql/pyrosql-driver/go => ../
