package gorm_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/assisrafael/gorm"
	_ "github.com/assisrafael/gorm/dialects/postgres"
)

type CheckoutCart struct {
}

func (c CheckoutCart) TableName() string {
	return "checkout.shopping_cart"
}

func TestTableNameWithSchema(t *testing.T) {
	var db *gorm.DB
	var err error
	dbDSN := os.Getenv("GORM_DSN")
	switch os.Getenv("GORM_DIALECT") {
	case "postgres":
		fmt.Println("testing postgres...")
		if dbDSN == "" {
			dbDSN = "user=gorm password=gorm dbname=gorm port=9920 sslmode=disable"
		}
		db, err = gorm.Open("postgres", dbDSN)

		if err != nil {
			panic(fmt.Sprintf("No error should happen when connecting to test database, but got err=%+v", err))
		}
	default:
		return
	}

	if debug := os.Getenv("DEBUG"); debug == "true" {
		db.LogMode(true)
	} else if debug == "false" {
		db.LogMode(false)
	}

	if err := DB.AutoMigrate(&CheckoutCart{}).Error; err != nil {
		panic(fmt.Sprintf("No error should happen when create table, but got %+v", err))
	}
}
