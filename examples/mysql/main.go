package main

import (
	"fmt"
	"os"
	"time"

	"github.com/dullgiulio/sqlretry"
	_ "github.com/go-sql-driver/mysql"
)

func makeMysqlDSN(args []string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", args[0], args[1], args[2], args[3], args[4])
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 6 {
		fail(fmt.Errorf("Usage: <user> <password> <host> <port> <database>"))
	}

	db, err := sqlretry.NewRetryDB("mysql", makeMysqlDSN(os.Args[1:6]))
	if err != nil {
		fail(err)
	}
	db.SetRetries(3)
	db.SetBackoff(2 * time.Second)
	db.SetMultiplier(1.5)
	db.SetParallel(1)

	_, err = db.Query("SELECT * FROM INFORMATION_SCHEMA;")
	if err != nil {
		fail(err)
	}
}
