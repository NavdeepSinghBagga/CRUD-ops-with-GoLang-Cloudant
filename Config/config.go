package Config

import (
	"os"
)

var DbName = "test-db"
var ApiKey = os.Getenv("cloudant_api")
var AuthURL = os.Getenv("cloudant_authurl")
