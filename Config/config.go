package Config

import (
	"os"
)

var DbName = "testdb"
var ApiKey = os.Getenv("cloudant_api")
var AuthURL = os.Getenv("cloudant_authurl")
