package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/core"
	"github.com/NavdeepSinghBagga/CRUD-ops-with-GoLang-Cloudant/Config"
)

func GetServerInfo(service *cloudantv1.CloudantV1) {
	getServerInformationOptions := service.NewGetServerInformationOptions()

	serverInformation, response, err := service.GetServerInformation(getServerInformationOptions)
	if err != nil || response.StatusCode != http.StatusOK {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(serverInformation, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("Server Information : ", string(result))
}

func ListDBs(service *cloudantv1.CloudantV1) {
	getAllDbsOptions := service.NewGetAllDbsOptions()

	dbList, response, err := service.GetAllDbs(getAllDbsOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(dbList, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("DB List: ", string(result))
}

func CreateDB(service *cloudantv1.CloudantV1, dbName string) {
	putDatabaseOptions := service.NewPutDatabaseOptions(
		dbName,
	)
	putDatabaseOptions.SetPartitioned(true)

	ok, response, err := service.PutDatabase(putDatabaseOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(ok, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("Databse Created: ", string(result))

}

func GetDBDetails(service *cloudantv1.CloudantV1, dbName string) {
	getDatabaseInformationOptions := service.NewGetDatabaseInformationOptions(
		dbName,
	)

	databaseInformation, response, err := service.GetDatabaseInformation(getDatabaseInformationOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		if response.StatusCode == http.StatusNotFound {
			fmt.Println("Database not found, creating the database!!")
			CreateDB(service, dbName)
			return
		}
		panic(err)
	}

	result, err := json.MarshalIndent(databaseInformation, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("DB Details: ", string(result))
	fmt.Println("Total Docmunets: ", *(databaseInformation.DocCount))
}

func main() {
	fmt.Println("Basic crud operations using GoLang and CloudantDB(CouchDB)")
	// Cloudant Connection
	authenticator := &core.IamAuthenticator{
		ApiKey: Config.ApiKey,
	}

	service, err := cloudantv1.NewCloudantV1(
		&cloudantv1.CloudantV1Options{
			URL:           Config.AuthURL,
			Authenticator: authenticator,
		},
	)
	if err != nil {
		fmt.Println("Error in Authentication.\nUnable to Establish connection.")
		panic(err)
	}
	fmt.Println("Connection to Cloudant is established!!")

	GetServerInfo(service)
	ListDBs(service)
	GetDBDetails(service, Config.DbName)
}
