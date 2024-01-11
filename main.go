package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
	"github.com/IBM/go-sdk-core/core"
	"github.com/NavdeepSinghBagga/CRUD-ops-with-GoLang-Cloudant/Config"
)

func GetServerInfo(service *cloudantv1.CloudantV1) {
	fmt.Println("GetServerInfo")
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
	fmt.Println("ListDBs")
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
	fmt.Println("CreateDB")
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
	fmt.Println("GetDBDetails")
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

func GetDBChangeInfo(service *cloudantv1.CloudantV1, dbName string) {
	fmt.Println("GetDBChangeInfo")
	postChangesOptions := service.NewPostChangesOptions(
		dbName,
	)

	changesResult, response, err := service.PostChanges(postChangesOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(changesResult, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("DB Changes info: ", string(result))

}

func ListAllDocs(service *cloudantv1.CloudantV1, dbName string) {
	fmt.Println("ListAllDocs")
	postAllDocsOptions := service.NewPostAllDocsOptions(
		dbName,
	)
	postAllDocsOptions.SetIncludeDocs(true)
	postAllDocsOptions.SetLimit(10)

	allDocsResult, response, err := service.PostAllDocs(postAllDocsOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(allDocsResult, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("All Documents: ", string(result))
}

func FindDocument(service *cloudantv1.CloudantV1, dbName string, docId string) *cloudantv1.Document {
	fmt.Println("FindDocument")
	getDocumentOptions := service.NewGetDocumentOptions(
		dbName,
		docId,
	)

	document, response, err := service.GetDocument(getDocumentOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("Document Found: ", string(result))
	return document
}

func CreateDoc(service *cloudantv1.CloudantV1, dbName string, documentName string) {
	fmt.Println("CreateDoc")
	seed := rand.NewSource(time.Now().UnixNano())
	randomNumber := rand.New(seed)
	newDoc := cloudantv1.Document{
		ID: core.StringPtr(dbName + ":" + fmt.Sprint(randomNumber.Int())),
	}
	newDoc.SetProperty("name", documentName)

	postDocumentOptions := service.NewPostDocumentOptions(
		dbName,
	)
	postDocumentOptions.SetDocument(&newDoc)

	documentResult, response, err := service.PostDocument(postDocumentOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(documentResult, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("New Document Created: ", string(result))

}

func DeleteDoc(service *cloudantv1.CloudantV1, dbName string, docId string) {
	fmt.Println("DeleteDoc")
	document := FindDocument(service, dbName, docId)
	deleteDocumentOptions := service.NewDeleteDocumentOptions(
		dbName,
		docId,
	)
	deleteDocumentOptions.SetRev(*document.Rev)

	deleteResult, response, err := service.DeleteDocument(deleteDocumentOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(deleteResult, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))

}

func ModifyDoc(service *cloudantv1.CloudantV1, dbName string, docId string) {
	fmt.Println("ModifyDoc")
	document := FindDocument(service, dbName, docId)
	var updatedName string
	fmt.Print("Enter updatedName: ")
	fmt.Scan(&updatedName)
	document.SetProperty("name", updatedName)

	putDocumentOptions := service.NewPutDocumentOptions(
		dbName,
		docId,
	)
	putDocumentOptions.SetDocument(document)

	updateResult, response, err := service.PutDocument(putDocumentOptions)
	if err != nil {
		fmt.Println("Response: ", response)
		panic(err)
	}

	result, err := json.MarshalIndent(updateResult, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println("Document Modified: ", string(result))
}

func RequesHeaderProcess(service *cloudantv1.CloudantV1, dbName string, docId string) {
	fmt.Println("RequesHeaderProcess")
	FindDocument(service, dbName, docId)
	headDocumentOptions := service.NewHeadDocumentOptions(
		dbName,
		docId,
	)

	response, err := service.HeadDocument(headDocumentOptions)
	if err != nil {
		panic(err)
	}

	fmt.Println("Response Status Code: ", response.StatusCode)
	fmt.Println("Response Headers: ", response.Headers)
	fmt.Println("Response Headers Etag:", response.Headers["Etag"])
}

func UserMenu(service *cloudantv1.CloudantV1) {

	var operationSelected int
	for {
		fmt.Println("-------------------------- Welcome To Cloudant CRUDs --------------------------")
		fmt.Println("1. GetDBDetails")
		fmt.Println("2. ListAllDocs")
		fmt.Println("3. FindDocument")
		fmt.Println("4. CreateDoc")
		fmt.Println("5. ModifyDoc")
		fmt.Println("6. DeleteDoc")
		fmt.Println("7. HTTP Response")
		fmt.Println("8. Exit")
		fmt.Scan(&operationSelected)

		switch operationSelected {
		case 1:
			GetDBDetails(service, Config.DbName)
		case 2:
			ListAllDocs(service, Config.DbName)
		case 3:
			var docId string
			fmt.Print("Enter docId: ")
			fmt.Scan(&docId)
			FindDocument(service, Config.DbName, docId)
		case 4:
			var documentName string
			fmt.Print("Enter name: ")
			fmt.Scan(&documentName)
			CreateDoc(service, Config.DbName, documentName)
		case 5:
			var docId string
			fmt.Print("Enter docId: ")
			fmt.Scan(&docId)
			ModifyDoc(service, Config.DbName, docId)
		case 6:
			var docId string
			fmt.Print("Enter docId: ")
			fmt.Scan(&docId)
			DeleteDoc(service, Config.DbName, docId)
		case 7:
			var docId string
			fmt.Print("Enter docId: ")
			fmt.Scan(&docId)
			RequesHeaderProcess(service, Config.DbName, docId)
		case 8:
			return
		default:
			fmt.Println("Please provide a valid input")
		}
	}
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
	UserMenu(service)
}
