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

func GetDBChangeInfo(service *cloudantv1.CloudantV1, dbName string) {
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
	fmt.Println("All Documents: ", string(result))
}

func FindDocument(service *cloudantv1.CloudantV1, dbName string, docId string) *cloudantv1.Document {
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
	fmt.Println("Document Found: ", string(result))
	return document
}

func CreateDoc(service *cloudantv1.CloudantV1, dbName string, documentName string) {
	newDoc := cloudantv1.Document{
		ID: core.StringPtr(dbName + "7:id123"),
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
	fmt.Println("Document Modified: ", string(result))
}

func UserMenu(service *cloudantv1.CloudantV1) {

	var operationSelected int
	for true {
		fmt.Println("-------------------------- Welcome To Cloudant CRUDs --------------------------")
		fmt.Println("1. GetDBDetails")
		fmt.Println("2. ListAllDocs")
		fmt.Println("3. FindDocument")
		fmt.Println("4. CreateDoc")
		fmt.Println("5. ModifyDoc")
		fmt.Println("6. DeleteDoc")
		fmt.Println("7. Exit")
		fmt.Scan(&operationSelected)

		switch operationSelected {
		case 1:
			GetDBDetails(service, Config.DbName)
			break
		case 2:
			ListAllDocs(service, Config.DbName)
			break
		case 3:
			var docId string
			fmt.Print("Enter docId: ")
			fmt.Scan(&docId)
			FindDocument(service, Config.DbName, docId)
			break
		case 4:
			var documentName string
			fmt.Print("Enter name: ")
			fmt.Scan(&documentName)
			CreateDoc(service, Config.DbName, documentName)
			break
		case 5:
			var docId string
			fmt.Print("Enter docId: ")
			fmt.Scan(&docId)
			ModifyDoc(service, Config.DbName, docId)
			break
		case 6:
			var docId string
			fmt.Print("Enter docId: ")
			fmt.Scan(&docId)
			DeleteDoc(service, Config.DbName, docId)
			break
		case 7:
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
	// ListDBs(service)
	// GetDBDetails(service, Config.DbName)
	// GetDBChangeInfo(service, Config.DbName) // may use later

	// ListAllDocs(service, Config.DbName)
	// FindDocument(service, Config.DbName, Config.DbName+":id123")
	// CreateDoc(service, Config.DbName)
	// DeleteDoc(service, Config.DbName, Config.DbName+"7:id123")
	// ModifyDoc(service, Config.DbName, Config.DbName+":id123")
}
