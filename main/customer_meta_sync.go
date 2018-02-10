package main

import (
	"time"
	"net/http"
	"text/template"
	"strconv"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type Customer_Meta_Sync struct {
	Client_Id 		int 		`json:"client_id"gorm:"column:client_id"`
	Imei 			string 		`json:"imei"gorm:"column:imei"`
	Last_Modified	time.Time 	`json:"last_modified"gorm:"column:last_modified"`
}

type query_values struct {
	Client_Id		int
	Imei			string
}

func (Customer_Meta_Sync) TableName() string {
	return DATABASE_NAME + ".dbo.Ven_Clientes_Meta_Sync"
}

func GetCustomerMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	var sQuery_TMPL string = ""
	var sQuery bytes.Buffer
	var Client_Id int = 0

	// Set DataBase
	DATABASE_NAME = r.Header.Get("host_database")

	// Template Query String
	sQuery_TMPL = `SELECT Ven_Clientes_Meta_Sync.last_modified 
   				   FROM {{.DBName}}Ven_Clientes_Meta_Sync
				   WHERE Ven_Clientes_Meta_Sync.client_id = {{.Client_Id}} AND LTRIM(RTRIM(Ven_Clientes_Meta_Sync.imei)) = '{{.Imei}}'`

	tmpl, err := template.New("sQuery_TMPL").Parse(sQuery_TMPL)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	// Get parameters Client_Id & IMEI
	Query := r.URL.Query()
	if Client_Id, err = strconv.Atoi(Query.Get("client_id")); err != nil {
		return http.StatusInternalServerError, err
	}

	// Substitution values
	substitute := query_values{Client_Id, Query.Get("imei")}

	if err := tmpl.Execute(&sQuery, substitute); err != nil {
		return  http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func PostCustomerMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {
	var client_meta_data Customer_Meta_Sync

	// Set DataBaseName
	DATABASE_NAME = r.Header.Get("host_database")

	// Obtengo el cuerpo del body
	err := json.NewDecoder(r.Body).Decode(&client_meta_data)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	// Obtengo la conexión a la base de datos
	db, ok := c.dbs[r.Header.Get("host_domain")]

	if ok {
		if dbc := db.Create(&client_meta_data) ; dbc.Error != nil {
			return http.StatusInternalServerError, dbc.Error
		}
	}
	return http.StatusOK, nil
}

func PutCustomerMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	var client_meta_data_sync Customer_Meta_Sync
	var params Customer_Meta_Sync
	var Client_Id int = 0
	var IMEI string = ""

	// Set DataBaseName
	DATABASE_NAME = r.Header.Get("host_database")

	query := r.URL.Query()

	// Obtener Id del Cliente
	if Client_Id, err = strconv.Atoi(query.Get("client_id")); err != nil {
		fmt.Println(err.Error())
		return http.StatusInternalServerError, err
	}

	// Obtener IMEI del Cliente
	if IMEI = query.Get("imei"); strings.Trim(IMEI, " ") == "" {
		fmt.Println("PutCustomerMetaDataSync ha sido llamado sin IMEI !")
		return http.StatusInternalServerError, nil
	}

	// Obtengo el cuerpo del body
	err = json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		fmt.Print(err) // Colocar el error en el LOG
		return http.StatusInternalServerError, err
	}

	db, ok := c.dbs[r.Header.Get("host_domain")]
	if ok {
		db.Unscoped().First(&client_meta_data_sync, "client_id = ? AND imei = ?", Client_Id, IMEI)
		if client_meta_data_sync.Client_Id != 0 {
			if dbc := db.Model(&client_meta_data_sync).Updates(&params); dbc.Error != nil {
				return http.StatusInternalServerError, dbc.Error
			}
		} else {
			fmt.Println("No ha sido encontrado el registro !")
			return http.StatusInternalServerError, nil
		}
	}
	return http.StatusOK, nil
}