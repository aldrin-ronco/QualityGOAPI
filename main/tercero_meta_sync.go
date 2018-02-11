package main

import (
	"time"
	"net/http"
	"bytes"
	"strconv"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
)

type Tercero_Meta_Sync struct {
	Id 				int64 		`json:"id"gorm:"column:id"gorm:"primary_key"`
	Tercero_Id 		int 		`json:"tercero_id"gorm:"column:tercero_id"`
	Imei 			string 		`json:"imei"gorm:"column:imei"`
	Last_Modified	time.Time 	`json:"last_modified"gorm:"column:last_modified"`
}

type tercero_query_values struct {
	Tercero_Id		int
	Imei			string
}

func (Tercero_Meta_Sync) TableName() string {
	return DATABASE_NAME + ".dbo.Cnt_Terceros_Meta_Sync"
}

func GetTerceroMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	var sQuery_TMPL string = ""
	var sQuery bytes.Buffer
	var tercero_id int = 0

	// Set DataBase
	DATABASE_NAME = r.Header.Get("host_database")

	// Template Query String
	sQuery_TMPL = `SELECT Cnt_Terceros_Meta_Sync.last_modified 
   				   FROM {{.DBName}}Cnt_Terceros_Meta_Sync
				   WHERE Cnt_Terceros_Meta_Sync.tercero_id = {{.Tercero_Id}} AND LTRIM(RTRIM(Cnt_Terceros_Meta_Sync.imei)) = '{{.Imei}}'`

	tmpl, err := template.New("sQuery_TMPL").Parse(sQuery_TMPL)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	// Get parameters Tercero_Id & IMEI
	Query := r.URL.Query()
	if tercero_id, err = strconv.Atoi(Query.Get("tercero_id")); err != nil {
		fmt.Println(err.Error())
		return http.StatusInternalServerError, err
	}

	// Substitution values
	substitute := tercero_query_values{tercero_id, Query.Get("imei")}

	if err := tmpl.Execute(&sQuery, substitute); err != nil {
		fmt.Println(err.Error())
		return  http.StatusInternalServerError, err
	}

	// Codigo no terminado

	return http.StatusOK, nil
}

func PostTerceroMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {
	var tercero_meta_data Tercero_Meta_Sync
	// Set DataBaseName
	DATABASE_NAME = r.Header.Get("host_database")
	// Obtengo el cuerpo del body
	err := json.NewDecoder(r.Body).Decode(&tercero_meta_data)
	if err != nil {
		fmt.Println(err.Error())
		return http.StatusInternalServerError, err
	}
	// Obtengo la conexión a la base de datos
	db, ok := c.dbs[r.Header.Get("host_domain")]
	if ok {
		if dbc := db.Create(&tercero_meta_data) ; dbc.Error != nil {
			fmt.Println(err.Error())
			return http.StatusInternalServerError, dbc.Error
		}
	}
	return http.StatusOK, nil
}

func PutTerceroMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	// http://localhost:9090/customers_mds?tercero_id=2461&imei=12HFG6754GFT75

	var tercero_meta_data_sync Tercero_Meta_Sync
	var params Tercero_Meta_Sync
	var tercero_id int = 0
	var IMEI string = ""

	// Set DataBaseName
	DATABASE_NAME = r.Header.Get("host_database")

	query := r.URL.Query()

	// Obtener Id del Tercero
	if tercero_id, err = strconv.Atoi(query.Get("tercero_id")); err != nil {
		fmt.Println(err.Error())
		return http.StatusInternalServerError, err
	}

	// Obtener IMEI del Tercero
	if IMEI = query.Get("imei"); strings.Trim(IMEI, " ") == "" {
		fmt.Println("PutTerceroMetaDataSync ha sido llamado sin IMEI !")
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
		db.Unscoped().Find(&tercero_meta_data_sync, "tercero_id = ? AND imei = ?", tercero_id, IMEI)
		fmt.Println(tercero_meta_data_sync)
		if tercero_meta_data_sync.Tercero_Id != 0 {
			if dbc := db.Model(&tercero_meta_data_sync).Updates(&params); dbc.Error != nil {
				fmt.Println(err.Error())
				return http.StatusInternalServerError, dbc.Error
			}
		} else {
			fmt.Println("No ha sido encontrado el registro !")
			return http.StatusInternalServerError, nil
		}
	}
	return http.StatusOK, nil
}
