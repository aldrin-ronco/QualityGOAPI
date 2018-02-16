package main

import (
	"time"
	"net/http"
	"strconv"
	"fmt"
	"encoding/json"
	"strings"
)

type Seller_Meta_Sync struct {
	Id 				int64 		`json:"id"gorm:"column:id"gorm:"primary_key"`
	Vendedor_Id 	int 		`json:"vendedor_id"gorm:"column:vendedor_id"`
	Imei 			string 		`json:"imei"gorm:"column:imei"`
	Last_Modified	time.Time 	`json:"last_modified"gorm:"column:last_modified"`
}

type seller_query_values struct {
	Vendedpr_Id		int
	Imei			string
}

func (Seller_Meta_Sync) TableName() string {
	return DATABASE_NAME + ".dbo.Cnt_Terceros_Meta_Sync"
}

func PostSellerMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {
	var seller_meta_data Seller_Meta_Sync
	// Set DataBaseName
	DATABASE_NAME = r.Header.Get("host_database")
	// Obtengo el cuerpo del body
	err := json.NewDecoder(r.Body).Decode(&seller_meta_data)
	if err != nil {
		fmt.Println(err.Error())
		return http.StatusInternalServerError, err
	}
	// Obtengo la conexión a la base de datos
	db, ok := c.dbs[r.Header.Get("host_domain")]
	if ok {
		if dbc := db.Create(&seller_meta_data) ; dbc.Error != nil {
			fmt.Println(err.Error())
			return http.StatusInternalServerError, dbc.Error
		}
	}
	return http.StatusOK, nil
}

func PutSellerMetaDataSync (c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	// http://localhost:9090/customers_mds?tercero_id=2461&imei=12HFG6754GFT75

	var seller_meta_data_sync Seller_Meta_Sync
	var params Seller_Meta_Sync
	var seller_id int = 0
	var IMEI string = ""

	// Set DataBaseName
	DATABASE_NAME = r.Header.Get("host_database")

	query := r.URL.Query()

	// Obtener Id del Tercero
	if seller_id, err = strconv.Atoi(query.Get("seller_id")); err != nil {
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
		db.Unscoped().Find(&seller_meta_data_sync, "seller_id = ? AND imei = ?", seller_id, IMEI)
		fmt.Println(seller_meta_data_sync)
		if seller_meta_data_sync.Vendedor_Id != 0 {
			if dbc := db.Model(&seller_meta_data_sync).Updates(&params); dbc.Error != nil {
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

