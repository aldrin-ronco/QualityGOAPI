package main

import (
	"fmt"
	"net/http"
	"strings"
	"encoding/json"
	"bytes"
	"github.com/gorilla/mux"
	"strconv"
	"text/template"
	"time"
)

type Seller struct {
	Id 					int 		`json:"id"gorm:"column:id"gorm:"primary_key"`
	Cedula				string 		`json:"cedula"gorm:"column:cedula"`
	Codven				string 		`json:"codven"gorm:"column:codven"`
	Nombre_Com			string 		`json:"nombre_com"`
	Activo				bool 		`json:"activo"gorm:"column:activo"`
	Last_Modified 		time.Time 	`json:"last_modified"gorm:"column:last_modified"`
	Last_Modified_Mds	*time.Time  `json:"last_modified_mds"`
	Deleted_At	   		*time.Time 	`json:"deleted_at"gorm:"column:deleted_at"`
}

type Response_Seller struct {
	Pagination 	pagination `json:"pagination"`
	Sellers		[]Seller   `json:"data"`
}

func (Seller) TableName() string {
	return DATABASE_NAME + ".DBO.Ven_Vendedor"
}


func GetSellers(c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	var sQuery_TMPL, sQuery_Counter string
	var sQuery, sDBPrefix bytes.Buffer
	var sFilter_Query, sFilter_Pagination string = "", ""
	var seller Seller
	var for_sync bool
	var sTop_Criteria string = ""

	response := &Response_Seller{}
	response.Sellers = make([]Seller, 0)

	// Set DataBaseName
	DATABASE_NAME = r.Header.Get("host_database")

	// Valid End-Points
	// /terceros -> All Terceros
	// /terceros/90 -> Tercero Id = 90
	// /terceros?pagen_no=1&page_size=50 -> Tercero's selection from 1 to 50
	// /terceros?pagen_no=2&page_size=50 -> Tercero's selection from 51 to 100
	// /terceros?pagen_no=1&page_size=50&filter=CARLOS -> Tercero's selection from 1 to 50 Where Tercero's name
	//  contains "CARLOS" string.
	// /terceros?for_sync=true&imei=846987a4153503f1 -> Para que me devuelva unicamente los terceros pedientes de sincronizacion para el imei : 846987a4153503f1

	vars := mux.Vars(r)
	query := r.URL.Query()

	sId, sFilter, sPage_size, sOffset, sPageNo, host_database, sForSync, IMEI  := vars["id"], query.Get("filter"), query.Get("page_size"),
		query.Get("offset"), query.Get("page_no"), r.Header.Get("host_database"), query.Get("for_sync"), query.Get("imei") // Check if Id is provided

	// Low Cost concatenation process
	sDBPrefix.WriteString(host_database)
	sDBPrefix.WriteString(".DBO.")

	// Query Values
	type query_values struct {
		Id         int
		Page_Size  int
		OffSet     int
		DBName     string
		Filter     string
		Top 	   string
		Pagination string
		Imei 	   string
	}

	// Casting
	id, _ := strconv.Atoi(sId)
	page_size, _ := strconv.Atoi(sPage_size)
	offset, _ := strconv.Atoi(sOffset)
	page_no, _ := strconv.Atoi(sPageNo)

	if strings.Trim(sForSync, " ") != "" {
		for_sync, err = strconv.ParseBool(sForSync)
		if err != nil {
			return http.StatusInternalServerError, err
		}
	} else {
		for_sync = false
	}

	// Just records with LastModified date diferent than lastSync
	if for_sync {
		sFilter_Query = "AND (Ven_Vendedor.Last_Modified<>VMS.Last_Modified OR VMS.Last_Modified IS NULL) "
		sTop_Criteria = "TOP (50) " // Sync data in 5 records chuncks per request
		if strings.Trim(IMEI, " ") == "" {
			fmt.Println("For_Sync ha sido llamado sin IMEI !")
			return http.StatusInternalServerError, nil
		}
	}

	// Set Filter
	if strings.Trim(sFilter, " ") != "" {
		sFilter_Query = sFilter_Query + "AND VEN.Nombre_Com LIKE '%" + strings.Replace(sFilter, " ", "%", -1) + "%'"
	}

	// OffSet, Page_Size and Page No.
	if strings.Trim(sPage_size, " ") != "" && strings.Trim(sPageNo, " ") != "" {
		sFilter_Pagination = "OFFSET " + strconv.Itoa((page_no-1)*page_size) + " ROWS \n" +
			"FETCH NEXT " + strconv.Itoa(page_size) + " ROWS ONLY"
	}

	// Fill Values
	substitute := query_values{Id: id, Page_Size: page_size, OffSet: offset,
		DBName: sDBPrefix.String(), Filter: sFilter_Query, Pagination: sFilter_Pagination,
		Top: sTop_Criteria, Imei: IMEI}

	// Query selection
	switch {
	case strings.Trim(sId, " ") != "": // If a customer id is provided
		sQuery_TMPL = `SELECT Ven_Vendedor.id, Ven_Vendedor.cedula, Ven_Vendedor.codven, VEN.nombre_com, 
  					   Ven_Vendedor.activo, Ven_Vendedor.last_modified, VMS.last_modified As last_modified_mds, Ven_Vendedor.deleted_at
  					   FROM {{.DBName}}Ven_Vendedor
					   LEFT JOIN {{.DBName}}Cnt_Terceros VEN ON VEN.CodTer = Ven_Vendedor.cedula
					   LEFT JOIN {{.DBName}}Ven_Vendedor_Meta_Sync VMS ON VMS.vendedor_id = ven_vendedor.id AND LTRIM(RTRIM(VMS.IMEI)) = '{{.Imei}}'
					   WHERE Ven_Vendedor.id = {{.Id}}
					   ORDER BY VEN.nombre_com`
	default:
		sQuery_TMPL = `SELECT {{.Top}} Ven_Vendedor.id, Ven_Vendedor.cedula, Ven_Vendedor.codven, VEN.nombre_com, 
					   Ven_Vendedor.activo, Ven_Vendedor.last_modified, VMS.last_modified As last_modified_mds, Ven_Vendedor.deleted_at
  					   FROM {{.DBName}}Ven_Vendedor
					   LEFT JOIN {{.DBName}}Cnt_Terceros VEN ON VEN.CodTer = Ven_Vendedor.cedula
					   LEFT JOIN {{.DBName}}Ven_Vendedor_Meta_Sync VMS ON VMS.vendedor_id = ven_vendedor.id AND LTRIM(RTRIM(VMS.IMEI)) = '{{.Imei}}' 
					   WHERE LTRIM(RTRIM(Ven_Vendedor.cedula))<>'' {{.Filter}}
					   ORDER BY VEN.nombre_com
					   {{.Pagination}}`
	}

	// Setup template
	tmpL, err := template.New("sQuery_TMPL").Parse(sQuery_TMPL)
	if err != nil {
		fmt.Println(err.Error())
		return http.StatusInternalServerError, err
	}

	err = tmpL.Execute(&sQuery, substitute)
	if err != nil {
		fmt.Println(err.Error())
		return http.StatusInternalServerError, err
	}

	//fmt.Println(sQuery.String())

	// Obtengo la conexión a la base de datos
	db, ok := c.dbs[r.Header.Get("host_domain")]

	if ok {
		rows, err := db.Raw(sQuery.String()).Rows()

		if err != nil {
			fmt.Println(sQuery.String()) // Si hay error en la sentencia, quiero ver la sentencia
			return http.StatusInternalServerError, err
		}

		// Send records to object array
		for rows.Next() {
			err := rows.Scan(&seller.Id, &seller.Cedula, &seller.Codven, &seller.Nombre_Com, &seller.Activo, &seller.Last_Modified, &seller.Last_Modified_Mds, &seller.Deleted_At)

			if err != nil {
				fmt.Println(err.Error())
				return http.StatusInternalServerError, err
			}
			response.Sellers = append(response.Sellers, seller)
		}

		// Get pagination info Just If Necesary
		var counter uint64
		if strings.Trim(sPage_size, " ") != "" && strings.Trim(sPageNo, " ") != "" {
			sQuery_Counter = fmt.Sprintf(`SELECT Count(Ven_Vendedor.id) As Total 
											 FROM %vVen_Vendedor
											 LEFT JOIN %vVen_Vendedor_Meta_Sync TMS ON VMS.vendedor_id = Ven_vendedor.id AND LTRIM(RTRIM(VMS.IMEI)) = '%v' 
											 WHERE Ven_Vendedor.Cedula<>'' %v`, sDBPrefix.String(), sDBPrefix.String(), IMEI, sFilter_Query)

			rows, err = db.Raw(sQuery_Counter).Rows()
			if err != nil {
				fmt.Println(err.Error())
				return http.StatusInternalServerError, err
			}

			if rows.Next() {
				err = rows.Scan(&counter)
				if err != nil {
					fmt.Println(err.Error())
					return http.StatusInternalServerError, err
				}
			}
		}
		response.Pagination.Total = counter
		response.Pagination.Page_Size = page_size
		response.Pagination.Page_No = page_no
		json.NewEncoder(w).Encode(response)
	}

	return http.StatusOK, nil
}