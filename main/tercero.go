package main

import (
	"net/http"
	"bytes"
	"github.com/gorilla/mux"
	"strconv"
	"strings"
	"fmt"
	"encoding/json"
	"text/template"
	"time"
)

type Tercero struct {
	Id				string 		`json:"id"`
	CodTer			string 		`json:"codter"`
	Nombre_1		string 		`json:"nombre_1"`
	Nombre_2		string 		`json:"nombre_2"`
	Apellido_1		string 		`json:"apellido_1"`
	Apellido_2		string 		`json:"apellido_2"`
	Telefono_1		string 		`json:"telefono_1"`
	Telefono_2		string 		`json:"telefono_2"`
	Celular_1		string 		`json:"celular_1"`
	Celular_2		string 		`json:"celular_2"`
	Direccion 		string 		`json:"direccion"`
	Regimen			string 		`json:"regimen"`
	Email			string 		`json:"email"`
	Registra_FecNac	string 		`json:"registra_fecnac"`
	FecNac		   *time.Time	`json:"fecnac"`
	Nombre_Com		string 		`json:"nombre_com"`
	TipId			string 		`json:"tipid"`
	CodDpto			string 		`json:"coddpto"`
	CodMcpio		string 		`json:"codmcpio"`
	TipCap			string 		`json:"tipcap"`
	Tels			string 		`json:"tels"`
	Nombre_Cal		string 		`json:"nombre_cal"`
	Nombre_Bus		string 		`json:"nombre_bus"`
	Fecha_Registro *time.Time 	`json:"fecha_registro"`
	CodPais			string 		`json:"codpais"`
	Last_Modified	time.Time 	`json:"last_modified"`
	Deleted_At	   *time.Time 	`json:"deleted_at"`
}

type Response struct {
	 Pagination pagination 	`json:"pagination"`
	 Terceros []Tercero 	`json:"data"`
}

func (Tercero) TableName() string  {
	return DATABASE_NAME + ".dbo.cnt_terceros"
}

func GetTerceros(c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	var sQuery_TMPL, sQuery_Counter string
	var sQuery, sDBPrefix bytes.Buffer
	var sFilter_Query, sFilter_Pagination string = "", ""
	var tercero Tercero
	var for_sync bool
	var sTop_Criteria string = ""

	response := &Response{}
	response.Terceros = make([]Tercero, 0)

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
	// OR Ven_Clientes.Deleted_At IS NOT NULL
	if for_sync {
		sFilter_Query = "AND (Cnt_Terceros.Last_Modified<>TMS.Last_Modified OR TMS.Last_Modified IS NULL) "
		sTop_Criteria = "TOP (50) " // Sync data in 5 records chuncks per request
		if strings.Trim(IMEI, " ") == "" {
			fmt.Println("For_Sync ha sido llamado sin IMEI !")
			return http.StatusInternalServerError, nil
		}
	}

	// Set Filter
	if strings.Trim(sFilter, " ") != "" {
		sFilter_Query = sFilter_Query + "AND Cnt_Terceros.Nombre_Com LIKE '%" + strings.Replace(sFilter, " ", "%", -1) + "%'"
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
		sQuery_TMPL = `SELECT Cnt_Terceros.id, Cnt_Terceros.codter, Cnt_Terceros.nombre_1, Cnt_Terceros.nombre_2, Cnt_Terceros.apellido_1, Cnt_Terceros.apellido_2,
   				       Cnt_Terceros.telefono_1, Cnt_Terceros.telefono_2, Cnt_Terceros.celular_1, Cnt_Terceros.celular_2, Cnt_Terceros.direccion, Cnt_Terceros.regimen,
					   Cnt_Terceros.email, Cnt_Terceros.registrafecnac, Cnt_Terceros.fecnac, Cnt_Terceros.nombre_com, Cnt_Terceros.tipid, Cnt_Terceros.coddpto,
					   Cnt_Terceros.codmcpio, Cnt_Terceros.tipcap, Cnt_Terceros.tels, nombre_cal, Cnt_Terceros.nombre_bus, Cnt_Terceros.fecharegistro,
					   Cnt_Terceros.codpais, Cnt_Terceros.last_modified, Cnt_Terceros.deleted_at  
					   FROM {{.DBName}}Cnt_Terceros
					   WHERE Cnt_Terceros.id={{.Id}}
					   ORDER BY Cnt_Terceros.Nombre_Com`

	default:
		sQuery_TMPL = `SELECT {{.Top}} Cnt_Terceros.id, Cnt_Terceros.codter, Cnt_Terceros.nombre_1, Cnt_Terceros.nombre_2, Cnt_Terceros.apellido_1, Cnt_Terceros.apellido_2,
   				       Cnt_Terceros.telefono_1, Cnt_Terceros.telefono_2, Cnt_Terceros.celular_1, Cnt_Terceros.celular_2, Cnt_Terceros.direccion, Cnt_Terceros.regimen,
					   Cnt_Terceros.email, Cnt_Terceros.registrafecnac, Cnt_Terceros.fecnac, Cnt_Terceros.nombre_com, Cnt_Terceros.tipid, Cnt_Terceros.coddpto,
					   Cnt_Terceros.codmcpio, Cnt_Terceros.tipcap, Cnt_Terceros.tels, nombre_cal, Cnt_Terceros.nombre_bus, Cnt_Terceros.fecharegistro,
					   Cnt_Terceros.codpais, Cnt_Terceros.last_modified, Cnt_Terceros.deleted_at  
					   FROM {{.DBName}}Cnt_Terceros 
				       LEFT JOIN {{.DBName}}Cnt_Terceros_Meta_Sync TMS ON TMS.tercero_id = Cnt_Terceros.id AND LTRIM(RTRIM(TMS.IMEI)) = '{{.Imei}}' 
					   WHERE LTRIM(RTRIM(Cnt_Terceros.CodTer))<>'' {{.Filter}}
					   ORDER BY Cnt_Terceros.Nombre_Com
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
			err := rows.Scan(&tercero.Id, &tercero.CodTer, &tercero.Nombre_1, &tercero.Nombre_2, &tercero.Apellido_1, &tercero.Apellido_2, &tercero.Telefono_1, &tercero.Telefono_2,
				&tercero.Celular_1, &tercero.Celular_2, &tercero.Direccion, &tercero.Regimen, &tercero.Email, &tercero.Registra_FecNac, &tercero.FecNac, &tercero.Nombre_Com,
				&tercero.TipId, &tercero.CodDpto, &tercero.CodMcpio, &tercero.TipCap, &tercero.Tels, &tercero.Nombre_Cal, &tercero.Nombre_Bus, &tercero.Fecha_Registro,
				&tercero.CodPais, &tercero.Last_Modified, &tercero.Deleted_At)

			if err != nil {
				fmt.Println(err.Error())
				return http.StatusInternalServerError, err
			}
			response.Terceros = append(response.Terceros, tercero)
		}

		// Get pagination info
		sQuery_Counter = fmt.Sprintf(`SELECT Count(Cnt_Terceros.id) As Total 
											 FROM %vCnt_Terceros
											 LEFT JOIN %vCnt_Terceros_Meta_Sync TMS ON TMS.tercero_id = Cnt_Terceros.id AND LTRIM(RTRIM(TMS.IMEI)) = '%v' 
											 WHERE Cnt_Terceros.CodTer<>'' %v`, sDBPrefix.String(), sDBPrefix.String(), IMEI, sFilter_Query)

		rows, err = db.Raw(sQuery_Counter).Rows()
		if err != nil {
			fmt.Println(err.Error())
			return http.StatusInternalServerError, err
		}

		var counter uint64
		if rows.Next() {
			err = rows.Scan(&counter)
			if err != nil {
				fmt.Println(err.Error())
				return http.StatusInternalServerError, err
			}
		}

		response.Pagination.Total = counter
		response.Pagination.Page_Size = page_size
		response.Pagination.Page_No = page_no
		json.NewEncoder(w).Encode(response)
	}
	return http.StatusOK, nil
}
