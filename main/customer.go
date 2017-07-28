package main

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"
)

type customer struct {
	CodCli         string    `json:"codcli"`
	Cedula         string    `json:"cedula"`
	Nombre_1       string    `json:"nombre_1"`
	Nombre_2       string    `json:"nombre_2"`
	Apellido_1     string    `json:"apellido_1"`
	Apellido_2     string    `json:"apellido_2"`
	Nombre_Com     string    `json:"nombre_com"`
	Nombre_Bus     string    `json:"nombre_bus"`
	Nombre_Cal     string    `json:"nombre_cal"`
	Telefono_1     string    `json:"telefono_1"`
	Telefono_2     string    `json:"telefono_2"`
	Celular_1      string    `json:"celular_1"`
	Celular_2      string    `json:"celular_2"`
	TELS           string    `json:"tels"`
	Direccion      string    `json:"direccion"`
	Regimen        string    `json:"regimen"`
	EMail          string    `json:"email"`
	RegistraFecNac bool      `json:"registra_fec_nac"`
	FecNac         time.Time `json:"fecnac"`
	CodMcpio       string    `json:"codmcpio"`
	CodDpto        string    `json:"coddpto"`
	TipCap         int8      `json:"tipcap"`
	TipID          string    `json:"tipid"`
	CodList        string    `json:"codlist"`
	FechaRegistro  time.Time `json:"fecha_registro"`
	MargenReteICA  float32   `json:"margen_rete_ica"`
	RetAnyBase     bool      `json:"ret_any_base"`
	CodVen         string    `json:"codven"`
	CodZona        string    `json:"codzona"`
	PlazoCR        int8      `json:"plazo_cr"`
	ExentoIVA      bool      `json:"exento_iva"`
	Activo         bool      `json:"activo"`
	MotivoBloqueo  string    `json:"motivo_bloqueo"`
	CodNeg         string    `json:"codneg"`
}

type customers struct {
	customers []customer
}

func GetCustomers(c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	var sQuery_TMPL string
	var sQuery, sDBPrefix bytes.Buffer
	var sFilter_Query, sFilter_Pagination string = "", ""
	var cust customer

	custmrs := &customers{}
	custmrs.customers = make([]customer, 0)

	// Valid End-Points
	// /customers -> All Customers
	// /customers/90 -> Customer Id = 90
	// /customers?pagen_no=1&page_size=50 -> Customer's selection from 1 to 50
	// /customers?pagen_no=2&page_size=50 -> Customer's selection from 51 to 100
	// /customers?pagen_no=1&page_size=50&filter=CARLOS -> Customer's selection from 1 to 50 Where customer's name
														  // contains "CARLOS" string.

	vars := mux.Vars(r)
	query := r.URL.Query()

	sId, sFilter, sPage_size, sOffset, sPage_No, host_database := vars["id"], query.Get("filter"), query.Get("page_size"),
		query.Get("offset"), query.Get("page_no"), r.Header.Get("host_database") // Check if Id is provided

	// Low Cost concatenation process
	sDBPrefix.WriteString(host_database)
	sDBPrefix.WriteString(".DBO.")

	// Query Values
	type query_values struct {
		Id        int
		Page_Size int
		OffSet    int
		DBName    string
		Filter    string
		Pagination string
	}

	// Casting
	id, _ := strconv.Atoi(sId)
	page_size, _ := strconv.Atoi(sPage_size)
	offset, _ := strconv.Atoi(sOffset)
	page_no, _ := strconv.Atoi(sPage_No)

	// Set Filter
	if strings.Trim(sFilter, " ") != "" {
		sFilter_Query = "AND CLI.Nombre_Com LIKE '%" + strings.Replace(sFilter, " ", "%", -1) + "%'"
	}

	// OffSet, Page_Size and Page No.
	if strings.Trim(sPage_size, " ") != "" && strings.Trim(sPage_No, " ") != "" {
		sFilter_Pagination = "OFFSET " + strconv.Itoa((page_no-1)*page_size) + " ROWS \n" +
							 "FETCH NEXT " + strconv.Itoa(page_size) + " ROWS ONLY"
	}

	// Fill Values
	substitute := query_values{Id: id, Page_Size: page_size, OffSet: offset, DBName: sDBPrefix.String(), Filter: sFilter_Query, Pagination:sFilter_Pagination}

	// Query selection
	switch {
	case strings.Trim(sId, " ") != "": // If a customer id is provided
		sQuery_TMPL = `
				SELECT Ven_Clientes.Cedula, Ven_Clientes.CodCli, CLI.Nombre_1, CLI.Nombre_2, CLI.Apellido_1, CLI.Apellido_2,
				CLI.NOMBRE_COM, CLI.NOMBRE_CAL, CLI.NOMBRE_BUS,
				CLI.Telefono_1, CLI.Telefono_2, CLI.Celular_1, CLI.Celular_2, CLI.TELS, CLI.Direccion, CLI.Regimen, CLI.EMail,
				CLI.RegistraFecNac, ISNULL(CLI.FecNac,GetDate()) As FecNac, CLI.CodMcpio, CLI.CodDpto, CLI.TipCap, CLI.TipID, LTRIM(RTRIM(Ven_Clientes.CODLIST)) As CodList,
				ISNULL(CLI.FechaRegistro,GetDate()) As FechaRegistro, Ven_Clientes.MARGENRETEICA, Ven_Clientes.RETANYBASE, Ven_Clientes.CodVen, Ven_Clientes.CodZona,
				Ven_Clientes.PlazoCR, Ven_Clientes.ExentoIVA, Ven_Clientes.Activo, Ven_Clientes.MotivoBloqueo, Ven_Clientes.CodNeg
				FROM {{.DBName}}Ven_Clientes
				LEFT JOIN {{.DBName}}Cnt_Terceros CLI ON CLI.CodTer = Ven_Clientes.Cedula
				WHERE Ven_Clientes.Id={{.Id}}
				ORDER BY CLI.Nombre_Com`
	default:
		sQuery_TMPL = `
				SELECT Ven_Clientes.Cedula, Ven_Clientes.CodCli, CLI.Nombre_1, CLI.Nombre_2, CLI.Apellido_1, CLI.Apellido_2,
				CLI.NOMBRE_COM, CLI.NOMBRE_CAL, CLI.NOMBRE_BUS,
				CLI.Telefono_1, CLI.Telefono_2, CLI.Celular_1, CLI.Celular_2, CLI.TELS, CLI.Direccion, CLI.Regimen, CLI.EMail,
				CLI.RegistraFecNac, ISNULL(CLI.FecNac,GetDate()) As FecNac, CLI.CodMcpio, CLI.CodDpto, CLI.TipCap, CLI.TipID, LTRIM(RTRIM(Ven_Clientes.CODLIST)) As CodList,
				ISNULL(CLI.FechaRegistro,GetDate()) As FechaRegistro, Ven_Clientes.MARGENRETEICA, Ven_Clientes.RETANYBASE, Ven_Clientes.CodVen, Ven_Clientes.CodZona,
				Ven_Clientes.PlazoCR, Ven_Clientes.ExentoIVA, Ven_Clientes.Activo, Ven_Clientes.MotivoBloqueo, Ven_Clientes.CodNeg
				FROM {{.DBName}}Ven_Clientes
				LEFT JOIN {{.DBName}}Cnt_Terceros CLI ON CLI.CodTer = Ven_Clientes.Cedula
				WHERE LTRIM(RTRIM(CLI.CodTer))<>'' {{.Filter}}
				ORDER BY CLI.Nombre_Com
				{{.Pagination}}
				`
	}

	// Setup template
	tmpL, err := template.New("sQuery_TMPL").Parse(sQuery_TMPL)
	if err != nil {
		panic(err)
		return http.StatusInternalServerError, err
	}

	err = tmpL.Execute(&sQuery, substitute)
	if err != nil {
		panic(err)
		return http.StatusInternalServerError, err
	}

	// Obtengo la conexión a la base de datos
	db, ok := c.dbs[r.Header.Get("host_domain")]

	if ok {

		rows, err := db.Raw(sQuery.String()).Rows()

		if err != nil {
			panic(err)
			return http.StatusInternalServerError, err
		}

		for rows.Next() {
			err := rows.Scan(&cust.CodCli, &cust.Cedula, &cust.Nombre_1, &cust.Nombre_2, &cust.Apellido_1, &cust.Apellido_2,
				&cust.Nombre_Com, &cust.Nombre_Bus, &cust.Nombre_Cal, &cust.Telefono_1, &cust.Telefono_2,
				&cust.Celular_1, &cust.Celular_2, &cust.TELS, &cust.Direccion, &cust.Regimen, &cust.EMail,
				&cust.RegistraFecNac, &cust.FecNac, &cust.CodMcpio, &cust.CodDpto, &cust.TipCap, &cust.TipID,
				&cust.CodList, &cust.FechaRegistro, &cust.MargenReteICA, &cust.RetAnyBase, &cust.CodVen,
				&cust.CodZona, &cust.PlazoCR, &cust.ExentoIVA, &cust.Activo, &cust.MotivoBloqueo, &cust.CodNeg)

			if err != nil {
				panic(err)
				return http.StatusInternalServerError, err
			}
			custmrs.customers = append(custmrs.customers, cust)
		}
		json.NewEncoder(w).Encode(custmrs.customers)
	}
	return http.StatusOK, nil
}
