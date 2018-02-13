package main

// PARA EJECUTAR : GO BUILD
// UBICARSE EN C:\projects\go\src\github.com\aldrin-ronco\QualityGOAPI\main>

// PARA EJECUTAR : GODEP SAVE
// UBICARSE EN C:\projects\go\src\github.com\aldrin-ronco\QualityGOAPI\main>


import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	"github.com/urfave/negroni"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"html/template"
	"bytes"
)


// Context app
type appContext struct {
	dbs map[string]*gorm.DB
}

// App Handler
type appHandler struct {
	*appContext
	h func(c *appContext, w http.ResponseWriter, r *http.Request) (int, error)
}

// Global Pagination struct
type pagination struct {
	Total     uint64 `json:"total"`
	Page_Size int    `json:"page_size"`
	Page_No   int    `json:"page_no"`
}

// Server HTTP
func (ah appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Updated to pass ah.appContext as a parameter to our handler type.
	status, err := ah.h(ah.appContext, w, r)
	if err != nil {
		log.Printf("HTTP %d: %q", status, err)
		switch status {
		case http.StatusNotFound:
			http.NotFound(w, r)
			// And if we wanted a friendlier error page:
			// err := ah.renderTemplate(w, "http_404.tmpl", nil)
		case http.StatusInternalServerError:
			http.Error(w, http.StatusText(status), status)
		default:
			http.Error(w, http.StatusText(status), status)
		}
	}
}

// Globals
var err error
var ctx *appContext
var DATABASE_NAME string

// Entry POINT
func main() {

	// Initialize context
	ctx = &appContext{dbs: make(map[string]*gorm.DB)}
	// ctx := context.GetContext()

	r := mux.NewRouter()
	// Paths
	r.Handle("/login-check", appHandler{ctx, loginCheck}).Methods("GET", "OPTIONS") // Check user credentials
	r.Handle("/profile-options", appHandler{ctx, profile_options}).Methods("GET", "OPTIONS")     // Return user's profile options

	// Customers
	r.Handle("/customers", appHandler{ctx, GetCustomers}).Methods("GET", "OPTIONS")
	r.Handle("/customers", appHandler{ctx, PostCustomers}).Methods("POST")
	r.Handle("/customers/{id}", appHandler{ctx,PutCustomers}).Methods("PUT")
	r.Handle("/customers/{id}", appHandler{ctx, GetCustomers}).Methods("GET", "OPTIONS")
	r.Handle("/customers/{id}", appHandler{ctx, DeleteCustomer}).Methods("DELETE")

	// Customers Meta Data Sync
	r.Handle("/customers_mds", appHandler{ctx, PostCustomerMetaDataSync}).Methods("POST")
	r.Handle("/customers_mds", appHandler{ctx, PutCustomerMetaDataSync}).Methods("PUT")

	// Tercero
	r.Handle("/others", appHandler{ctx, GetTerceros}).Methods("GET", "OPTIONS")
	r.Handle("/others/{id}", appHandler{ctx, GetTerceros}).Methods("GET", "OPTIONS")

	// Tercero Meta Data Sync
	r.Handle("/others_mds", appHandler{ctx, PostTerceroMetaDataSync}).Methods("POST")
	r.Handle("/others_mds", appHandler{ctx, PutTerceroMetaDataSync}).Methods("PUT")

	n := negroni.Classic()

	// Middleware que se encarga de setear la base de datos
	n.Use(negroni.HandlerFunc(setup))

	n.UseHandler(r)

	port := os.Getenv("PORT")

	if port == "" {
		log.Fatal("$PORT must be set")
	}

	if host, _ := os.Hostname(); host == "QUALITYPC_AR" {
		port = "9090"
	}
	// Cors
	log.Println("Servidor escuchando en puerto", port)
	// Start Sever
	http.ListenAndServe(":"+port, n)
}

// Return Object with user profile options
func profile_options(c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	// For Interpolation String
	var sQuery, sDBPrefix bytes.Buffer

	host_domain, user_name, host_database := strings.ToLower(r.Header.Get("host_domain")), r.Header.Get("user_name"), r.Header.Get("host_database")

	if strings.Trim(host_database, " ") == "" {
		panic(err)
		return http.StatusInternalServerError, err
	}

	// Low Cost concatenation process
	sDBPrefix.WriteString(host_database)
	sDBPrefix.WriteString(".DBO.")

	db, ok := c.dbs[host_domain]

	// Substitution fields
	type query_values struct {
		DBName string
		UserName string
	}

	// Substitution values
	subsitute := query_values{DBName:sDBPrefix.String(), UserName:user_name}

	if ok {

		// Obtengo el nombre del usuario del cual se desea onbtener el perfil de opciones
		// vars := mux.Vars(request)

		// Prepare sentence
		sQuery_TMPL := `
		SELECT DISTINCT Gen_Menu.Modulo As CodeModulo, Gen_Modulos.Nombre As NombreModulo,
		Gen_Modulos.Orden As OrdenModulo, Gen_Menu.OrdenGrupo, Gen_Menu.OrdenItem, Gen_Menu.Grupo As NombreGrupo,
		Gen_Menu.Descripcion, Gen_Menu.Formulario
		FROM  {{.DBName}}Gen_Menu
		LEFT  JOIN {{.DBName}}Gen_Modulos 	 ON Gen_Modulos.Modulo = Gen_Menu.Modulo
		INNER JOIN {{.DBName}}Cfg_DetaPerfil ON Cfg_DetaPerfil.Formulario = Gen_Menu.Formulario
		WHERE Cfg_DetaPerfil.Codper IN (SELECT CodPer FROM {{.DBName}}Cfg_PerfilxUsua WHERE Cfg_PerfilxUsua.Codusu = '{{.UserName}}')
		ORDER BY Gen_Modulos.Orden, Gen_Menu.OrdenGrupo, Gen_Menu.OrdenItem`

		tmpl, err := template.New("sQuery_TMPL").Parse(sQuery_TMPL)

		if err != nil {
			panic(err)
			return http.StatusInternalServerError, err
		}

		err = tmpl.Execute(&sQuery, subsitute)

		// sQuery.String get's interpolated string template
		rows, err := db.Raw(sQuery.String()).Rows()

		if err != nil {
			panic(err)
			return http.StatusInternalServerError, err
		}

		// options
		type Option struct {
			Description string `json:"description"`
			FormName    string `json:"form_name"`
		}

		// Groups
		type Group struct {
			Description string   `json:"description"`
			Order       string   `json:"order"`
			Options     []Option `json:"options"`
		}

		// Module
		type Module struct {
			Description string  `json:"description"`
			Code        string  `json:"code"`
			Groups      []Group `json:"groups"`
		}

		type Response struct {
			Modules []Module `json:"modules"`
		}

		// Resultado de la sentencia
		type Query_Result struct {
			CodeModulo   string
			NombreModulo string
			OrdenModulo  string
			OrdenGrupo   string
			OrdenItem    int
			NombreGrupo  string
			Descripcion  string
			Formulario   string
		}

		var oResult Query_Result
		oResponse := &Response{} // Initialize response object
		oResponse.Modules = make([]Module, 0)

		for rows.Next() {

			if err := rows.Scan(&oResult.CodeModulo, &oResult.NombreModulo, &oResult.OrdenModulo, &oResult.OrdenGrupo, &oResult.OrdenItem, &oResult.NombreGrupo, &oResult.Descripcion, &oResult.Formulario); err != nil {
				log.Fatal(err)
			}

			// Search for Module
			var found bool = false
			for _, v := range oResponse.Modules {
				if v.Code == oResult.CodeModulo {
					found = true
					break
				}
			}
			if !found {
				oResponse.Modules = append(oResponse.Modules, Module{oResult.NombreModulo, oResult.CodeModulo, nil})
				oResponse.Modules[len(oResponse.Modules)-1].Groups = make([]Group, 0)
			}

			// Seek for module index
			index_module := -1
			for i, v := range oResponse.Modules {
				if v.Code == oResult.CodeModulo {
					index_module = i
					break
				}
			}

			index_group := -1

			// Seek for Groups
			if index_module >= 0 {

				for i, v := range oResponse.Modules[index_module].Groups {
					if v.Description == oResult.NombreGrupo {
						index_group = i
						break
					}
				}

				if index_group < 0 {
					oResponse.Modules[index_module].Groups = append(oResponse.Modules[index_module].Groups,
						Group{oResult.NombreGrupo, oResult.OrdenGrupo, nil})
					oResponse.Modules[index_module].Groups[len(oResponse.Modules[index_module].Groups)-1].Options = make([]Option, 0)
				}

			}

			// Seek for Index Group
			for i, v := range oResponse.Modules[index_module].Groups {
				if v.Description == oResult.NombreGrupo {
					index_group = i
					break
				}
			}

			// Add menu option
			oResponse.Modules[index_module].Groups[index_group].Options = append(oResponse.Modules[index_module].Groups[index_group].Options,
				Option{oResult.Descripcion, oResult.Formulario})

		}

		json.NewEncoder(w).Encode(oResponse)
	}
	return http.StatusOK, nil
}

// Setup ORM Instance (Negroni Middleware)
func setup(writer http.ResponseWriter, request *http.Request, next http.HandlerFunc) {

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization, host_user, host_pwd, host_id, host_database, host_ip, host_port, user_name, user_pwd, host_domain")

	switch request.Method {
	case "OPTIONS":
		writer.WriteHeader(http.StatusOK)
		return
	case "GET":
	default:
	}

	host_domain := strings.ToLower(request.Header.Get("host_domain")) // me aseguro que este en minuscula

	// Check for domain before continue
	if strings.Trim(host_domain,"  ") == "" {
		log.Println("El dominimo no puede estar vacío !")
		return
	}

	_, ok := ctx.dbs[host_domain]

	if !ok { // If the domain isn't present, add it.

		host_user := request.Header.Get("host_user")
		host_pwd := request.Header.Get("host_pwd")
		host_ip := request.Header.Get("host_ip")
		host_port := request.Header.Get("host_port")

		// The connection is ever made to Master database
		ctx.dbs[host_domain], err = gorm.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
		host_user, host_pwd, host_ip, host_port, "Master"))
		ctx.dbs[host_domain].LogMode(true)
		// Error check
		if err != nil {
			log.Println("Setup - >", err.Error())
			return
		} else {
			log.Println(fmt.Sprintf("Setup -> Se ha registrado el dominio %s con la base de datos %s", host_domain, "Master"))
		}
	}

	next(writer, request)
}

// Check user credentials and return a list of databases assigned to the user.
func loginCheck(c *appContext, w http.ResponseWriter, r *http.Request) (int, error) {

	// Get domain
	host_domain := r.Header.Get("host_domain")

	// Search in Map
	db, ok := c.dbs[host_domain]

	// If Found !
	if ok {

		// Preparo la sentencia
		sQuery := fmt.Sprintf(`
		SELECT Gen_Databases.Id, Gen_Databases.DataBaseName, Gen_Databases.DataBaseAlias, Gen_Databases.LastBackUp, Master.DBO.Cfg_Usuarios.PWD
		FROM Master.dbo.Cfg_Usuarios
		INNER JOIN  (
			SELECT Master.dbo.Cfg_Usuarios.UserName, Gen_DataBases.DataBaseName
			FROM Master.dbo.Cfg_Usuarios
			CROSS JOIN Master.dbo.Gen_DataBases
			LEFT JOIN Master.dbo.Cfg_UsuariosxEmp ON Master.dbo.Cfg_UsuariosxEmp.CodUsu = Master.dbo.Cfg_Usuarios.UserName AND
													Master.dbo.Cfg_UsuariosxEmp.DataBaseName = Master.dbo.Gen_DataBases.DataBaseName
			WHERE Master.DBO.Cfg_Usuarios.UserName = '%s' AND Master.dbo.Cfg_Usuarios.Activo = 1 AND Master.dbo.Cfg_UsuariosxEmp.DataBaseName IS NULL
		) U ON U.UserName = Cfg_Usuarios.UserName
		LEFT JOIN  Master.dbo.Gen_DataBases ON Gen_DataBases.DataBaseName = U.DataBaseName
		ORDER BY U.DataBaseName`, r.Header.Get("user_name"))

		// Execute RAW Query
		rows, err := db.Raw(sQuery).Rows()

		if err != nil {
			log.Panic(err)
			return http.StatusInternalServerError, err
		}

		type DataBase struct {
			Id			  int `json:"id"`
			DatabaseName  string `json:"database_name"`
			DatabaseAlias string `json:"database_alias"`
			LastBackup    time.Time `json:"last_backup"`
			pwd           string // Campo no exportado por estar en minuscula, no va incluido en el .json, otra forma de omitir la exportación del campo es con el tag `json:"-"`
		}

		type User_Profile struct {
			DataBases []DataBase `json:"databases"`
		}

		type Response struct {
			Logged       bool         `json:"logged"`
			User_Profile User_Profile `json:"user_profile"`
		}

		oResponse := &Response{}
		oResponse.User_Profile.DataBases = make([]DataBase, 20) // Maximo 20 Bases de datos por servidor
		var result DataBase
		index := 0
		for ; rows.Next(); index++ {
			rows.Scan(&result.Id, &result.DatabaseName, &result.DatabaseAlias, &result.LastBackup, &result.pwd)
			oResponse.User_Profile.DataBases[index].Id = result.Id
			oResponse.User_Profile.DataBases[index].DatabaseName = result.DatabaseName
			oResponse.User_Profile.DataBases[index].DatabaseAlias = result.DatabaseAlias
			oResponse.User_Profile.DataBases[index].LastBackup = result.LastBackup
			oResponse.User_Profile.DataBases[index].pwd = result.pwd
		}

		// ----------------------------------------------------------------
		// User Is Logged When :
		// 1. The Pwd match with DataBase Pwd
		// 2. Has at least one DataBase assigned
		// ----------------------------------------------------------------
		if user_pwd := strings.ToUpper(GetMD5Hash(r.Header.Get("user_pwd"))); user_pwd == result.pwd && index > 0 {
			oResponse.Logged = true
		} else {
			oResponse.Logged = false
		}

		// Send Response
		oResponse.User_Profile.DataBases = oResponse.User_Profile.DataBases[0:index] // Redimensiono el slice
		json.NewEncoder(w).Encode(oResponse)
	} else {
		log.Println("No ha sido encontrado el domino",host_domain)
		return http.StatusNotFound, nil
	}
	return http.StatusOK, nil
}

// MD5 Encoding
func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
