package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	"net/http"
	"strings"
	"log"
	"time"
	"crypto/md5"
	"encoding/hex"
	"os"
)

var dbs map[string]*gorm.DB // Public Map to store db's instances
var err error

func main() {
	// Make a map to store databases instances
	dbs = make(map[string]*gorm.DB)
	r := mux.NewRouter()
	// Paths
	r.HandleFunc("/setup", setup).Methods("GET") // Setup client database instance
	r.HandleFunc("/login-check", loginCheck).Methods("GET") // Check user credentials
	r.HandleFunc("/profile-options", profile_options).Methods("GET") // Return user's profile options

	http.Handle("/", Middleware(r))

	port := os.Getenv("PORT")

	//log.Println(os.Environ()) //

	if port == "" {
		log.Fatal("$PORT must be set")
	}
	// Start Sever
	http.ListenAndServe(":" + port, nil)
}

func profile_options(writer http.ResponseWriter, request *http.Request) {

	host_domain, user_name := request.Header.Get("host_domain"), request.Header.Get("user_name")

	db, ok := dbs[host_domain]

	if ok {

		// Obtengo el nombre del usuario del cual se desea onbtener el perfil de opciones
		// vars := mux.Vars(request)

		// Prepare sentence
		sQuery := fmt.Sprintf(`
		SELECT DISTINCT Gen_Menu.Modulo As CodeModulo, Gen_Modulos.Nombre As NombreModulo,
		Gen_Modulos.Orden As OrdenModulo, Gen_Menu.OrdenGrupo, Gen_Menu.Grupo As NombreGrupo,
		Gen_Menu.Descripcion, Gen_Menu.Formulario
		FROM  Gen_Menu
		LEFT  JOIN Gen_Modulos ON Gen_Modulos.Modulo = Gen_Menu.Modulo
		INNER JOIN Cfg_DetaPerfil ON Cfg_DetaPerfil.Formulario = Gen_Menu.Formulario
		WHERE Cfg_DetaPerfil.Codper IN (SELECT CodPer FROM Cfg_PerfilxUsua WHERE Cfg_PerfilxUsua.Codusu = '%s')
		ORDER BY Gen_Modulos.Orden, Grupo, Descripcion`,user_name)

		rows, err := db.Raw(sQuery).Rows()

		if err != nil {
			panic(err)
			return
		}

		// options
		type Option struct {
			Description string `json:"description"`
			FormName string `json:"form_name"`
		}

		// Groups
		type Group struct {
			Description string `json:"description"`
			Order string `json:"order"`
			Options []Option `json:"options"`
		}

		// Module
		type Module struct {
			Description string `json:"description"`
			Code string `json:"code"`
			Groups []Group `json:"groups"`
		}

		//type Modules struct {
		//	Module []Module
		//}

		type Data struct {
			Modules []Module
		}

		type Response struct {
			Data Data
		}

		// Resultado de la sentencia
		type Query_Result struct {
			CodeModulo string
			NombreModulo string
			OrdenModulo string
			OrdenGrupo string
			NombreGrupo string
			Descripcion string
			Formulario string
		}

		var oResult Query_Result
		oResponse := &Response{} // Initialize response object
		oResponse.Data.Modules = make([]Module, 0) // Max 10 Modules


		for rows.Next() {

			rows.Scan(&oResult.CodeModulo,&oResult.NombreModulo,&oResult.OrdenModulo,&oResult.OrdenGrupo,&oResult.NombreGrupo,&oResult.Descripcion,&oResult.Formulario)

			// Search for Module
			var found bool = false
			for _, v := range oResponse.Data.Modules {
				if v.Code == oResult.CodeModulo {
					found = true
					break
				}
			}
			if !found {
				oResponse.Data.Modules = append(oResponse.Data.Modules, Module{oResult.NombreModulo, oResult.CodeModulo, nil})
				oResponse.Data.Modules[len(oResponse.Data.Modules)-1].Groups = make([]Group,0)
			}

			// Seek for module index
			index_module := -1
			for i, v := range oResponse.Data.Modules {
				if v.Code == oResult.CodeModulo {
					index_module = i
					break
				}
			}

			index_group := -1

			// Seek for Groups
			if index_module>=0 {

				for i, v := range oResponse.Data.Modules[index_module].Groups {
					if v.Description == oResult.NombreGrupo {
						index_group = i
						break
					}
				}

				if index_group<0 {
					oResponse.Data.Modules[index_module].Groups = append(oResponse.Data.Modules[index_module].Groups,
					Group{oResult.NombreGrupo, oResult.OrdenGrupo, nil})
					oResponse.Data.Modules[index_module].Groups[len(oResponse.Data.Modules[index_module].Groups)-1].Options = make([]Option,0)
				}

			}

			// Seek for Index Group
			for i, v := range oResponse.Data.Modules[index_module].Groups {
				if v.Description == oResult.NombreGrupo {
					index_group = i
					break
				}
			}

			// Add menu option
			oResponse.Data.Modules[index_module].Groups[index_group].Options = append(oResponse.Data.Modules[index_module].Groups[index_group].Options,
				Option{oResult.Descripcion, oResult.Formulario} )

		}

		json.NewEncoder(writer).Encode(oResponse)
	}

}


// Intercepta peticiones HTTP y prepara el entorno
func Middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// En cada petición se asegura de que la base de datos para esta empresa existe en el mapa
		setup(w, r)
		// Set Headers
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		//w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		//w.Header().Set("Access-Control-Allow-Headers",
		//	"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		// Procesa la petición
		h.ServeHTTP(w, r)
	})
}

// Inicializa la conexión con la base de datos
func setup(writer http.ResponseWriter, request *http.Request) {

	// Busco la LLave en el mapa para ver si existe
	_, ok := dbs[request.Header.Get("host_domain")]

	if !ok { // Si el dominio no está en el mapa

/*		type Result struct {
			Status string `json:"status"`
			ErrMsj string `json:"errMsj"`
			Domain string `json:"domain"`
		}*/

		// Obtengo todas las cabeceras del request
		host_domain := strings.ToLower(request.Header.Get("host_domain")) // me aseguro que este en minuscula
		host_user := request.Header.Get("host_user")
		host_pwd := request.Header.Get("host_pwd")
		host_database := request.Header.Get("host_database")
		host_ip := request.Header.Get("host_ip")
		host_port := request.Header.Get("host_port")

		// "sqlserver://sa:Qu4l1ty@190.248.137.122:1433?database=BD_COMERCIAL_ML")
		dbs[host_domain], err = gorm.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
			host_user, host_pwd, host_ip, host_port, host_database))
		if err != nil {
			//result := &Result{Domain: host_domain, Status: "error", ErrMsj: err.Error()}
			//json.NewEncoder(writer).Encode(result)
		} else {
			if err != nil {
				//result := &Result{Domain: host_domain, Status: "error", ErrMsj: err.Error()}
				//json.NewEncoder(writer).Encode(result)
			} else {
				//result := &Result{Domain: host_domain, Status: "success", ErrMsj: ""}
				//json.NewEncoder(writer).Encode(result)
			}
		}
	}
}

// Verifica las credenciales del usuario y retorna un objeto con las bases de datos a las que puede acceder
func loginCheck(writer http.ResponseWriter, request *http.Request) {

	var host_domain string = request.Header.Get("host_domain")

	// Obtengo el dominio que está realizando el request
	db, ok := dbs[host_domain]

	if ok {

		// Preparo la sentencia
		sQuery := fmt.Sprintf(`
		SELECT Gen_Databases.DataBaseName, Gen_Databases.DataBaseAlias, Gen_Databases.LastBackUp, Master.DBO.Cfg_Usuarios.PWD
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
		ORDER BY U.DataBaseName`, request.Header.Get("user_name"))

		// Obtengo la información de la base de datos
		rows, err := db.Raw(sQuery).Rows()

		if err != nil {
			log.Panic(err)
			return
		}

		//log.Println(time.Now().Local())
		//log.Println(time.Now().Date())
		//log.Print(count(rows))
		type DataBase struct {
			DataBaseName string
			DataBaseAlias string
			LastBackUp time.Time
			pwd	string // Campo no exportado por estar en minuscula, no va incluido en el .json, otra forma de omitir la exportación del campo es con el tag `json:"-"`
		}

		// Estructura de la respuesta
		type User_Profile struct {
			DataBases []DataBase `json:"databases"`
		}

		type Data struct {
			Logged bool `json:"logged"`
			User_Profile User_Profile `json:"user_profile"`
		}

		// Response
		type Response struct {
			Data Data `json:"data"`
		}

		oResponse := &Response{}
		oResponse.Data.User_Profile.DataBases = make([]DataBase,20) // Maximo 20 Bases de datos por servidor
		var result DataBase
		index:=0
		for ;rows.Next(); index++ {
			rows.Scan(&result.DataBaseName,&result.DataBaseAlias,&result.LastBackUp,&result.pwd)
			oResponse.Data.User_Profile.DataBases[index].DataBaseName = result.DataBaseName
			oResponse.Data.User_Profile.DataBases[index].DataBaseAlias = result.DataBaseAlias
			oResponse.Data.User_Profile.DataBases[index].LastBackUp = result.LastBackUp
			oResponse.Data.User_Profile.DataBases[index].pwd = result.pwd
		}

		// Esta logueado si la contraseña coincide y tiene por lo menos una base de datos asignada
		if user_pwd := strings.ToUpper(GetMD5Hash(request.Header.Get("user_pwd"))); user_pwd == result.pwd && index>0 {
			oResponse.Data.Logged = true
		} else {
			oResponse.Data.Logged = false
		}

		// Envíar respuesta
		oResponse.Data.User_Profile.DataBases = oResponse.Data.User_Profile.DataBases[0:index] // Redimensiono el slice
		json.NewEncoder(writer).Encode(oResponse)

	}
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
