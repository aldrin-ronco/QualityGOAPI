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

var dbs map[string]*gorm.DB // Map publico para almacenar las bases de datos
var err error

func main() {
	// Crear el mapa para almacenar las bases de datos
	dbs = make(map[string]*gorm.DB)
	r := mux.NewRouter()
	// Rutas
	r.HandleFunc("/setup", setup).Methods("GET")
	r.HandleFunc("/login-check", loginCheck).Methods("GET")

	http.Handle("/", Middleware(r))

	port := os.Getenv("PORT")

	//log.Println(os.Environ()) //

	if port == "" {
		log.Fatal("$PORT must be set")
	}
	// Iniciar Servidor
	http.ListenAndServe(":" + port, nil)
}

// Intercepta peticiones HTTP y prepara el entorno
func Middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// En cada petición se asegura de que la base de datos para esta empresa existe en el mapa
		setup(w, r)
		// Set Headers
		w.Header().Set("Content-Type", "application/json")
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
			Pwd	string
		}

		// Estructura de la respuesta
		type User_profile struct {
			DataBases []DataBase
		}

		type Data struct {
			Logged bool
			User_profile User_profile
		}

		// Response
		type Response struct {
			Data Data
		}

		// DB Result
		type Result struct {
			DataBaseName string
			DataBaseAlias string
			LastBackUp time.Time
			Pwd	string
		}

		oResponse := &Response{}
		oResponse.Data.User_profile.DataBases = make([]DataBase,20) // Maximo 20 Bases de datos por servidor
		var result Result
		index:=0
		for ;rows.Next(); index++ {
			rows.Scan(&result.DataBaseName,&result.DataBaseAlias,&result.LastBackUp,&result.Pwd)
			oResponse.Data.User_profile.DataBases[index].DataBaseName = result.DataBaseName
			oResponse.Data.User_profile.DataBases[index].DataBaseAlias = result.DataBaseAlias
			oResponse.Data.User_profile.DataBases[index].LastBackUp = result.LastBackUp
			oResponse.Data.User_profile.DataBases[index].Pwd = result.Pwd
		}

		// Esta logueado si la contraseña coincide y tiene por lo menos una base de datos asignada
		if user_pwd := strings.ToUpper(GetMD5Hash(request.Header.Get("user_pwd"))); user_pwd == result.Pwd && index>0 {
			oResponse.Data.Logged = true
		} else {
			oResponse.Data.Logged = false
		}

		// Envíar respuesta
		oResponse.Data.User_profile.DataBases = oResponse.Data.User_profile.DataBases[0:index] // Redimensiono el slice
		json.NewEncoder(writer).Encode(oResponse)

	}
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}