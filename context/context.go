package context

import (
	"github.com/jinzhu/gorm"
	"fmt"
)

type AppContext struct {
	DBS map[string]*gorm.DB
}

var ctx *AppContext

func GetContext() *AppContext {
	//if ctx == nil {
		ctx.DBS = make(map[string]*gorm.DB)
	//}
	return ctx
}

func (c AppContext) SetHost(host_domain, host_user, host_pwd, host_ip, host_port string) error {
	var err error
	GetContext().DBS[host_domain], err = gorm.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
		host_user, host_pwd, host_ip, host_port, "Master"))
		return err
}