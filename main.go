package main

import (
	"async-comm/pkg"
)

var cnf *pkg.Config
var log pkg.Logger

func init() {
	cnf = pkg.InitializeConfig()
	log = pkg.InitializeLogger()
}

func main() {
	log.Infof("starting service %s", cnf.Server.App)
	for _, r := range cnf.Server.Routines {
		switch r.Role {
		case "producer":
			pMsg, err := pkg.ParseTemplate(r, cnf.Server.App)
			if err != nil {
				log.Errorf("failed to parse message '%s' - %s", r.Message.Format, err.Error())
				continue
			}
			log.Info("Published message : ", pMsg)
		}
	}
}
