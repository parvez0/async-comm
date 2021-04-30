package asynccommtest

import (
	"async-comm/internal/app/asynccommtest/config"
	"sync"
)

func (a *App) InitiateConsumers(r config.Routine, app *App, wg *sync.WaitGroup)  {
	defer wg.Done()
	exits, err := app.Rdb.GrpExits(r.Q, r.Group)
	if err != nil {
		a.log.Errorf("stream '%s' doesn't exits - error : %s", r.Q, err.Error())
		return
	}
	if !exits {
		err = app.Rdb.CreateGrp(r.Q, r.Group)
		if err != nil {
			a.log.Errorf("failed to create group '%s' - error : %s", r.Group, err.Error())
			return
		}
	}
}