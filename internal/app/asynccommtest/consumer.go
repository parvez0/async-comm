package asynccommtest

import (
	"async-comm/internal/app/asynccommtest/config"
	"sync"
)

func (a *App) InitiateConsumers(r config.Routine, app *App, wg *sync.WaitGroup)  {
	defer wg.Done()
	for {
		// TODO exit if consumer group is not present
	}
}