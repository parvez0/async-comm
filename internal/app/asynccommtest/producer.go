package asynccommtest

import (
	"async-comm/internal/app/asynccommtest/config"
	"sync"
	"time"
)

func (a *App) InitiateProducer(ru config.Routine, app *App, wg *sync.WaitGroup, quit chan bool) {
	defer wg.Done()
	// TODO call asy.create()
	for {
		select {
		case <-quit:
			a.log.Warnf("sigterm received, safely stopping producer '%s'", ru.Name)
			return
		default:
			err := app.Push(&ru)
			if err != nil {
				if _, ok := err.(*MessageFormatError); ok {
					a.log.Errorf(err.Error())
					a.log.Warnf("producer '%s' is shutting down due to fatal error.", ru.Name)
					return
				}
				a.log.Errorf(err.Error())
			}
			time.Sleep(time.Duration(ru.Message.Freq) * time.Millisecond)
		}
	}
}