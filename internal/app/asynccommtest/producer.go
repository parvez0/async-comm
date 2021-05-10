package main

import (
	"bytes"
	"context"
	"sync"
	"text/template"
	"time"
)

func (a *App) InitiateProducer(ctx context.Context, ru Routine, app string, wg *sync.WaitGroup) {
	defer wg.Done()
	err := a.aclib.CreateQ(ru.Q, false)
	if err != nil {
		a.log.Panicf("failed to create q '%s' for producer '%s' - %s", ru.Q, ru.Name, err.Error())
	}
	for {
		select {
		case <-ctx.Done():
			a.log.Warnf("sigterm received, safely stopping producer '%s'", ru.Name)
			return
		default:
			msg, err := ParseTemplate(&ru, app)
			if err != nil {
				a.log.Errorf(err.Error())
				a.log.Warnf("producer '%s' is shutting down due to fatal error.", ru.Name)
			}
			a.log.Infof("Posting %s", msg)
			res, err := a.aclib.Push(ru.Q, []byte(msg))
			if err != nil {
				a.log.Infof("Failed Posting %s", msg)
				a.log.Debugf("failed to push message on stream '%s': %s", ru.Q, err.Error())
			} else {
				a.log.Infof("Posted %s Successfully", msg)
				a.log.Debugf("messaged pushed generated  { q: %s, id: %s }", ru.Q, res)
			}
			time.Sleep(time.Duration(ru.Message.Freq) * time.Millisecond)
		}
	}
}

func (a *App) DeleteQ(q string) error {
	return a.aclib.DeleteQ(q)
}

// ParseTemplate parses a string into the defined template with
// the defined values for e.g. here format="{{.APP}}-{{.Producer}}-{{.Time}}"
// will be parsed into the ProducerMessage values and a string will be
// returned upon success other wise it will return a non nil error
func ParseTemplate(r *Routine, app string) (string, error) {
	cur := GetCurTime()
	msg := Message{
		App:      app,
		Producer: r.Name,
		Time:     cur,
	}
	tmp, err := template.New("message").Parse(r.Message.Format)
	if err != nil {
		return "", err
	}
	buf := bytes.Buffer{}
	err = tmp.Execute(&buf, msg)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}