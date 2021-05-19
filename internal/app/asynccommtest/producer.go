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

	a.log.Infof("Total number of messages configured for Producer: %s are: %d", ru.Name, ru.Message.TotalMsgs)
	a.log.Infof("Frequency of messages configured for Producer: %s are: %d", ru.Name, ru.Message.Freq)
	if ru.Message.TotalMsgs != 0 {
		a.log.Infof("Producer: %s will post %d number of messgaes in %s queue", ru.Name, ru.Message.TotalMsgs, ru.Q)
	}

	totalMsgs := 0
	for {
		select {
		case <-ctx.Done():
			a.log.Warnf("sigterm received, safely stopping producer '%s'", ru.Name)
			return
		default:
			if ru.Message.TotalMsgs != 0 && totalMsgs >= ru.Message.TotalMsgs {
				a.log.Infof("Producer: %s exiting because total number of messages: %d requested are posted in the Q",
					ru.Name, ru.Message.TotalMsgs)
				return
			}
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
				totalMsgs += 1
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
