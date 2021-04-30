package asynccommtest

import (
	"async-comm/internal/app/asynccommtest/config"
	"async-comm/internal/app/asynccommtest/logger"
	"async-comm/pkg/redis"
	"bytes"
	"fmt"
	"text/template"
	"time"
)

type App struct {
	Rdb *redis.Redis
	cnf *config.Config
	log logger.Logger
}

type ProducerMessage struct {
	App string
	Producer string
	Time string
}

// ParseTemplate parses a string into the defined template with
// the defined values for e.g. here format="{{.APP}}-{{.Producer}}-{{.Time}}"
// will be parsed into the ProducerMessage values and a string will be
// returned upon success other wise it will return a non nil error
func ParseTemplate(r *config.Routine, app string) (string, error) {
	t := time.Now()
	cur := fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	msg := ProducerMessage{
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


// custom error types

// MessageFormatError is type error for specifying any errors
// encountered during the template parsing of message
type MessageFormatError struct {
	Message string
	Description string
}

func (m *MessageFormatError) Error() string {
	return fmt.Sprintf("error: %s, description: %s", m.Message, m.Message)
}