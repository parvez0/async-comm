package main

import (
	"context"
	"os"
	"sync"
	"time"
)

func (a *App) InitiateConsumers(ctx context.Context, r Routine, wg *sync.WaitGroup)  {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			a.log.Warnf("sigterm received, safely stopping consumer '%s'", r.Name)
			return
		default:
			msg, id,  err := a.aclib.Pull(r.Q, r.Name, 100 * time.Millisecond)
			if err != nil || id == "" {
				// ignoring consumer readTimeout error
				// ReadTimeoutError get triggers when no messages were received during the specified time
				if err.Error() == "redis: nil" {
					continue
				}
				a.log.Debugf("Failed Pulling: { stream: %s, consumer: %s, error: %s }", r.Q, r.Name, err.Error())
			} else {
				sMsg := string(msg)
				msgTime := GetTimeFromString(sMsg)
				tm := time.Now()
				timeTaken := tm.Sub(msgTime)
				a.log.Infof("Pulled %s at %s by %s", sMsg, FormatTime(tm), r.Name)
				a.log.Infof("Time %s remained in the Queuing System: %dms", sMsg, timeTaken.Milliseconds())
				time.Sleep(time.Duration(r.ProcessingTime) * time.Millisecond)
				if r.Name != os.Getenv("TEST_CONSUMER") {
					err = a.aclib.Ack(r.Q, id)
					if err != nil {
						a.log.Errorf("failed to ack message %s by consumer %s : %s", id, r.Name, err.Error())
					}
					a.log.Infof("Processing over for %s at %s", sMsg, GetCurTime())
					a.log.Infof("Total Time taken for processing %s: %dms", sMsg, time.Now().Sub(msgTime).Milliseconds())
				}
			}
		}
	}
}
