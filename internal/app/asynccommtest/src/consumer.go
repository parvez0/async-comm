package src

import (
	"context"
	"sync"
	"time"
)

func (a *App) RegisterConsumer(ctx context.Context, r Routine, wg *sync.WaitGroup)  {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			sts, err := a.aclib.SetKey("active_consumer_" + r.Name, time.Now().String(), time.Duration(r.RefreshTime) * time.Millisecond)
			if err != nil {
				a.log.Panicf("failed to register consumer %s - %s", r.Name, err.Error())
			}
			refreshTime := r.RefreshTime - (r.RefreshTime/10)
			a.log.Infof("consumer '%s' registered with refreshTime: '%dms', status: '%s'", r.Name, refreshTime, sts)
			time.Sleep(time.Duration(refreshTime) * time.Millisecond)
		}
	}
}

func (a *App) InitiateConsumers(ctx context.Context, r Routine, wg *sync.WaitGroup)  {
	defer wg.Done()
	start := time.Now().Add(time.Duration(r.RefreshTime) * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			a.log.Warnf("sigterm received, safely stopping consumer '%s'", r.Name)
			return
		default:
			if time.Now().After(start) {
				err := a.aclib.ClaimPendingMessages(r.Q, r.Name)
				if err != nil {
					a.log.Errorf("failed to claim pending messages for stream '%s' by consumer '%s': %s", r.Q, r.Name, err.Error())
				}
				a.log.Debugf("pending message claim request initiated by consumer '%s'", r.Name)
				start = time.Now().Add(time.Duration(r.RefreshTime) * time.Millisecond)
			}
			msg, id,  err := a.aclib.Pull(ctx, r.Q, r.Name)
			if err != nil {
				a.log.Errorf("failed to consume message: { stream: %s, consumer: %s, error: %s }", r.Q, r.Name, err.Error())
			} else {
				a.log.Infof("Pulled %s at %s by %s", string(msg), GetCurTime(), r.Name)
			}
			time.Sleep(time.Duration(r.ProcessingTime) * time.Millisecond)
			if id != "" {
				err = a.aclib.Ack(r.Q, id)
				if err != nil {
					a.log.Errorf("failed to ack message %s by consumer %s : %s", id, r.Name, err.Error())
				}
			}
		}
	}
}