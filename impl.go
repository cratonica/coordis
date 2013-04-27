package coordis

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/nu7hatch/gouuid"
	"time"
)

type keyType string

const (
	ktPrereq     keyType = "P"
	ktWaitingOn  keyType = "W"
	ktBlockedBy  keyType = "B"
	ktReady      keyType = "R"
	ktTypeOf     keyType = "T"
	ktData       keyType = "D"
	ktAssignedTo keyType = "A"
)

type impl struct {
	client         redis.Conn
	prefix         string
	waiters        int
	isShuttingDown bool
	waitCom        chan int
}

// Instantiate a new Coordis, prefix is prepended to
// each key that Coordis stores in Redis to avoid
// collisions with other data.
func NewCoordis(client redis.Conn, prefix string) Coordis {
	ret := &impl{client, prefix, 0, false, make(chan int)}
	go func() {
		for !ret.isShuttingDown || ret.waiters > 0 {
			ret.waiters += <-ret.waitCom
		}
	}()
	return ret
}

// Left is the front of the queue
func (tm *impl) scheduleInner(t *Task) (string, error) {
	uid, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	id := uid.String()
	if t.Prereqs != nil && len(t.Prereqs) > 0 {
		for _, p := range t.Prereqs {
			pId, err := tm.scheduleInner(p)
			if err != nil {
				return "", err
			}
			tm.client.Send("SADD", tm.buildKey(ktPrereq, id), pId)
			tm.client.Send("SET", tm.buildKey(ktWaitingOn, pId), id)
		}
		tm.client.Send("LPUSH", tm.buildKey(ktBlockedBy, t.Type), id)
	} else {
		tm.client.Send("LPUSH", tm.buildKey(ktReady, t.Type), id)
	}
	tm.client.Send("SET", tm.buildKey(ktTypeOf, id), t.Type)
	tm.client.Send("SET", tm.buildKey(ktData, id), t.Data)
	return id, nil
}

func (tm *impl) Schedule(task *Task) error {
	tm.client.Send("MULTI")
	_, err := tm.scheduleInner(task)
	if err != nil {
		tm.client.Do("DISCARD")
		return err
	}
	_, err = tm.client.Do("EXEC")
	return err
}

// Blocks until one available (or until Shutdown is called)
func (tm *impl) WaitForNext(theType string, assignTo string) (string, error) {
	tm.waitCom <- 1
	defer func() { tm.waitCom <- -1 }()
	for !tm.isShuttingDown {
		r, err := redis.String(tm.client.Do("BRPOPLPUSH", tm.buildKey(ktReady, theType), tm.buildKey(ktAssignedTo, assignTo), 1))
		if err != redis.ErrNil {
			return r, err
		}
	}
	return "", ErrShutdown
}

func (tm *impl) GetData(id string) (string, error) {
	return redis.String(tm.client.Do("GET", tm.buildKey(ktData, id)))
}

func (tm *impl) SetCompleted(id, assignedTo string) error {
	assignedToKey := tm.buildKey(ktAssignedTo, assignedTo)
	totalRemoved, err := redis.Int(tm.client.Do("LREM", assignedToKey, 0, id))
	if totalRemoved != 1 {
		return errors.New(fmt.Sprintf("SetCompleted: %s is not assigned to %s", id, assignedTo))
	}

	waitingOnKey := tm.buildKey(ktWaitingOn, id)
	blockedId, err := redis.String(tm.client.Do("GET", waitingOnKey))
	if err != nil && err != redis.ErrNil {
		return err
	}
	if err != redis.ErrNil {
		// Handle anyone blocked by this task
		preKey := tm.buildKey(ktPrereq, blockedId)

		// This needs to be atomic or we get a race condition
		tm.client.Send("MULTI")
		tm.client.Send("SREM", preKey, id)
		tm.client.Send("EXISTS", preKey)
		replies, err := redis.Values(tm.client.Do("EXEC"))

		if err != nil {
			return err
		}
		if replies[0] != int64(1) {
			return errors.New(fmt.Sprintf("SetCompleted: Removed %d when SREMed %s from %s", replies[0], id, preKey))
		}
		stillBlocked := replies[1] == int64(1)
		if !stillBlocked {
			typeOfKey := tm.buildKey(ktTypeOf, blockedId)
			typeOf, err := redis.String(tm.client.Do("GET", typeOfKey))
			if err != nil {
				return err
			}
			readyKey := tm.buildKey(ktReady, typeOf)
			_, err = tm.client.Do("LPUSH", readyKey, blockedId)
			if err != nil {
				return err
			}
			blockedKey := tm.buildKey(ktBlockedBy, typeOf)
			totalRemoved, err := redis.Int(tm.client.Do("LREM", blockedKey, 0, blockedId))
			if err != nil {
				return err
			}
			if totalRemoved != 1 {
				return errors.New(fmt.Sprintf("LREMed %d of %s from %s\n", totalRemoved, blockedId, blockedKey))
			}
		}
		_, err = tm.client.Do("DEL", waitingOnKey)
		if err != nil {
			return err
		}
	}

	didDelete, err := redis.Bool(tm.client.Do("DEL", tm.buildKey(ktTypeOf, id)))
	if err != nil {
		return err
	}
	if !didDelete {
		return errors.New(fmt.Sprintf("Unable to DEL %s", tm.buildKey(ktTypeOf, id)))
	}
	didDelete, err = redis.Bool(tm.client.Do("DEL", tm.buildKey(ktData, id)))
	if err != nil {
		return err
	}
	if !didDelete {
		return errors.New(fmt.Sprintf("Unable to DEL %s", tm.buildKey(ktData, id)))
	}

	return nil
}

func (tm *impl) GetAssignedTasks(who string) ([]string, error) {
	ret, err := redis.Strings(tm.client.Do("LRANGE", tm.buildKey(ktAssignedTo, who), 0, -1))
	if err == redis.ErrNil {
		return []string{}, nil
	}
	return ret, err
}

func (tm *impl) Shutdown() {
	tm.isShuttingDown = true
	for tm.waiters > 0 {
		time.Sleep(time.Second)
	}
}

func (tm *impl) buildKey(primary keyType, id string) string {
	return fmt.Sprintf("%s:%s:%s", tm.prefix, primary, id)
}
