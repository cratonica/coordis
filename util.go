package coordis

import (
	"github.com/garyburd/redigo/redis"
)

// Clears all keys in a database that start with the given
// prefix. Do not include the semicolon at the end of prefix.
func FlushKeysByPrefix(conn redis.Conn, prefix string) error {
	keys, err := redis.Strings(conn.Do("KEYS", prefix+":*"))
	if err != nil {
		if err == redis.ErrNil {
			return nil
		}
		return err
	}
	for _, v := range keys {
		conn.Send("DEL", v)
	}
	err = conn.Flush()
	if err != nil && err != redis.ErrNil {
		return err
	}
	for _, _ = range keys {
		_, err = conn.Receive()
		if err != nil && err != redis.ErrNil {
			return err
		}
	}
	return nil
}
