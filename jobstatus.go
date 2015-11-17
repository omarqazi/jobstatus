// package jobstatus helps track the status of jobs using redis
package jobstatus

import (
	"gopkg.in/redis.v3"
	"github.com/bitly/go-simplejson"
	"log"
)

var RedisClient *redis.Client

type Job struct {
	Id string
}

func (j Job) RedisBase() string {
	return "jobstatus-job-" + j.Id
}

func (j Job) RedisKey(suffix string) string {
	return (j.RedisBase() + "-" + suffix)
}

type Status struct {
	JobId string
	Log string // The full log the job spit out
	Complete int64 // How many parts are complete
	Total int64 // How many parts total?
	Message string // Summary of current status
	State string // queued, started, error, or done
}

func (s Status) Job() Job {
	return Job{s.JobId}
}

func (s Status) Save() error {
	statusJson := simplejson.New()
	statusJson.Set("message",s.Message)
	statusJson.Set("state",s.State)
	statusJson.Set("complete",s.Complete)
	statusJson.Set("total",s.Total)
	bytes, err := statusJson.MarshalJSON()
	if err != nil {
		return err
	}
	
	err = RedisClient.Set(s.Job().RedisKey("state"),string(bytes),0).Err()
	if err != nil {
		return err
	}
	
	err = RedisClient.Set(s.Job().RedisKey("log"),s.Log,0).Err()
	if err != nil {
		return err
	}
	
	RedisClient.Publish(s.Job().RedisKey("updated"),string(bytes))
	return nil
}

func (s *Status) Read() error {
	j := s.Job()
	statusJson, err := RedisClient.Get(j.RedisKey("state")).Result()
	if err == redis.Nil { // If key not found use default
		s.Message = ""
		s.State = "queued"
		s.Complete = 0
		s.Total = 0
		err = nil
	} else if err == nil { // If key found, parse value
		pjson, err := simplejson.NewJson([]byte(statusJson))
		if err != nil {
			return err
		}
		
		s.Message, err = pjson.Get("message").String()
		s.State, err = pjson.Get("state").String()
		s.Complete, err = pjson.Get("complete").Int64()
		s.Total, err = pjson.Get("total").Int64()
		if err != nil {
			return err
		}
	} else { // on error, returne rror
		return err
	}
	
	s.Log, err = RedisClient.Get(j.RedisKey("log")).Result()
	if err == redis.Nil { // If log is nil
		s.Log = "" // Initialize to blank string
		err = nil // and don't return that as an error
	}
	return err
}

func (s Status) UpdateChannel() (chan Status) {
	updateChannel := make(chan Status,1)
	
	go func() {
		defer close(updateChannel)
		
		pubsub, err := RedisClient.Subscribe(s.Job().RedisKey("updated"))
		if err != nil {
			log.Println(err)
			return
		}
		
		updateChannel <- s		
		_, err = pubsub.ReceiveMessage()
		if err != nil {
			log.Println(err)
			return
		}
		if err := s.Read(); err != nil {
			log.Println(err)
			return
		}
		updateChannel <- s
	}()
	
	return updateChannel
}
func (j *Job) GetStatus() (Status, error) {
	s := Status{JobId: j.Id}
	err := s.Read()
	return s, err	
}

