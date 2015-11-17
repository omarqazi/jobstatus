package jobstatus

import (
	"testing"
	"gopkg.in/redis.v3"
	"time"
)

func init() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	})
}

func TestRedis(t *testing.T) {
	if err := RedisClient.Ping(	).Err(); err != nil {
		t.Error("redis did not respond")
	}
}

func TestJobStatus(t *testing.T) {
	j := Job{"some-job-id"}
	status, err := j.GetStatus()
	if err != nil {
		t.Error("Error getting job status:",err,status)
	}
	newStatusMessage := "Hello world"
	status.Message = newStatusMessage
	if err := status.Save(); err != nil {
		t.Error("Error saving status:",err)
	}
	
	status, err = j.GetStatus()
	if err != nil {
		t.Error("Error getting job status:",err,status)
	}
	
	if status.Message != newStatusMessage {
		t.Error("Status message not saved properly. Expected",newStatusMessage,"but got",status.Message)
	}
}

func TestUpdates(t *testing.T) {
	j := Job{"auto-updates-test"}
	status, err := j.GetStatus()
	if err != nil {
		t.Error(err)
	}
	
	uc := status.UpdateChannel()
	aMessage := "hello world"
	status.Message = aMessage
	<-uc
	if err := status.Save(); err != nil {
		t.Error("Error saving status:",err)
		return
	}
	
	t.Log("listenig to channel")
	timeout := time.Tick(1 * time.Second)
	select {
	case <-timeout:
		t.Error("Error: channel did not get update within 1 second")
	case updatedStatus := <-uc:
		if updatedStatus.Message != aMessage {
			t.Error("Expected message",aMessage,"but got",updatedStatus.Message)
		}
	}
}