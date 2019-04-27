package test_data

import (
	"fmt"
	"kafka-to-elastic-pipeline/pkg/types"
	"math/rand"
)

func UsersIterator(usersChannel chan types.User, controlChannel chan struct{}) {
	for {
		user := makeUser()
		select {
		case <-controlChannel:
			return
		case usersChannel <- *user:
		}
	}
}

func TweetsIterator(tweetsChannel chan types.Tweet, controlChannel chan struct{}) {
	for {
		tweet := types.Tweet{
			Message:       fmt.Sprintf("Tweet %d", rand.Int31()),
			User:          makeUser(),
			Tags:          []string{fmt.Sprintf("tag%d", rand.Int()), fmt.Sprintf("tag%d", rand.Int())},
			RemoteAddress: fmt.Sprintf("%d.%d.%d.%d", rand.Intn(255), rand.Intn(255), rand.Intn(255), rand.Intn(255)),
		}
		select {
		case <-controlChannel:
			return
		case tweetsChannel <- tweet:
		}
	}
}

func makeUser() *types.User {
	user := types.User{
		Name: fmt.Sprintf("User name %d", rand.Int31()),
		Id:   fmt.Sprintf("User id %d", rand.Int31()),
	}
	return &user
}
