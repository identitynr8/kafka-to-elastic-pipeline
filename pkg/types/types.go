package types

type Tweet struct {
	Message       string
	User          *User
	Tags          []string
	RemoteAddress string
}

type EnrichedTweet struct {
	Message       string
	User          *User
	Tags          []string
	RemoteAddress string

	City    map[string]string
	Country map[string]string
}

type User struct {
	Name string
	Id   string
}
