package main

import (
	"kafka-to-elastic-pipeline/application"
)

// Make `main` just call another function for the sake of integration testing
func main() {
	application.Application()
}
