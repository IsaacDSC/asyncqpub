package main

import (
	"fmt"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

var (
	redisAddr, port string
)

func init() {
	redisAddr = os.Getenv("HOST_ADDR")
	if redisAddr == "" {
		log.Fatal("required redis_addr")
	}

	port = os.Getenv("PORT")
	if port == "" {
		port = ":3000"
	} else {
		port = ":" + port
	}
}

func main() {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()

	h := asynqmon.New(asynqmon.Options{
		RootPath:     "/monitoring", // RootPath specifies the root for asynqmon app
		RedisConnOpt: asynq.RedisClientOpt{Addr: redisAddr},
	})

	mux := http.NewServeMux()
	mux.Handle(h.RootPath()+"/", h)

	mux.HandleFunc("/publisher/*", func(w http.ResponseWriter, r *http.Request) {
		paths := strings.Split(r.URL.Path, "/")
		if len(paths) != 3 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("required event name"))
			return
		}

		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		eventName := paths[2]
		info, err := client.Enqueue(NewPublisher(eventName, body))
		if err != nil {
			log.Fatalf("could not enqueue task: %v", err)
		}

		log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
		w.Write(body)
	})

	log.Println("[*] Server listen")
	log.Println(fmt.Sprintf("[*] http://localhost%s/monitoring/", port))
	if err := http.ListenAndServe(port, mux); err != nil {
		log.Fatal(err)
	}
}

func NewPublisher(eventName string, payload []byte) *asynq.Task {
	return asynq.NewTask(eventName, payload)
}
