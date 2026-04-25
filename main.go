package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
)

// Queue - это структура данных для хранения элементов очереди.
type Queue struct {
	sync.Mutex
	items []string
}

// NewQueue создает и возвращает новую очередь.
func NewQueue() *Queue {
	return &Queue{
		items: make([]string, 0),
	}
}

// Push добавляет элемент в очередь.
func (q *Queue) Push(msg string) {
	q.Lock()
	defer q.Unlock()
	q.items = append(q.items, msg)
}

// Pop удаляет и возвращает первый элемент из очереди. Если очередь пуста, возвращает пустую строку.
func (q *Queue) Pop() string {
	q.Lock()
	defer q.Unlock()
	if len(q.items) == 0 {
		return ""
	}
	msg := q.items[0]
	q.items = q.items[1:]
	return msg
}

// Храним доступные очереди.
type QueuesMap struct {
	sync.Mutex
	queues map[string]*Queue
}

// NewQueueMap создает и возвращает новый экземпляр QueuesMap.
func NewQueuesMap() *QueuesMap {
	return &QueuesMap{
		queues: make(map[string]*Queue),
	}
}

// Получаем очередь по имени, если ее нет - создаем новую.
func (q *QueuesMap) getQueue(name string) *Queue {
	q.Lock()
	defer q.Unlock()

	// Если очередь уже существует, возвращаем ее
	if queue, exists := q.queues[name]; exists {
		return queue
	}
	// Иначе создаем новую очередь, сохраняем ее в мапу и возвращаем
	q.queues[name] = NewQueue()
	return q.queues[name]
}

// Обработчик HTTP запросов.
func handler(w http.ResponseWriter, r *http.Request, qM *QueuesMap) {
	// Получаем имя очереди из query параметров
	queue := r.URL.Query().Get("queue")
	// Если имя очереди пустое, то возвращаем 400ку
	if queue == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// Получаем очередь по имени (если ее нет - создается новая)
	q := qM.getQueue(queue)

	switch r.Method {
	case http.MethodPut:
		msg := r.URL.Query().Get("v")
		if msg == "" {
			// Если сообщение пустое, то возвращаем 400ку
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Добавляем сообщение в очередь и возвращаем 200
		q.Push(msg)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		msg := q.Pop()
		if msg == "" {
			// Если очередь пуста, то возвращаем 404
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Если сообщение успешно получено, то возвращаем его и 200
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(msg))
	default:
		// Если метод не поддерживается, то возвращаем 405
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func main() {
	// Берем порт из аргументов командной строки, по умолчанию 8080
	port := flag.Int("port", 8080, "Port to run the server on")
	flag.Parse()

	qM := NewQueuesMap()

	log.Printf("Starting test project on port: %d\n", *port)

	// Регаем обработчик и пробрасываем в замыкании ссылку на нашу структуру с очередями
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handler(w, r, qM)
	})

	// Запускаем простой HTTP сервер на заданном порту
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
