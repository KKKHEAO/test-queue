package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	initQueueSize = 16
	growFactor    = 2
)

// Queue - это структура данных для хранения элементов очереди.
type Queue struct {
	sync.Mutex
	buf     []string
	head    int
	tail    int
	count   int
	waiters []chan string // Каналы для ожидания сообщений, если очередь пуста
}

// NewQueue создает и возвращает новую очередь.
func NewQueue() *Queue {
	return &Queue{
		buf:     make([]string, initQueueSize),
		waiters: make([]chan string, 0),
	}
}

// grow увеличивает размер буфера очереди, когда он заполнен.
func (q *Queue) grow() {
	newBuf := make([]string, len(q.buf)*growFactor)
	for i := 0; i < q.count; i++ {
		newBuf[i] = q.buf[(q.head+i)%len(q.buf)]
	}
	q.buf = newBuf
	q.head = 0
	q.tail = q.count
}

// Push добавляет элемент в очередь.
func (q *Queue) Push(msg string) {
	q.Lock()

	// Если есть ожидающие, значит буфер пуст — отдаём сразу
	if len(q.waiters) > 0 {
		ch := q.waiters[0]
		q.waiters = q.waiters[1:]
		q.Unlock()
		ch <- msg
		return
	}

	// Увеличиваем размер буфера, если он заполнен
	if q.count == len(q.buf) {
		q.grow()
	}

	q.buf[q.tail] = msg
	q.tail = (q.tail + 1) % len(q.buf)
	q.count++
	q.Unlock()
}

// PopTimeout удаляет и возвращает первый элемент из очереди.
// Если очередь пуста, то ждёт таймаут и возвращает пустую строку.
func (q *Queue) PopTimeout(timeout time.Duration) string {
	q.Lock()

	// В буфере есть элементы, можно сразу вернуть
	if q.count > 0 {
		msg := q.buf[q.head]
		q.head = (q.head + 1) % len(q.buf)
		q.count--
		q.Unlock()
		return msg
	}
	// При нулевом таймауте возвращаем пустую строку
	if timeout == 0 {
		q.Unlock()
		return ""
	}

	// Если буфер пуст, регистрируем канал ожидания
	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	q.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-ch:
		// Push уже отправил сообщение и удалил канал из waiters.
		// Просто возвращаем сообщение.
		return msg
	case <-timer.C:
		q.Lock()
		// Пытаемся удалить канал из waiters.
		// Если Push уже успел его забрать — не найдём, и это ок.
		for i, w := range q.waiters {
			if w == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		q.Unlock()

		// Push мог уже отправить сообщение, но мы не успели его прочитать.
		select {
		case msg := <-ch:
			return msg
		default:
			return ""
		}
	}
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
		timeout := 0
		// Получаем таймаут из query параметров, если он есть
		if t := r.URL.Query().Get("timeout"); t != "" {
			timeout, _ = strconv.Atoi(t)
		}

		msg := q.PopTimeout(time.Duration(timeout) * time.Second)
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
