package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	// Ограничиваем количество одновременных соединений
	listenerCounter := make(chan struct{}, 2)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		listenerCounter <- struct{}{}

		go func(conn net.Conn) {
			keysMap := make(map[string]string)
			mtx := &sync.RWMutex{}

			defer conn.Close()
			defer func() {
				<-listenerCounter
			}()

			for {
				err = parseContent(conn, keysMap, mtx)
				if err != nil {
					if err != io.EOF {
						conn.Write([]byte(err.Error()))
					}
					break
				}
			}
		}(conn)
	}
}

func readLines(reader *bufio.Reader) (string, error) {
	content, err := readLine(reader)
	if err != nil {
		return "", err
	}
	_, err = readLine(reader)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func readLine(reader *bufio.Reader) (string, error) {
	content, isPrefix, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	if isPrefix {
		// прочитана не вся строка
		return "", fmt.Errorf("wrong string format")
	}

	return string(content), nil
}

func parseContent(conn net.Conn, keysMap map[string]string, mtx *sync.RWMutex) error {
	reader := bufio.NewReader(conn)

	firstByte, err := reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return err
		}
		return err
	}
	if firstByte != '*' {
		// это не массив
		return fmt.Errorf("waiting array")
	}

	// проверяем длину переданную
	count, err := readLines(reader)
	if err != nil {
		return err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return err
	}
	if countInt < 1 {
		// не та длина
		return fmt.Errorf("wrong length")
	}

	var command string
	if countInt == 1 {
		command, err = readLine(reader)
	} else {
		command, err = readLines(reader)
	}
	if err != nil {
		return err
	}
	stringCommand := string(command)
	switch stringCommand {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		if countInt != 2 {
			// не та длина
			return fmt.Errorf("wrong length")
		}
		text, err := readLine(reader)
		if err != nil {
			return err
		}
		message := "$" + strconv.Itoa(len(text)) + "\r\n" + string(text) + "\r\n"
		conn.Write([]byte(message))
	case "SET":
		if countInt != 3 {
			// не та длина
			return fmt.Errorf("wrong length")
		}
		key, err := readLines(reader)
		if err != nil {
			return err
		}
		value, err := readLine(reader)
		if err != nil {
			return err
		}

		mtx.Lock()
		keysMap[key] = value
		mtx.Unlock()

		conn.Write([]byte("+OK\r\n"))
	case "GET":
		if countInt != 2 {
			// не та длина
			return fmt.Errorf("wrong length")
		}
		key, err := readLine(reader)
		if err != nil {
			return err
		}

		mtx.RLock()
		saved, ok := keysMap[key]
		mtx.RUnlock()

		var message string
		if !ok {
			message = "$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n"
			conn.Write([]byte(message))
		} else {
			message = "$" + strconv.Itoa(len(saved)) + "\r\n" + saved + "\r\n"

		}

		conn.Write([]byte(message))
	default:
		conn.Write([]byte("unknown command"))
	}
	return nil
}
