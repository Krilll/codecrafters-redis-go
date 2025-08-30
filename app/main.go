package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
	listenerCounter := make(chan struct{}, 100)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		listenerCounter <- struct{}{}

		go func(conn net.Conn) {
			defer conn.Close()
			defer func() {
				<-listenerCounter
			}()

			for {
				err = parseContent(conn)
				if err != nil {
					fmt.Println(err)
					if err == io.EOF {
						os.Exit(1)
					}
				}
			}
		}(conn)
	}
}

func getUserArray(content []byte) []string {
	words := []string{}
	current := ""
	for i := 0; i < len(content); i++ {
		if content[i] == '\r' {
			i += 3
			words = append(words, current)
			current = ""
		} else {
			current += string(content[i])
		}
	}
	if current != "" {
		words = append(words, current)
	}
	return words
}

func parseContent(conn net.Conn) error {
	reader := bufio.NewReader(conn)

	// определяем тип
	firstByte, err := reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return err
	}
	if firstByte != '*' {
		// это не массив
		return fmt.Errorf("waiting array")
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return err
	}
	// fmt.Println(content)
	// conn.Write(content)

	words := getUserArray(content)
	if len(words) < 2 {
		// не та длина
		return fmt.Errorf("wrong length")
	}

	getLen, err := strconv.Atoi(words[0])
	if err != nil {
		return err
	}
	if getLen != (len(words) - 1) {
		fmt.Println(getLen)
		fmt.Println(len(words))
		for q, a := range words {
			fmt.Println(q, a)
		}
		// неправильное кол-во элементов
		return fmt.Errorf("incorrect number of elements")
	}

	switch words[1] {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		if len(words) < 3 {
			// нечего выводить
			return fmt.Errorf("need words")
		}
		// fmt.Println(len(words))
		// fmt.Println(words[2])
		message := "$" + strconv.Itoa(len(words[2])) + "\r\n" + words[2] + "\r\n"
		conn.Write([]byte(message))
	default:
		conn.Write([]byte("unknown command"))
	}
	return nil
}
