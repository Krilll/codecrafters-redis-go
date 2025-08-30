package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
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
				err = reader(conn)
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

func reader(conn net.Conn) error {
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

	// получаем кол-во элементов
	countString, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	countString = strings.TrimSuffix(countString, "\r\n")

	count, err := strconv.Atoi(countString)
	if err != nil {
		return err
	}
	conn.Write([]byte(countString))

	// all := []byte{}
	count++
	for i := range count {
		_ = i
		firstByte, err := reader.ReadByte()
		if err != nil {
			return err
		}

		if firstByte == '$' {
			// пришла команда или ее аргумент

			word, err := reader.ReadString('\n')
			if err != nil {
				return err
			}

			word = strings.TrimSuffix(word, "\r\n")
			conn.Write([]byte(word))

			cc, err := strconv.Atoi(word)
			if err != nil {
				return err
			}

			strBytes := make([]byte, cc)
			_, err = io.ReadFull(reader, strBytes)
			if err != nil {
				return err
			}

			conn.Write(strBytes)

		}
		// all = append(all, firstByte)
		conn.Write([]byte("Step "))

	}
	// conn.Write(all)
	// bufParts := bytes.Split(buf[:], []byte("\r\n"))

	// if len(bufParts) < 3 {
	// 	conn.Write([]byte("Waiting for command\r\n"))
	// 	break
	// }

	// command := string(bufParts[0][:])
	// switch command {
	// case "PING":
	// 	conn.Write([]byte("+PONG\r\n"))
	// case "ECHO":
	// 	// msg, err := bufParts[1]
	// 	// if err != nil {
	// 	conn.Write([]byte("Waiting for text\r\n"))
	// 	// } else {
	// 	// 	conn.Write(msg)
	// 	// }
	// default:
	// 	conn.Write([]byte(command))
	// }

	// switch {
	// case len(parts) == 0:
	// 	conn.Write([]byte("Waiting for command\r\n"))
	// case
	// }
	// strBuf := string(buf[:])

	// conn.Write([]byte(strBuf))

	// // fmt.Println("Client buf", buf)

	// if err != nil {
	// 	if err == io.EOF {
	// 		fmt.Println("Client close connection")
	// 		break
	// 	}
	// 	fmt.Println("Error listener: ", err.Error())
	// 	os.Exit(1)
	// }

	// conn.Write([]byte("+PONG\r\n"))
	return nil

}
