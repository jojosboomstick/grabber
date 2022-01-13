package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var (
	WORKERS        int    = 2
	REPORT_TIMEOUT int    = 10
	DUP_TO_STOP    int    = 500
	HASH_FILE      string = "hash.bin"
	QUOTES_FILE    string = "quotes.txt"
)

var used map[string]struct{} = make(map[string]struct{})

func grab() <-chan string {
	c := make(chan string)
	for i := 0; i < WORKERS; i++ {
		go func() {
			for {
				res, err := http.Get("http://vpustotu.ru/moderation/")
				if err != nil {
					log.Fatal(err)
				}

				if res.StatusCode != 200 {
					log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
					res.Body.Close()
				}

				doc, err := goquery.NewDocumentFromReader(res.Body)
				if err != nil {
					log.Fatal(err)
				}
				if s := strings.TrimSpace(doc.Find(".fi_text").Text()); s != "" {
					c <- s
				}
				time.Sleep(100 * time.Microsecond)
				res.Body.Close()
			}
		}()
	}
	fmt.Println("Запущено потоков: ", WORKERS)
	return c
}

// check
func check(e error) {
	if e != nil {
		panic(e)
	}
}

// md5Hash return cheksum of data in string
func md5Hash(s []byte) string {
	k := md5.Sum(s)
	return hex.EncodeToString([]byte(k[:]))
}

func readHashes() {
	if _, err := os.Stat(HASH_FILE); err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Файл хешей не найден, будет создан новый.")
			return
		}
	}

	fmt.Println("Чтение хешей...")
	hashFile, err := os.OpenFile(HASH_FILE, os.O_RDONLY, 0666)
	check(err)
	defer hashFile.Close()

	//читать будем блоками по 16 байт - как раз один хеш:
	data := make([]byte, 16)
	for {
		n, err := hashFile.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		if n == 16 {
			used[hex.EncodeToString(data)] = struct{}{}
		}
	}
	fmt.Println("Завершено. Прочитано хешей:", len(used))
}

func main() {
	flag.IntVar(&WORKERS, "workers", WORKERS, "кол-во потоков")
	flag.IntVar(&REPORT_TIMEOUT, "timeout", REPORT_TIMEOUT, "частота отчетов(сек)")
	flag.IntVar(&DUP_TO_STOP, "dup", DUP_TO_STOP, "кол-во дубликатов для остановки")
	flag.StringVar(&HASH_FILE, "hashf", HASH_FILE, "файл хешей")
	flag.StringVar(&QUOTES_FILE, "quotesf", QUOTES_FILE, "файл цитат")
	flag.Parse()

	quotesFile, err := os.OpenFile(QUOTES_FILE, os.O_APPEND|os.O_CREATE, 0666)
	check(err)
	defer quotesFile.Close()

	readHashes()
	hashFile, err := os.OpenFile(HASH_FILE, os.O_APPEND|os.O_CREATE, 0666)
	check(err)
	defer hashFile.Close()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	ticker := time.NewTicker(time.Duration(REPORT_TIMEOUT) * time.Second)
	defer ticker.Stop()

	quotesCount, dupCount := 0, 0
	quotesCh := grab()

	for {
		select {
		case quote := <-quotesCh:
			quotesCount++
			hashString := md5Hash([]byte(quote))
			if _, ok := used[hashString]; !ok {
				used[hashString] = struct{}{}
				hashFile.Write([]byte(hashString))
				quotesFile.WriteString(quote + "\n\n")
				dupCount = 0
			} else {
				if dupCount++; dupCount == DUP_TO_STOP {
					fmt.Println("Достигнут предел повторов, завершаю работу. Всего записей: ", len(used))
					return
				}
			}
		case <-signalCh: //если пришла информация от нотификатора сигналов:
			fmt.Println("CTRL-C: Завершаю работу. Всего записей: ", len(used))
			return
		case <-ticker.C:
			fmt.Printf("Всего %d / Повторов %d (%d записей/сек) \n", len(used), dupCount, quotesCount/REPORT_TIMEOUT)
			quotesCount = 0
		}
	}
}
