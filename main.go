package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/beevik/etree"
	"golang.org/x/text/encoding/charmap"
)

const parserOpenBlockTagLiteral = "parser_open_block_tag"

var (
	isWindows = strings.Contains(strings.ToLower(runtime.GOOS), "windows")
)

type Config struct {
	FieldOrder []string
	FieldMap   map[string]string
}

type Record map[string]string

func loadConfig(configFile string) *Config {
	fieldOrder := []string{
		"Номер",
		"Название",
		"Вес брутто(кг)",
		"Цена товара",
		"Валюта",
		"Курс",
		"Таможенная стоимость",
		"Производитель",
		"Модель",
		"Торговая марка",
		"Количество",
		"Единица измерения",
		"Код товара",
		"Инвойс",
	}

	fieldMap := map[string]string{
		parserOpenBlockTagLiteral:  "ESADout_CUGoods",
		"GoodsNumeric":             "Номер",
		"GoodsDescription":         "Название",
		"GrossWeightQuantity":      "Вес брутто(кг)",
		"InvoicedCost":             "Цена товара",
		"ContractCurrencyCode":     "Валюта",
		"ContractCurrencyRate":     "Курс",
		"CustomsCost":              "Таможенная стоимость",
		"Manufacturer":             "Производитель",
		"GoodsModel":               "Модель",
		"TradeMark":                "Торговая марка",
		"GoodsQuantity":            "Количество",
		"MeasureUnitQualifierName": "Единица измерения",
		"Code":                     "Код товара",
		"PrDocumentNumber":         "Инвойс",
	}

	config := &Config{
		FieldOrder: fieldOrder,
		FieldMap:   fieldMap,
	}

	if configFile == "" {
		configFile = ".xml_to_csv_cfg"
	}

	if file, err := os.Open(configFile); err == nil {
		defer func() { _ = file.Close() }()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.Split(line, "=")
			if len(parts) == 2 {
				xmlTag := strings.TrimSpace(parts[0])
				csvField := strings.TrimSpace(parts[1])
				config.FieldMap[xmlTag] = csvField
				found := false
				for _, field := range config.FieldOrder {
					if field == csvField {
						found = true
						break
					}
				}
				if !found {
					config.FieldOrder = append(config.FieldOrder, csvField)
				}
			}
		}
	}

	return config
}

func main() {
	if isWindows {
		defer func() {
			fmt.Println("Нажмите Enter для выхода...")
			_, _ = fmt.Scanln()
		}()
	}

	var dataDir, configFile string

	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	} else {
		dataDir = "data"
	}

	if len(os.Args) > 2 {
		configFile = os.Args[2]
	} else {
		configFile = "xml_to_csv_cfg"
	}

	config := loadConfig(configFile)

	files, err := filepath.Glob(filepath.Join(dataDir, "*.[xX][mM][lL]"))
	if err != nil {
		fmt.Println("Ошибка при поиске XML файлов:", err)
		return
	}

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	var records []Record

	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			recs := parseXML(f, config)
			if len(recs) > 0 {
				mu.Lock()
				records = append(records, recs...)
				mu.Unlock()
			}
		}(file)
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		fmt.Println("Таймаут")
		return
	}

	if len(records) > 0 {
		writeCSV(records, config)
	} else {
		fmt.Println("Нет данных... завершение программы")
	}
}

func parseXML(filename string, config *Config) []Record {
	doc := etree.NewDocument()
	if err := doc.ReadFromFile(filename); err != nil {
		return nil
	}

	blockTag, exists := config.FieldMap[parserOpenBlockTagLiteral]
	if !exists {
		return nil
	}

	var records []Record
	for _, block := range doc.FindElements("//" + blockTag) {
		record := make(Record)
		for xmlTag, csvField := range config.FieldMap {
			if xmlTag == parserOpenBlockTagLiteral {
				continue
			}

			elem := block.FindElement(".//" + xmlTag)
			if elem != nil {
				if elem.Text() == "ContractCurrencyCode" {
					fmt.Println("1")
				}
				record[csvField] = elem.Text()
			}
		}
		if len(record) > 0 {
			records = append(records, record)
		}
	}
	return records
}

func writeCSV(records []Record, config *Config) {
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	filename := fmt.Sprintf("result_%s.csv", timestamp)

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Ошибка при создании CSV файла:", err)
		return
	}
	defer func() { _ = file.Close() }()

	var writer *csv.Writer
	if isWindows {
		encoder := charmap.Windows1251.NewEncoder()
		writer = csv.NewWriter(encoder.Writer(file))
	} else {
		writer = csv.NewWriter(file)
	}
	writer.Comma = ';'
	defer writer.Flush()

	if len(records) == 0 {
		return
	}

	headers := getHeaders(records, config)
	if err := writer.Write(headers); err != nil {
		fmt.Println("Ошибка при записи заголовков:", err)
		return
	}

	for _, record := range records {
		row := make([]string, len(headers))
		for i, header := range headers {
			row[i] = record[header]
		}
		if err := writer.Write(row); err != nil {
			fmt.Println("Ошибка при записи строки:", err)
			return
		}
	}
}

func getHeaders(records []Record, config *Config) []string {
	var headers []string
	usedFields := make(map[string]bool)

	for _, csvField := range config.FieldOrder {
		if !usedFields[csvField] {
			headers = append(headers, csvField)
			usedFields[csvField] = true
		}
	}

	for _, record := range records {
		for key := range record {
			if !usedFields[key] {
				headers = append(headers, key)
				usedFields[key] = true
			}
		}
	}

	return headers
}
