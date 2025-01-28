package main

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

const (
	dbHost     = "localhost"
	dbPort     = 5432
	dbUser     = "validator"
	dbPassword = "val1dat0r"
	dbName     = "project-sem-1"
)

type InsertResult struct {
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
}

func main() {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Database ping error: %v", err)
	}

	createTableIfNotExists(db)

	http.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received %s request to /api/v0/prices", r.Method)
		switch r.Method {
		case http.MethodPost:
			handlePostPrices(db, w, r)
		case http.MethodGet:
			handleGetPrices(db, w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func createTableIfNotExists(db *sql.DB) {
	query := `
		CREATE TABLE IF NOT EXISTS prices (
			id SERIAL PRIMARY KEY,
			product_id INT NOT NULL,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			price NUMERIC NOT NULL,
			create_date DATE NOT NULL
		)
	`
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	log.Println("Table 'prices' ensured")
}

func handlePostPrices(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
		http.Error(w, "Expected multipart/form-data", http.StatusBadRequest)
		return
	}

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		log.Printf("Error parsing form: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("Error getting file: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	defer file.Close()

	log.Printf("Received file: %s", header.Filename)
	if !strings.HasSuffix(strings.ToLower(header.Filename), ".zip") {
		http.Error(w, "File must be a ZIP archive", http.StatusBadRequest)
		return
	}

	buf, err := readFileToBytes(file)
	if err != nil {
		log.Printf("Error reading file: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	zipReader, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		log.Printf("Error opening ZIP file: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	csvFile := findCSV(zipReader)
	if csvFile == nil {
		log.Println("CSV file not found in ZIP")
		http.Error(w, "CSV file not found in ZIP", http.StatusBadRequest)
		return
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(context.Background(), `
		INSERT INTO prices (product_id, name, category, price, create_date)
		VALUES ($1, $2, $3, $4, $5)
	`)
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	reader, err := openCSVFromZip(csvFile)
	if err != nil {
		log.Printf("Error opening CSV file: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	_, _ = reader.Read() // Skip header row
	inserted := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			continue
		}
		if len(record) < 6 {
			log.Printf("Skipping invalid record: %v", record)
			continue
		}

		productID, _ := strconv.Atoi(record[0])
		price, _ := strconv.ParseFloat(record[4], 64)
		date, _ := time.Parse("2006-01-02", record[5])

		_, err = stmt.ExecContext(context.Background(), productID, record[1], record[2], price, date)
		if err != nil {
			log.Printf("Error inserting record: %v", err)
			continue
		}
		inserted++
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	result, err := getInsertResult(db, inserted)
	if err != nil {
		log.Printf("Error getting result: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleGetPrices(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT id, product_id, name, category, price, create_date
		FROM prices
		ORDER BY id ASC
	`)
	if err != nil {
		log.Printf("Error querying database: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	all := [][]string{{"id", "product_id", "name", "category", "price", "create_date"}}
	for rows.Next() {
		var id, productID int
		var name, category string
		var price float64
		var createDate time.Time

		if err := rows.Scan(&id, &productID, &name, &category, &price, &createDate); err != nil {
			log.Printf("Error scanning row: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		all = append(all, []string{
			strconv.Itoa(id),
			strconv.Itoa(productID),
			name,
			category,
			fmt.Sprintf("%.2f", price),
			createDate.Format("2006-01-02"),
		})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over rows: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var csvBuf bytes.Buffer
	writer := csv.NewWriter(&csvBuf)
	if err := writer.WriteAll(all); err != nil {
		log.Printf("Error writing CSV: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var zipBuf bytes.Buffer
	zipWriter := zip.NewWriter(&zipBuf)
	zipFile, err := zipWriter.Create("data.csv")
	if err != nil {
		log.Printf("Error creating ZIP file: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if _, err := zipFile.Write(csvBuf.Bytes()); err != nil {
		log.Printf("Error writing to ZIP file: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if err := zipWriter.Close(); err != nil {
		log.Printf("Error closing ZIP writer: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="prices.zip"`)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(zipBuf.Bytes()); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func getInsertResult(db *sql.DB, inserted int) (*InsertResult, error) {
	var totalCategories int
	err := db.QueryRow(`SELECT COUNT(DISTINCT category) FROM prices`).Scan(&totalCategories)
	if err != nil {
		return nil, fmt.Errorf("error querying total categories: %v", err)
	}

	var totalPrice float64
	err = db.QueryRow(`SELECT COALESCE(SUM(price), 0) FROM prices`).Scan(&totalPrice)
	if err != nil {
		return nil, fmt.Errorf("error querying total price: %v", err)
	}

	return &InsertResult{
		TotalItems:      inserted,
		TotalCategories: totalCategories,
		TotalPrice:      totalPrice,
	}, nil
}

func readFileToBytes(file io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, file)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func findCSV(zr *zip.Reader) *zip.File {
	for _, file := range zr.File {
		if strings.HasSuffix(file.Name, "data.csv") {
			return file
		}
	}
	return nil
}

func openCSVFromZip(file *zip.File) (*csv.Reader, error) {
	f, err := file.Open()
	if err != nil {
		return nil, err
	}
	return csv.NewReader(f), nil
}
