package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

var db *sql.DB
var clientesLimites = make(map[int]int)
var clientesSaldosIniciais = make(map[int]int)
var clientesDadosAtualizando = false //map em Go nao eh thread-safe

func init() {
	db_username := os.Getenv("DB_USERNAME")
	if db_username == "" {
		db_username = "admin"
	}
	db_password := os.Getenv("DB_PASSWORD")
	if db_password == "" {
		db_password = "123"
	}
	db_hostname := os.Getenv("DB_HOSTNAME")
	if db_hostname == "" {
		db_hostname = "localhost"
	}
	db_name := os.Getenv("DB_NAME")
	if db_name == "" {
		db_name = "rinha"
	}
	fmt.Println("Conectando no BD...")
	fmt.Println("DB_HOSTNAME:" + db_hostname)
	fmt.Println("DB_NAME:" + db_name)
	fmt.Println("DB_USERNAME:" + db_username)

	var err error
	db, err = sql.Open("postgres", "postgres://"+db_username+":"+db_password+"@"+db_hostname+"/"+db_name+"?sslmode=disable")
	if err != nil {
		log.Fatal("Invalid DB config:", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal("DB unreachable:", err)
	}
	fmt.Println("Conectado OK!!!")
}

func handle_generico(w http.ResponseWriter, r *http.Request) {
	id, tipo_transacao := obter_dados_rota(r.URL.Path)
	if tipo_transacao == "transacoes" && r.Method == "POST" {
		handle_transacoes(w, r, id)
	} else if tipo_transacao == "extrato" && r.Method == "GET" {
		handle_extrato(w, r, id)
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - Pagina nao encontrada"))
	}
}

// GET /health

func handle_health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("{\"Status\" : \"OK\"}"))
	return
}

// POST /clientes/[id]/transacoes
func handle_transacoes(w http.ResponseWriter, r *http.Request, id int) {
	var resB bytes.Buffer
	_, err := resB.ReadFrom(r.Body)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error"))
		return
	}

	var transacao Transacao
	err = json.Unmarshal(resB.Bytes(), &transacao)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error"))
		return
	}

	if transacao.Valor <= 0 {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("422 - Valor deve ser positivo"))
		return
	}

	if transacao.Tipo != "c" && transacao.Tipo != "d" {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("422 - Tipo deve ser 'c' ou 'd' (minusculo)"))
		return
	}

	var descricao_tamanho = len(transacao.Descricao)
	if descricao_tamanho < 1 || descricao_tamanho > 10 {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("422 - Descricao deve ter tamanho entre 1 e 10"))
		return
	}

	var saldo int
	if transacao.Tipo == "c" {
		err := db.QueryRow(`with novo_saldo as (UPDATE saldos SET saldo = saldo + $1 WHERE cliente_id = $2 RETURNING saldo) insert into transacoes (cliente_id, valor, descricao, tipo, saldo) values ($3, $4, $5, $6, (select * from novo_saldo)) returning saldo`, transacao.Valor, id, id, transacao.Valor, transacao.Descricao, transacao.Tipo).Scan(&saldo)
		if err != nil {
			if strings.Contains(err.Error(), "not-null constraint") {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("404 - Cliente nao encontrado"))
			} else {
				fmt.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("500 - Internal Server Error"))
			}
			return
		}
	} else {
		err := db.QueryRow(`with novo_saldo as (UPDATE saldos SET saldo = saldo - $1 WHERE cliente_id = $2 RETURNING saldo) insert into transacoes (cliente_id, valor, descricao, tipo, saldo) values ($3, $4, $5, $6, (select * from novo_saldo)) returning saldo`, transacao.Valor, id, id, transacao.Valor, transacao.Descricao, transacao.Tipo).Scan(&saldo)
		if err != nil {
			if strings.Contains(err.Error(), "not-null constraint") {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("404 - Cliente nao encontrado"))
			} else if strings.Contains(err.Error(), "check constraint") {
				w.WriteHeader(http.StatusUnprocessableEntity)
				w.Write([]byte("422 - Sem saldo"))
			} else {
				fmt.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("500 - Internal Server Error"))
			}
			return
		}
	}

	transacaoResposta := TransacaoResposta{obter_cliente_limite_cache(id), saldo}
	jsonStr, err := json.Marshal(transacaoResposta)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error"))
		return
	}

	io.WriteString(w, string(jsonStr[:]))
}

// GET /clientes/[id]/extrato
func handle_extrato(w http.ResponseWriter, r *http.Request, id int) {
	rows, err := db.Query(`select valor, tipo, descricao, data_hora_inclusao, saldo from transacoes where cliente_id = $1 order by data_hora_inclusao desc limit 10`, id)
	if err, ok := err.(*pq.Error); ok {
		fmt.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error"))
		return
	}

	data_extrato_str := time.Now().Format("2006-01-02T15:04:05.000000Z")
	var extratoSaldo ExtratoSaldo
	var transacoes []ExtratoTransacao
	for rows.Next() {
		var transacao ExtratoTransacao
		err = rows.Scan(&transacao.Valor, &transacao.Tipo, &transacao.Descricao, &transacao.Data_Hora_Inclusao, &transacao.Saldo)
		if err != nil {
			fmt.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("500 - Internal Server Error"))
			return
		}
		if extratoSaldo.Data_Extrato == "" {
			extratoSaldo.Total = transacao.Saldo
			extratoSaldo.Data_Extrato = data_extrato_str
			extratoSaldo.Limite = obter_cliente_limite_cache(id)
		}
		transacoes = append(transacoes, transacao)
	}

	// nao foi encontrato nenhuma linha de extrato. Isso tambem pode ocorrer se o id eh inexistente
	if len(transacoes) == 0 {
		limite := obter_cliente_limite_cache(id)
		if limite == -1 {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("404 - Cliente nao encontrado"))
			return
		}
		extratoSaldo.Total = obter_cliente_saldo_inicial_cache(id)
		extratoSaldo.Data_Extrato = data_extrato_str
		extratoSaldo.Limite = limite
	}

	extrato := Extrato{
		extratoSaldo,
		transacoes,
	}

	jsonStr, err := json.Marshal(extrato)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error"))
		return
	}
	io.WriteString(w, string(jsonStr[:]))
}

func obter_dados_rota(url string) (int, string) {
	partes_url := strings.Split(url, "/")
	id, err := strconv.Atoi(partes_url[2])
	if err != nil {
		fmt.Println(err)
		return -1, ""
	}
	tipo_transacao_str := partes_url[3]
	if tipo_transacao_str == "transacoes" {
		return id, "transacoes"
	} else if tipo_transacao_str == "extrato" {
		return id, "extrato"
	} else {
		return -1, ""
	}
}

func obter_cliente_limite_cache(id int) int {
	for clientesDadosAtualizando {
		time.Sleep(10 * time.Millisecond)
	}
	limite, ok := clientesLimites[id]
	if !ok {
		clientesDadosAtualizando = true
		var limite_db int
		err := db.QueryRow(`SELECT limite FROM clientes WHERE cliente_id = $1`, id).Scan(&limite_db)
		if err != nil {
			fmt.Println(err)
			return -1
		}
		clientesLimites[id] = limite_db
		clientesDadosAtualizando = false
		fmt.Printf("Cliente %d inserido no cache com valor de limite igual a %d\n", id, limite_db)
		return limite_db
	} else {
		return limite
	}
}

func obter_cliente_saldo_inicial_cache(id int) int {
	for clientesDadosAtualizando {
		time.Sleep(10 * time.Millisecond)
	}
	saldo_inicial, ok := clientesSaldosIniciais[id]
	if !ok {
		clientesDadosAtualizando = true
		var saldo_inicial_db int
		err := db.QueryRow(`SELECT saldo_inicial FROM clientes WHERE cliente_id = $1`, id).Scan(&saldo_inicial_db)
		if err != nil {
			fmt.Println(err)
			return -1
		}
		clientesSaldosIniciais[id] = saldo_inicial_db
		clientesDadosAtualizando = false
		fmt.Printf("Cliente %d inserido no cache com valor de saldo inicial igual a %d\n", id, saldo_inicial_db)
		return saldo_inicial_db
	} else {
		return saldo_inicial
	}
}

func main() {
	http.HandleFunc("/clientes/", handle_generico)
	http.HandleFunc("/health", handle_health)

	http_port := os.Getenv("HTTP_PORT")
	if http_port == "" {
		http_port = "8080"
	}

	fmt.Println("Servidor iniciado e aguardando conexoes na PORTA " + http_port)
	err := http.ListenAndServe(":"+http_port, nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}

type Transacao struct {
	Valor     int    `json:"valor"`
	Tipo      string `json:"tipo"`
	Descricao string `json:"descricao"`
}

type TransacaoResposta struct {
	Limite int `json:"limite"`
	Saldo  int `json:"saldo"`
}

type Extrato struct {
	Saldo              ExtratoSaldo       `json:"saldo"`
	Ultimas_Transacoes []ExtratoTransacao `json:"ultimas_transacoes,omitempty"`
}

type ExtratoSaldo struct {
	Total        int    `json:"total"`
	Data_Extrato string `json:"data_extrato"`
	Limite       int    `json:"limite"`
}

type ExtratoTransacao struct {
	Valor              int    `json:"valor"`
	Tipo               string `json:"tipo"`
	Descricao          string `json:"descricao"`
	Data_Hora_Inclusao string `json:"realizada_em"`
	Saldo              int    `json:"-"`
}

type Cliente struct {
	Id            int
	Nome          string
	Limite        int
	Saldo_Inicial int
}
