package main

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

var db *sql.DB
var clientesLimites = make(map[int]int)
var clientesSaldosIniciais = make(map[int]int)
var clientesDadosAtualizando = false //map em Go nao eh thread-safe
var modo_ping_on = false
var modo_ping_validacao_on = false
var modo_ping_transacoes_only = false
var modo_ping_transacoes_insert_only = false
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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
	db_max_connections_str := os.Getenv("DB_MAX_CONNECTIONS")
	if db_max_connections_str == "" {
		db_max_connections_str = "10"
	}
	db_max_connections, err := strconv.Atoi(db_max_connections_str)
	if err != nil {
		// ... handle error
		panic(err)
	}

	fmt.Println("Conectando no BD...")
	fmt.Println("DB_HOSTNAME:" + db_hostname)
	fmt.Println("DB_NAME:" + db_name)
	fmt.Println("DB_USERNAME:" + db_username)
	fmt.Println("DB_MAX_CONNECTIONS:" + db_max_connections_str)
	fmt.Println("VERSAO:1.2.0")

	db, err = sql.Open("postgres", "postgres://"+db_username+":"+db_password+"@"+db_hostname+"/"+db_name+"?sslmode=disable")
	if err != nil {
		log.Fatal("Invalid DB config:", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal("DB unreachable:", err)
	}
	db.SetMaxOpenConns(db_max_connections)
	fmt.Println("Conectado OK!!!")

	if os.Getenv("PING_ON") != "" {
		modo_ping_on = true
		fmt.Println("**** EXECUTANDO EM MODO PING ****")
	}

	if os.Getenv("PING_VALIDACAO_ON") != "" {
		modo_ping_validacao_on = true
		fmt.Println("**** EXECUTANDO EM MODO PING VALIDACAO (SEM ACESSO AO BD) ****")
	}

	if os.Getenv("PING_TRANSACOES_ONLY_ON") != "" {
		modo_ping_transacoes_only = true
		fmt.Println("**** EXECUTANDO EM MODO PING COM VALIDACAO E COM A EXECUCAO REAL SOMENTE DO ENPOINT /transacoes ****")
	}

	if os.Getenv("PING_TRANSACOES_INSERT_ONLY_ON") != "" {
		modo_ping_transacoes_insert_only = true
		fmt.Println("**** EXECUTANDO EM MODO PING COM VALIDACAO E COM A EXECUCAO REAL SOMENTE DO ENPOINT /transacoes POREM SOMENTE COM AS INSTRUCOES SQL DE TRANSACOES EXECUTANDO ****")
	}
}

func handle_generico(w http.ResponseWriter, r *http.Request) {
	if modo_ping_on {
		if strings.Contains(r.URL.Path, "extrato") {
			io.WriteString(w, "{\"saldo\": {\"total\": -9098,\"data_extrato\": \"2024-01-17T02:34:41.217753Z\",\"limite\":100000},\"ultimas_transacoes\": [{\"valor\": 10,\"tipo\":\"c\",\"descricao\":\"descricao\",\"realizada_em\":\"2024-01-17T02:34:38.543030Z\"}]}")
		} else {
			io.WriteString(w, "{\"limite\": 1000, \"saldo\": 1000}")
		}
		return
	}

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

func parse_request(request []byte) (Transacao, error) {
	var tamanho_array = len(request)
	var params = make(map[string]string)
outer:
	for i := 0; i < tamanho_array; i++ {
		if request[i] == '"' {
			// comecou um atributo. ir acumulando ate chegar no proximo '"'
			var chave strings.Builder
			for j := i + 1; j < tamanho_array; j++ {
				if request[j] == '"' {
					for k := j + 1; k < tamanho_array; k++ {
						if request[k] == ':' {
							var valor strings.Builder
							for w := k + 1; w < tamanho_array; w++ {
								if request[w] == ',' || request[w] == '}' {
									params[chave.String()] = valor.String()
									i = w + 1
									continue outer
								} else {
									valor.WriteByte(request[w])
								}
							}
						}
					}
					// achou toda a chave, agora pegar o valor
				} else {
					chave.WriteByte(request[j])
				}
			}
		}
	}

	valor_str, ok_valor := params["valor"]
	if !ok_valor {
		return Transacao{}, errors.New("valor nao informado")
	}

	valor, err := strconv.Atoi(strings.Trim(valor_str, " "))
	if err != nil {
		fmt.Println("Erro ao tratar json de entrada - " + err.Error())
		return Transacao{}, errors.New("valor invalido")
	}

	tipo, ok := params["tipo"]
	if !ok {
		return Transacao{}, errors.New("tipo nao informado")
	} else {
		achou := false
		tamanho := len(tipo)
		var valor strings.Builder
	outer_tipo:
		for i := 0; i < tamanho; i++ {
			if tipo[i] == '"' {
				for j := i + 1; j < tamanho; j++ {
					if tipo[j] == '"' {
						tipo = valor.String()
						achou = true
						break outer_tipo
					}
					valor.WriteByte(tipo[j])
				}
			}
		}
		if !achou {
			return Transacao{}, errors.New("tipo invalido")
		}
	}

	descricao, ok := params["descricao"]
	if !ok {
		return Transacao{}, errors.New("descricao nao informada")
	} else {
		achou := false
		tamanho := len(descricao)
		var valor strings.Builder
	outer_descricao:
		for i := 0; i < tamanho; i++ {
			if descricao[i] == '"' {
				for j := i + 1; j < tamanho; j++ {
					if descricao[j] == '"' {
						descricao = valor.String()
						achou = true
						break outer_descricao
					}
					valor.WriteByte(descricao[j])
				}
			}
		}
		if !achou {
			return Transacao{}, errors.New("descricao invalida")
		}
	}

	return Transacao{
		valor,
		tipo,
		descricao,
	}, nil
}

// POST /clientes/[id]/transacoes
func handle_transacoes(w http.ResponseWriter, r *http.Request, id int) {
	var resB bytes.Buffer
	_, err := resB.ReadFrom(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Internal Server Error - resB.ReadFrom(r.Body) - " + err.Error()))
		return
	}

	// var transacao Transacao
	// err = json.Unmarshal(resB.Bytes(), &transacao)
	var transacao, err_parse = parse_request(resB.Bytes())
	if err_parse != nil {
		fmt.Println("Erro ao tratar json de entrada - " + err_parse.Error())
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("422 - Erro ao tratar json de entrada - " + err_parse.Error()))
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

	if modo_ping_validacao_on {
		io.WriteString(w, "{\"limite\": 1000, \"saldo\": 1000}")
		return
	}

	var saldo string
	if modo_ping_transacoes_insert_only {
		_, err := db.Exec("insert into transacoes (cliente_id, valor, descricao, tipo, saldo) values ($1, $2, $3, $4, 0)", id, transacao.Valor, transacao.Descricao, transacao.Tipo)
		if err != nil {
			fmt.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("500 - Internal Server Error - " + err.Error()))
			return
		}
	} else {
		if transacao.Tipo == "c" {
			err := db.QueryRow("with novo_saldo as (UPDATE saldos SET saldo = saldo + $1 WHERE cliente_id = $2 RETURNING saldo) insert into transacoes (cliente_id, valor, descricao, tipo, saldo) values ($3, $4, $5, $6, (select * from novo_saldo)) returning saldo", transacao.Valor, id, id, transacao.Valor, transacao.Descricao, transacao.Tipo).Scan(&saldo)
			if err != nil {
				if strings.Contains(err.Error(), "not-null constraint") {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte("404 - Cliente nao encontrado"))
				} else {
					fmt.Println(err)
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("500 - Internal Server Error - " + err.Error()))
				}
				return
			}
		} else {
			err := db.QueryRow("with novo_saldo as (UPDATE saldos SET saldo = saldo - $1 WHERE cliente_id = $2 RETURNING saldo) insert into transacoes (cliente_id, valor, descricao, tipo, saldo) values ($3, $4, $5, $6, (select * from novo_saldo)) returning saldo", transacao.Valor, id, id, transacao.Valor, transacao.Descricao, transacao.Tipo).Scan(&saldo)
			if err != nil {
				if strings.Contains(err.Error(), "not-null constraint") {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte("404 - Cliente nao encontrado"))
				} else if strings.Contains(err.Error(), "check constraint") || strings.Contains(err.Error(), "saldo_valido") {
					w.WriteHeader(http.StatusUnprocessableEntity)
					w.Write([]byte("422 - Sem saldo"))
				} else {
					fmt.Println(err)
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("500 - Internal Server Error - " + err.Error()))
				}
				return
			}
		}
	}

	limite := obter_cliente_limite_cache(id)
	// transacaoResposta := TransacaoResposta{limite, saldo}
	// jsonStr, err := json.Marshal(transacaoResposta)
	// if err != nil {
	// 	fmt.Println(err)
	// 	w.WriteHeader(http.StatusInternalServerError)
	// 	w.Write([]byte("500 - Internal Server Error - " + err.Error()))
	// 	return
	// }

	var json strings.Builder
	json.WriteString("{\"limite\":")
	json.WriteString(strconv.Itoa(limite))
	json.WriteString(",\"saldo\":")
	json.WriteString(saldo)
	json.WriteString("}")
	io.WriteString(w, json.String())
}

// GET /clientes/[id]/extrato
func handle_extrato(w http.ResponseWriter, r *http.Request, id int) {
	if modo_ping_validacao_on || modo_ping_transacoes_only || modo_ping_transacoes_insert_only {
		io.WriteString(w, "{\"saldo\": {\"total\": -9098,\"data_extrato\": \"2024-01-17T02:34:41.217753Z\",\"limite\":100000},\"ultimas_transacoes\": [{\"valor\": 10,\"tipo\":\"c\",\"descricao\":\"descricao\",\"realizada_em\":\"2024-01-17T02:34:38.543030Z\"}]}")
		return
	}

	rows, err := db.Query("select valor, tipo, descricao, data_hora_inclusao, saldo from transacoes where cliente_id = $1 order by data_hora_inclusao desc limit 10", id)
	if err != nil {
		if err != sql.ErrNoRows {
			fmt.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("500 - Internal Server Error - " + err.Error()))
			return
		}
	}

	var json strings.Builder
	json.WriteString("{\"saldo\":{\"total\":")

	var achou_linhas = false
	var primeira_linha = true
	var escreveu_extrato = false
	data_extrato_str := time.Now().Format("2006-01-02T15:04:05.000000Z")
	for rows.Next() {
		var transacao ExtratoTransacao
		err = rows.Scan(&transacao.Valor, &transacao.Tipo, &transacao.Descricao, &transacao.Data_Hora_Inclusao, &transacao.Saldo)
		if err == sql.ErrNoRows {
			break
		} else if err != nil {
			fmt.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("500 - Internal Server Error - " + err.Error()))
			return
		}
		if !escreveu_extrato {
			json.WriteString(strconv.Itoa(transacao.Saldo))
			json.WriteString(",\"data_extrato\":\"")
			json.WriteString(data_extrato_str)
			json.WriteString("\",\"limite\":")
			json.WriteString(strconv.Itoa(obter_cliente_limite_cache(id)))
			json.WriteString("},\"ultimas_transacoes\":[")
			escreveu_extrato = true
		}

		if !primeira_linha {
			json.WriteString(",")
		}
		json.WriteString("{\"valor\":")
		json.WriteString(strconv.Itoa(transacao.Valor))
		json.WriteString(",\"tipo\":\"")
		json.WriteString(transacao.Tipo)
		json.WriteString("\",\"descricao\":\"")
		json.WriteString(transacao.Descricao)
		json.WriteString("\",\"realizada_em\":\"")
		json.WriteString(transacao.Data_Hora_Inclusao)
		json.WriteString("\"}")
		primeira_linha = false
		achou_linhas = true
	}
	rows.Close()

	if achou_linhas {
		json.WriteString("]}")
	} else {
		var saldoInicial = obter_cliente_saldo_inicial_cache(id)
		if saldoInicial == -1 {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("404 - Cliente nao encontrado"))
			return
		} else {
			json.WriteString(strconv.Itoa(saldoInicial))
			json.WriteString(",\"data_extrato\":\"")
			json.WriteString(data_extrato_str)
			json.WriteString("\",\"limite\":")
			json.WriteString(strconv.Itoa(obter_cliente_limite_cache(id)))
			json.WriteString("}}")
		}
	}
	io.WriteString(w, json.String())
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
		err := db.QueryRow("SELECT limite FROM clientes WHERE cliente_id = $1", id).Scan(&limite_db)
		if err != nil {
			fmt.Printf("Erro ao obter limite do cliente %d - %s\n", id, err.Error())
			clientesDadosAtualizando = false
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
		fmt.Printf("Aguardando atualizacao do cache por 10 milis")
		time.Sleep(10 * time.Millisecond)
	}
	saldo_inicial, ok := clientesSaldosIniciais[id]
	if !ok {
		clientesDadosAtualizando = true
		var saldo_inicial_db int
		err := db.QueryRow("SELECT saldo_inicial FROM clientes WHERE cliente_id = $1", id).Scan(&saldo_inicial_db)
		if err != nil {
			fmt.Printf("Erro ao obter saldo_inicial do cliente %d - %s", id, err.Error())
			clientesDadosAtualizando = false
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
