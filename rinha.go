package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
)

var dbpool *pgxpool.Pool
var clientesLimites = make(map[int]int)
var clientesDadosAtualizando = false //map em Go nao eh thread-safe
// var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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
	fmt.Println("VERSAO:1.0.0 - usando channel")

	config, err := pgxpool.ParseConfig("postgres://" + db_username + ":" + db_password + "@" + db_hostname + "/" + db_name + "?sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "DB config error: %v\n", err)
		os.Exit(1)
	}
	config.MinConns = int32(db_max_connections)
	config.MaxConns = int32(db_max_connections)

	dbpool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}
	if err = dbpool.Ping(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "DB unreacheble: %v\n", err)
		os.Exit(1)
	}
	//dbpool.SetMaxOpenConns(db_max_connections)
	fmt.Println("Conectado OK!!!")
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
									if chave.String() == "valor" {
										if (request[w] < 48 || request[w] > 57) && request[w] != 32 {
											return Transacao{}, errors.New("valor invalido")
										}
										if request[w] >= 48 && request[w] <= 57 {
											valor.WriteByte(request[w])
										}
									} else {
										for y := w + 1; y < tamanho_array; y++ {
											if request[y] == ',' || request[y] == '}' {
												return Transacao{}, errors.New("nao achou aspas")
											}
											if request[y] == '"' {
												for z := y + 1; z < tamanho_array; z++ {
													if request[z] == '"' {
														params[chave.String()] = valor.String()
														i = z + 1
														continue outer
													} else {
														valor.WriteByte(request[z])
													}
												}
											}
										}
									}
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

	valor, err := strconv.Atoi(valor_str)
	if err != nil {
		fmt.Println("Erro ao tratar json de entrada - " + err.Error())
		return Transacao{}, errors.New("valor invalido")
	}

	tipo, ok := params["tipo"]
	if !ok {
		return Transacao{}, errors.New("tipo nao informado")
	}

	descricao, ok := params["descricao"]
	if !ok {
		return Transacao{}, errors.New("descricao nao informada")
	}

	return Transacao{
		valor,
		tipo,
		descricao,
	}, nil
}

func crebitar(transacao Transacao, id int, limite int) (int, string) {
	var json_saldo string
	var err_crebitar error
	if transacao.Tipo == "c" {
		err_crebitar = dbpool.QueryRow(context.Background(), "select creditar($1, $2, $3, $4, $5)", id, limite, transacao.Valor, transacao.Tipo, transacao.Descricao).Scan(&json_saldo)
	} else {
		err_crebitar = dbpool.QueryRow(context.Background(), "select debitar($1, $2, $3, $4, $5)", id, limite, transacao.Valor, transacao.Tipo, transacao.Descricao).Scan(&json_saldo)
	}
	if err_crebitar != nil {
		if strings.Contains(err_crebitar.Error(), "not-null constraint") {
			return 404, "-1"
		} else if strings.Contains(err_crebitar.Error(), "check") {
			return 422, "-1"
		} else {
			fmt.Println(err_crebitar)
			return 500, "-1"
		}
	} else {
		return 200, json_saldo
	}
}

func extrato_mock(id int, limite int) (string, error) {
	var json strings.Builder
	data_extrato_str := time.Now().Format("2006-01-02T15:04:05.000000Z")
	json.WriteString("{\"saldo\":{\"total\":0")
	json.WriteString(",\"data_extrato\":\"")
	json.WriteString(data_extrato_str)
	json.WriteString("\",\"limite\":")
	json.WriteString(strconv.Itoa(limite))
	json.WriteString("},\"ultimas_transacoes\":[")
	json.WriteString("]}")
	return json.String(), nil
}

func extrato(id int, limite int) (string, error) {
	ch := make(chan string)
	go func() {
		rows, err := dbpool.Query(context.Background(), `
			select '{"valor":'||valor||',"tipo":"'||tipo||'","descricao":"'||descricao||'","realizada_em":"'||data_hora_inclusao||'"}', '{"saldo":{"total":'||saldo||',"data_extrato":"'||now()||'","limite":' 
			from transacoes 
			where cliente_id = $1 
			order by id 
			desc limit 10
		   `, id)
		if err != nil {
			fmt.Println(err)
			ch <- " "
			return
		}
		i := 0
		var json strings.Builder
		for rows.Next() {
			var json_inicio string
			var json_transacao string
			err = rows.Scan(&json_transacao, &json_inicio)
			if err != nil {
				fmt.Println(err)
				ch <- " "
				rows.Close()
				return
			}
			if i > 0 {
				json.WriteString(",")
			} else {
				json.WriteString(json_inicio)
				json.WriteString(strconv.Itoa(limite))
				json.WriteString("},\"ultimas_transacoes\":[")
			}
			i++
			json.WriteString(json_transacao)
		}
		rows.Close()
		if i == 0 {
			data_extrato_str := time.Now().Format("2006-01-02T15:04:05.000000Z")
			ch <- "{\"saldo\":{\"total\":0,\"data_extrato\":\"" + data_extrato_str + "\",\"limite\":" + strconv.Itoa(limite) + "}}"
			return
		}
		json.WriteString("]}")
		ch <- json.String()
	}()
	received_json := <-ch
	if received_json[0] == '{' {
		return received_json, nil
	} else {
		return received_json, errors.New("erro inesperado")
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
		err := dbpool.QueryRow(context.Background(), "SELECT limite FROM clientes WHERE cliente_id = $1", id).Scan(&limite_db)
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

func setupFiber(http_port string) {
	app := fiber.New()

	// Define a route for the GET method on the root path '/'
	app.Post("/clientes/:id/transacoes", func(c fiber.Ctx) error {
		id, err := strconv.Atoi(c.Params("id"))
		if err != nil {
			return c.SendStatus(http.StatusNotFound)
		}
		limite := obter_cliente_limite_cache(id)
		if limite == -1 {
			return c.SendStatus(http.StatusNotFound)
		}

		transacao, err := parse_request(c.Body())
		if err != nil {
			return c.SendStatus(http.StatusUnprocessableEntity)
		}
		if transacao.Valor <= 0 {
			return c.SendStatus(http.StatusUnprocessableEntity)
		}

		if transacao.Tipo != "c" && transacao.Tipo != "d" {
			return c.SendStatus(http.StatusUnprocessableEntity)
		}

		var descricao_tamanho = len(transacao.Descricao)
		if descricao_tamanho < 1 || descricao_tamanho > 10 {
			return c.SendStatus(http.StatusUnprocessableEntity)
		}

		http_code, json_resultado := crebitar(transacao, id, limite)
		if http_code == 404 {
			return c.SendStatus(http.StatusNotFound)
		} else if http_code == 422 {
			return c.SendStatus(http.StatusUnprocessableEntity)
		} else if http_code == 500 {
			return c.SendStatus(http.StatusInternalServerError)
		}
		return c.SendString(json_resultado)
	})
	app.Get("/clientes/:id/extrato", func(c fiber.Ctx) error {
		id, err := strconv.Atoi(c.Params("id"))
		if err != nil {
			return c.SendStatus(http.StatusNotFound)
		}

		limite := obter_cliente_limite_cache(id)
		if limite == -1 {
			return c.SendStatus(http.StatusNotFound)
		}

		json, err := extrato(id, limite)
		if err != nil {
			return c.SendStatus(http.StatusInternalServerError)
		}

		return c.SendString(json)
	})

	// Start the server on port 3000
	app.Listen(":" + http_port)
}

func main() {
	http_port := os.Getenv("HTTP_PORT")
	if http_port == "" {
		http_port = "8080"
	}
	setupFiber(http_port)
	//setupDefault(http_port)
	fmt.Println("Servidor iniciado e aguardando conexoes na PORTA " + http_port)
}

type Transacao struct {
	Valor     int    `json:"valor"`
	Tipo      string `json:"tipo"`
	Descricao string `json:"descricao"`
}

type TransacaoUri struct {
	Id int `uri:"id" binding:"required"`
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
