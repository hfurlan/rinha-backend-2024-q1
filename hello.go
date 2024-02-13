package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func handle(w http.ResponseWriter, r *http.Request) {
	id, tipo_transacao := obter_dados_rota(r.URL.Path)
	if tipo_transacao == 0 {
		handle_transacao(w, r, id)
	} else if tipo_transacao == 1 {
		handle_extrato(w, r, id)
	} else {
		// return 404
	}
}

// POST /clientes/[id]/transacoes
func handle_transacao(w http.ResponseWriter, r *http.Request, id int) {
	var resB bytes.Buffer
	_, err := resB.ReadFrom(r.Body)
	if err != nil {
		fmt.Println("Erro: resB.ReadFrom")
		fmt.Println(err)
		return
	}

	var transacao Transacao
	err = json.Unmarshal(resB.Bytes(), &transacao)
	if err != nil {
		fmt.Printf("ERRO: resB.Bytes(%s)", resB.String())
		fmt.Println("json.Unmarshal")
		fmt.Println(err)
		return
	}

	// chamar repositorio inserindo a linha de transacao e atualizando o saldo
	//update saldo set saldo = saldo + valor returning saldo
	transacaoResposta := TransacaoResposta{1, 2}
	jsonStr, err := json.Marshal(transacaoResposta)
	if err != nil {
		fmt.Println(err)
		return
	}
	io.WriteString(w, string(jsonStr[:]))
}

// GET /clientes/[id]/extrato
func handle_extrato(w http.ResponseWriter, r *http.Request, id int) {
	//Recuperar os dados basicos do cliente
	//select * from cliente where cliente_id = X

	//Recuperar as ultimas 10 transacoes com o saldo atualizado
	//select s.saldo, t.* from transacao t join saldo s on t.cliente_id = s.cliente_id where t.cliente_id = X limit 10

	extratoSaldo := ExtratoSaldo{
		Total       int    `json:total`
		DataExtrato string `json:data_extrato`
		cliente.Limite
	}

	extratoTransacao := ExtratoTransacao{

	}

	extrato := Extrato {
		extratoSaldo,
	}

	jsonStr, err := json.Marshal(extrato)
	if err != nil {
		fmt.Println(err)
		return
	}
	io.WriteString(w, string(jsonStr[:]))
}

func obter_dados_rota(url string) (int, int) {
	partes_url := strings.Split(url, "/")
	id, err := strconv.Atoi(partes_url[1])
	if err != nil {
		return -1, -1
	}
	tipo_transacao_str := partes_url[2]
	if tipo_transacao_str == "transacao" {
		return id, 0
	} else if tipo_transacao_str == "extrato" {
		return id, 1
	} else {
		return -1, -1
	}
}

func main() {
	http.HandleFunc("/clientes/", handle)

	err := http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}

type Transacao struct {
	Valor     int    `json:valor`
	Tipo      string `json:tipo`
	Descricao string `json:descricao`
}

type TransacaoResposta struct {
	Limite int `json:limite`
	Saldo  int `json:saldo`
}

type Extrato struct {
	Saldo             ExtratoSaldo       `json:saldo`
	UltimasTransacoes []ExtratoTransacao `json:ultimas_transacoes`
}

type ExtratoSaldo struct {
	Total       int    `json:total`
	DataExtrato string `json:data_extrato`
	Limite      int    `json:limite`
}

type ExtratoTransacao struct {
	Valor       int    `json:valor`
	Tipo        string `json:tipo`
	Descricao   string `json:descricao`
	RealizadaEm string `json:realizada_em`
}
