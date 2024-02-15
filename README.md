# Resumo
Programa para participar da Rinha backend 2024 - Q1 (https://github.com/zanfranceschi/rinha-de-backend-2024-q1)

# Stack

```
Banco de Dados - PostgreSQL
Linguagem - Go
```

# Estratégia

- Tratar o controle de concorrência no banco de dados.
- Utilizar tabela unlogged para controle do saldo pois é mais eficiente no acesso concorrente
- Utilizar atualização do saldo e inclusão de registro na tabela de transação em uma unica instrução, garatindo atomicidade nas duas operações (ou vai tudo ou não vai nada)

# Premissas

- Se o BD cair, é preciso utilizar endpoint de recuperação (/recovery) antes de re-submeter as requisições (apesar de resiliencia estar fora do escopo da rinha)

# Exemplos

## Transação

```
curl -d '{"valor": 1000, "tipo" : "c", "descricao" : "sasdo"}' -H "Content-Type: application/json" -X POST http://localhost:8081/clientes/1/transacoes
```

## Extrato

```
curl -H "Content-Type: application/json" -X GET http://localhost:8081/clientes/1/extrato
```

# Build imagem Docker

docker build -t rinha-backend-2024-q1-go .

# Executar imagem Docker

docker run -it --rm --net="host" rinha-backend-2024-q1-go

# Push to Docker Hub

docker tag rinha-backend-2024-q1-go hfurlan/rinha-backend-2024-q1:1.0.0-go
docker push hfurlan/rinha-backend-2024-q1:1.0.0-go



