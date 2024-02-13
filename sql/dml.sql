INSERT INTO cliente (cliente_id, nome, limite)
VALUES
    (1, 'o barato sai caro', 1000 * 100),
    (2, 'zan corp ltda', 800 * 100),
    (3, 'les cruders', 10000 * 100),
    (4, 'padaria joia de cocaia', 100000 * 100),
    (5, 'kid mais', 5000 * 100);

INSERT INTO saldo (cliente_id, saldo, limite)
VALUES 
    (1, (select saldo_inicial from cliente where cliente_id = 1), (select limite from cliente where cliente_id = 1)),
    (2, (select saldo_inicial from cliente where cliente_id = 2), (select limite from cliente where cliente_id = 2)),
    (3, (select saldo_inicial from cliente where cliente_id = 3), (select limite from cliente where cliente_id = 3)),
    (4, (select saldo_inicial from cliente where cliente_id = 4), (select limite from cliente where cliente_id = 4)),
    (5, (select saldo_inicial from cliente where cliente_id = 5), (select limite from cliente where cliente_id = 5));
