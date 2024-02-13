create table transacao (
    cliente_id int not null,
    valor numeric not null,
    descricao varchar(10) not null,
    data_hora_inclusao timestamp default NOW()
);

create table cliente (
    cliente_id int not null,
    nome varchar(100) not null,
    limite numeric not null,
    saldo_inicial numeric not null,
    primary key(cliente)
);

create unlogged table saldo (
    cliente_id int not null,
    saldo numeric not null CHECK (saldo > (limite * -1)),
    limite numeric not null,
    primary key(cliente)
);