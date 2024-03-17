create table transacoes (
    id int generated always as identity,
    cliente_id int not null,
    valor int,
    descricao varchar(10),
    tipo char(1),
    saldo int,
    data_hora_inclusao timestamp default NOW(),
    primary key(cliente_id, id)
) partition by range (cliente_id);

create table clientes (
    cliente_id int not null,
    nome varchar(100) not null,
    limite int not null constraint limite_positivo check (limite >= 0),
    saldo_inicial int not null constraint saldo_inicial_positivo check (saldo_inicial >= 0),
    primary key(cliente_id)
);

create unlogged table saldos (
    cliente_id int not null,
    saldo int not null constraint saldo_valido check (saldo >= (limite * -1)),
    limite int not null,
    primary key(cliente_id)
);

create or replace function inserir_saldo()
  returns TRIGGER 
  language PLPGSQL
  as
$$
begin
    execute format('CREATE TABLE %I PARTITION OF transacoes FOR VALUES FROM (%s) TO (%s)', 'transacoes_' || cast(NEW.cliente_id as varchar), NEW.cliente_id, NEW.cliente_id + 1);
    insert into saldos (cliente_id, saldo, limite) values (NEW.cliente_id, NEW.saldo_inicial, NEW.limite);
    return NEW;
end;
$$;

create or replace function remover_saldo()
  returns TRIGGER 
  language PLPGSQL
  as
$$
begin
    execute format('DROP TABLE %I', 'transacoes_' || cast(OLD.cliente_id as varchar), 'transacoes_' || cast(NEW.cliente_id as varchar));
    delete from saldos where cliente_id = OLD.cliente_id;
    return OLD;
end;
$$;

create or replace trigger clientes_inserir_saldo
    after insert on clientes
    for each row
    execute function inserir_saldo();

create or replace trigger clientes_remover_saldo
    after delete ON clientes
    for each row
    execute function remover_saldo();

create function creditar(p_cliente_id int, p_limite int, p_valor int, p_tipo char(1), p_descricao varchar(10)) RETURNS varchar AS $$
declare
  saldo_atualizado int;
  json_resultado varchar(30); 
begin
  perform pg_advisory_xact_lock(p_cliente_id);
  update saldos set saldo = saldo + p_valor where cliente_id = p_cliente_id returning saldo into saldo_atualizado;
  insert into transacoes (cliente_id, valor, descricao, tipo, saldo) values (p_cliente_id, p_valor, p_descricao, 'c', saldo_atualizado);
  return '{"limite":'||p_limite||',"saldo":'||saldo_atualizado||'}';
end;
$$ LANGUAGE plpgsql;

create function debitar(p_cliente_id int, p_limite int, p_valor int, p_tipo varchar(1), p_descricao varchar(10)) RETURNS varchar AS $$
declare
  saldo_atualizado int;
  json_resultado varchar(30); 
begin
  perform pg_advisory_xact_lock(p_cliente_id);
  update saldos set saldo = saldo - p_valor where cliente_id = p_cliente_id returning saldo into saldo_atualizado;
  insert into transacoes (cliente_id, valor, descricao, tipo, saldo) values (p_cliente_id, p_valor, p_descricao, 'd', saldo_atualizado);
  return '{"limite":'||p_limite||',"saldo":'||saldo_atualizado||'}';
end;
$$ LANGUAGE plpgsql;
