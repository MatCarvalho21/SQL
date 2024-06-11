DROP SCHEMA IF EXISTS audit CASCADE;
CREATE SCHEMA audit;
SET search_path=audit;

/***
Início do código
***/

--
-- PASSO 1: Gravar as alterações em uma tabela e criar função de trigger para gravar as alterações de forma geral
--

CREATE TABLE audit.historico_mudancas_CHO (
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    user_name TEXT,
    action_tstamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    action TEXT NOT NULL CHECK (action IN ('I', 'D', 'U')),
    original_data TEXT,
    new_data TEXT,
    query TEXT
) WITH (fillfactor=100);

CREATE OR REPLACE FUNCTION audit.if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.historico_mudancas_CHO (schema_name, table_name, user_name, action, original_data, new_data, query)
        VALUES (TG_TABLE_SCHEMA::TEXT, TG_TABLE_NAME::TEXT, session_user::TEXT, substring(TG_OP, 1, 1), v_old_data, v_new_data, current_query());
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        v_old_data := ROW(OLD.*);
        INSERT INTO audit.historico_mudancas_CHO (schema_name, table_name, user_name, action, original_data, query)
        VALUES (TG_TABLE_SCHEMA::TEXT, TG_TABLE_NAME::TEXT, session_user::TEXT, substring(TG_OP, 1, 1), v_old_data, current_query());
        RETURN OLD;
    ELSIF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.historico_mudancas_CHO (schema_name, table_name, user_name, action, new_data, query)
        VALUES (TG_TABLE_SCHEMA::TEXT, TG_TABLE_NAME::TEXT, session_user::TEXT, substring(TG_OP, 1, 1), v_new_data, current_query());
        RETURN NEW;
    ELSE
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %', TG_OP, NOW();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;


--
-- PASSO 2: Salvar modificações das tabelas utilizadas nas dimensões como registro de log
--

-- 
-- PASSO 3: Triggers específicos para salvar inserções em tabelas espelho
--

/***
FILIAL
***/
-- Para salvar as modificações na tabela Filial
CREATE TRIGGER filial_trg
AFTER INSERT OR UPDATE OR DELETE ON cho_db.filial
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Cria a tabela audit.ins_filial como uma cópia vazia de cho_db.filial
Create table audit.ins_filial as select * from cho_db.filial where 1=0; 

/***
Trigger para salvar inserções
***/
-- Função para capturar inserções na tabela Filial e salvá-las na tabela audit.ins_filial
CREATE OR REPLACE FUNCTION audit.ins_filial_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_filial VALUES (NEW.filialid, NEW.filialrua, NEW.filialbairro, NEW.filialmunicipio, NEW.filialestado);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_filial_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_filial_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_filial_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_filial_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Filial e chamar a função audit.ins_filial_func
DROP TRIGGER IF EXISTS filiar_trg_insert ON cho_db.filial;
CREATE TRIGGER filiar_trg_insert
AFTER INSERT ON cho_db.filial
FOR EACH ROW EXECUTE PROCEDURE audit.ins_filial_func();


/***
CLIENTE
***/
-- Para salvar as modificações na tabela Cliente
CREATE TRIGGER cliente_trg
AFTER INSERT OR UPDATE OR DELETE ON cho_db.cliente
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Cria a tabela audit.ins_cliente como uma cópia vazia de cho_db.cliente
Create table audit.ins_cliente as select * from cho_db.cliente where 1=0; 

/***
Trigger para salvar inserções
***/
-- Função para capturar inserções na tabela Cliente e salvá-las na tabela audit.ins_cliente
CREATE OR REPLACE FUNCTION audit.ins_cliente_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_cliente VALUES (NEW.clienteid, NEW.clientenome, NEW.clientesobrenome, NEW.clientetiposang, 
                                              NEW.clienterua, NEW.clientebairro, NEW.clientemunicipio, NEW.clienteestado, 
                                              NEW.clientecpf, NEW.clientedatanasc);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_cliente_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_cliente_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_cliente_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_cliente_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Cliente e chamar a função audit.ins_cliente_func
DROP TRIGGER IF EXISTS cliente_trg_insert ON cho_db.cliente;
CREATE TRIGGER cliente_trg_insert
AFTER INSERT ON cho_db.cliente
FOR EACH ROW EXECUTE PROCEDURE audit.ins_cliente_func();


/***
PEDIDO
***/
-- Para salvar as modificações na tabela Pedido
CREATE TRIGGER pedido_trg
AFTER INSERT OR UPDATE OR DELETE ON cho_db.pedido
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Cria a tabela audit.ins_pedido como uma cópia vazia de cho_db.pedido
Create table audit.ins_pedido as select * from cho_db.pedido where 1=0; 

/***
Trigger para salvar inserções
***/
-- Função para capturar inserções na tabela Pedido e salvá-las na tabela audit.ins_pedido
CREATE OR REPLACE FUNCTION audit.ins_pedido_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_pedido VALUES (NEW.pedidoid, NEW.pedidodata, NEW.clienteid, NEW.filialid);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_pedido_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_pedido_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_pedido_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_pedido_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Pedido e chamar a função audit.ins_pedido_func
DROP TRIGGER IF EXISTS pedido_trg_insert ON cho_db.pedido;
CREATE TRIGGER pedido_trg_insert
AFTER INSERT ON cho_db.pedido
FOR EACH ROW EXECUTE PROCEDURE audit.ins_pedido_func();


/***
PEDIDO ITEM
***/
-- Para salvar as modificações na tabela PedidoItem
CREATE TRIGGER pedidoitem_trg
AFTER INSERT OR UPDATE OR DELETE ON cho_db.pedidoitem
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Cria a tabela audit.ins_pedidoitem como uma cópia vazia de cho_db.pedidoitem
Create table audit.ins_pedidoitem as select * from cho_db.pedidoitem where 1=0; 

/***
Trigger para salvar inserções
***/
-- Função para capturar inserções na tabela PedidoItem e salvá-las na tabela audit.ins_pedidoitem
CREATE OR REPLACE FUNCTION audit.ins_pedidoitem_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_pedidoitem VALUES (NEW.quantidade, NEW.pedidoid, NEW.itemid);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_pedidoitem_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_pedidoitem_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_pedidoitem_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_pedidoitem_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela PedidoItem e chamar a função audit.ins_pedidoitem_func
DROP TRIGGER IF EXISTS pedidoitem_trg_insert ON cho_db.pedidoitem;
CREATE TRIGGER pedidoitem_trg_insert
AFTER INSERT ON cho_db.pedidoitem
FOR EACH ROW EXECUTE PROCEDURE audit.ins_pedidoitem_func();


/***
ITEM
***/
-- Para salvar as modificações na tabela Item
CREATE TRIGGER item_trg
AFTER INSERT OR UPDATE OR DELETE ON cho_db.item
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

/***
Trigger para salvar inserções
***/
-- Cria a tabela audit.ins_item como uma cópia vazia de cho_db.item
Create table audit.ins_item as select * from cho_db.item where 1=0; 

-- Função para capturar inserções na tabela Item e salvá-las na tabela audit.ins_item
CREATE OR REPLACE FUNCTION audit.ins_item_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_item VALUES (NEW.itemid, NEW.itemnome, NEW.itemcategoria, NEW.itemprecovenda);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_item_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_item_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_item_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_item_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Item e chamar a função audit.ins_item_func
DROP TRIGGER IF EXISTS item_trg_insert ON cho_db.item;
CREATE TRIGGER item_trg_insert
AFTER INSERT ON cho_db.item
FOR EACH ROW EXECUTE PROCEDURE audit.ins_item_func();


-- 
-- PASSO 4: Atualizar as dimensões no Datawarehouse
--

/*****
Atualiza a dimensão do calendário na tabela cho.CalendarioDimension com base nos dados da tabela audit.ins_pedido. 
A função audit.ins_dim_calendario_func captura inserções na tabela audit.ins_pedido e, para cada nova inserção,
insere uma nova linha na tabela cho.CalendarioDimension se a data correspondente não estiver presente. 
Os campos são gerados a partir da data do pedido, incluindo chave do calendário, data completa, dia da semana, dia, mês, trimestre e ano.
Depois de inserir os dados, a tabela audit.ins_filial é truncada para preparação de novas inserções.
******/
CREATE OR REPLACE FUNCTION audit.ins_dim_calendario_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
v_new_data := ROW(NEW.*);
INSERT INTO cho.CalendarioDimension (
  CalendarioKEY,
  DataCompleta,
  DiaDaSemana,
  Dia,
  Mes,
  Trimestre,
  Ano
)
SELECT
    gen_random_uuid(),
	a.datacompleta,
	a.diasemana,
	a.dia,
	a.mes,
	a.trimestre,
	a.ano
FROM (
  SELECT DISTINCT
    p.PedidoData as datacompleta,
    to_char(p.PedidoData, 'Day') as diasemana,
    extract(day from p.PedidoData) as dia,
    to_char(p.PedidoData, 'Month') as mes,
    cast(to_char(p.PedidoData, 'Q') as int) as trimestre,
    extract(year from p.PedidoData) as ano
  FROM 
    audit.ins_pedido p 
  WHERE p.PedidoData NOT IN (SELECT DataCompleta FROM cho.CalendarioDimension)
) as a;
RETURN NEW;
else
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
RETURN NULL;
end if;

EXCEPTION
WHEN data_exception THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN unique_violation THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN others THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Trigger para que uma nova data for adicionado, atualizamos a dimensão de calendário, se for uma data nova
CREATE TRIGGER dim_calendario_insert_trg
AFTER INSERT ON audit.ins_pedido
EXECUTE PROCEDURE audit.ins_dim_calendario_func();


/*****
Atualiza a dimensão de filial na tabela cho.FilialDimension com base nas inserções na tabela audit.ins_filial.
A função audit.ins_dim_filial_func captura inserções na tabela audit.ins_filial e insere os dados correspondentes na tabela cho.FilialDimension.
Depois de inserir os dados, a tabela audit.ins_filial é truncada para preparação de novas inserções.
******/
CREATE OR REPLACE FUNCTION audit.ins_dim_filial_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
v_new_data := ROW(NEW.*);
INSERT INTO cho.FilialDimension (FilialKEY, FilialID, FilialRua, FilialBairro, FilialMunicipio, FilialEstado) SELECT gen_random_uuid(), filialid, filialrua, filialbairro, filialmunicipio, filialestado FROM audit.ins_filial;

RETURN NEW;
else
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
RETURN NULL;
end if;

EXCEPTION
WHEN data_exception THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN unique_violation THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN others THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Trigger para que uma nova filial for adicionado, atualizamos a dimensão filial
CREATE TRIGGER dim_filial_insert_trg
AFTER INSERT ON audit.ins_filial
EXECUTE PROCEDURE audit.ins_dim_filial_func();


/*****
Atualiza a dimensão de itens na tabela cho.TipoPratoDimension com base nas inserções na tabela audit.ins_item.
A função audit.ins_dim_Item_func captura inserções na tabela audit.ins_item e insere os dados correspondentes na tabela cho.TipoPratoDimension.
Depois de inserir os dados, a tabela audit.ins_item é truncada para preparação de novas inserções.
******/
CREATE OR REPLACE FUNCTION audit.ins_dim_Item_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
v_new_data := ROW(NEW.*);
INSERT INTO cho.TipoPratoDimension (TipoPratoKEY, ItemID, ItemCategoria)
SELECT gen_random_uuid(), itemid, itemcategoria
FROM audit.ins_item;

RETURN NEW;
else
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
RETURN NULL;
end if;

EXCEPTION
WHEN data_exception THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN unique_violation THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN others THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Trigger para que um novo produto for adicionado, atualizamos a dimensão produto 
CREATE TRIGGER dim_item_insert_trg
AFTER INSERT ON audit.ins_item
EXECUTE PROCEDURE audit.ins_dim_Item_func();


/*****
Atualiza as dimensões relacionadas ao cliente nas tabelas cho.ClienteDimension, cho.EndereçoClienteDimension e cho.TipoSangClienteDimension com base nas inserções na tabela audit.ins_cliente.
A função audit.ins_dim_cliente_func captura inserções na tabela audit.ins_cliente e insere os dados correspondentes nas tabelas de dimensão de cliente, endereço e tipo sanguíneo.
Depois de inserir os dados, a tabela audit.ins_cliente é truncada para preparação de novas inserções.
******/
CREATE OR REPLACE FUNCTION audit.ins_dim_cliente_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
v_new_data := ROW(NEW.*);

INSERT INTO cho.ClienteDimension (
  ClienteKEY,
  ClienteID,
  ClienteNomeCompleto,
  ClienteCPF
)
SELECT
  gen_random_uuid(),
  clienteid,
  CONCAT(clientenome, ' ', clientesobrenome),
  clientecpf
FROM
  audit.ins_cliente;

INSERT INTO cho.EndereçoClienteDimension (
  EnderecoKEY,
  Logradouro,
  Bairro,
  Municipio,
  Estado
)
SELECT
  gen_random_uuid(),
  clienterua,
  clientebairro,
  clientemunicipio,
  clienteestado
FROM
  audit.ins_cliente;

INSERT INTO cho.TipoSangClienteDimension (
    TipoSangKEY,
    ClienteTipoSang
)
SELECT
    gen_random_uuid(),
    clientetiposang
FROM
    audit.ins_cliente;

RETURN NEW;
else
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
RETURN NULL;
end if;

EXCEPTION
WHEN data_exception THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN unique_violation THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN others THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Trigger para que um novo cliente for adicionado, atualizamos a dimensão cliente, tipo sanguineo do cliente e endereço cliente
CREATE TRIGGER dim_cliente_trg_insert
AFTER INSERT ON audit.ins_cliente
EXECUTE PROCEDURE audit.ins_dim_cliente_func();

/*****
Atualiza as tabela de fatos detalhada com base nas inserções realizadas.

As tabelas pedido e pedido item devem ser atualizadas.
******/
CREATE OR REPLACE FUNCTION audit.ins_Receita_Detalhada_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
v_new_data := ROW(NEW.*);
INSERT INTO cho.ReceitaDetalhada(
    TransacaoID, 
    PedidoID, 
    ItemID,
    ValorPrato, 
    Quantidade,
    PedidoHora,
    FilialKEY,
    ClienteKEY,
    EnderecoKEY, 
    CalendarioKEY,  
    TipoPratoKEY,
    TipoSangKEY
)
SELECT 
    gen_random_uuid(),
    p.PedidoID,
    i.ItemID,
    i.ItemPrecoVenda,
    pi.Quantidade,
    TO_CHAR(p.PedidoData, 'HH24:MI:SS'),
    fd.FilialKEY,
    cd.ClienteKEY,
    ed.EnderecoKEY,
    cad.CalendarioKEY,
    tdp.TipoPratoKEY,
    tsd.TipoSangKEY
FROM 
    cho_db.Pedido p
JOIN 
    cho_db.PedidoItem pi ON p.PedidoID = pi.PedidoID
JOIN 
    cho_db.Item i ON pi.ItemID = i.ItemID
JOIN
    cho_db.Cliente c ON c.ClienteID = p.ClienteID
JOIN
    cho.ClienteDimension cd ON c.ClienteID = cd.ClienteID
JOIN
    cho.FilialDimension fd ON p.FilialID = fd.FilialID
JOIN
    cho.CalendarioDimension cad ON p.PedidoData = cad.DataCompleta
JOIN
    cho.TipoPratoDimension tdp ON i.ItemID = tdp.ItemID
JOIN
    cho.TipoSangClienteDimension tsd ON c.ClienteTipoSang = tsd.ClienteTipoSang
JOIN
    cho.EndereçoClienteDimension ed ON c.ClienteRua = ed.Logradouro 
    AND c.ClienteBairro = ed.Bairro
    AND c.ClienteMunicipio = ed.Municipio
    AND c.ClienteEstado = ed.Estado
LEFT JOIN
    cho.ReceitaDetalhada rd ON p.PedidoID = rd.PedidoID AND pi.ItemID = rd.ItemID
WHERE 
    rd.PedidoID IS NULL;

RETURN NEW;
else
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
RETURN NULL;
end if;

EXCEPTION
WHEN data_exception THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN unique_violation THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN others THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Trigger para que quando novos pedidos forem adicionados, atualizarmos a tabela de fatos detalhados.
CREATE TRIGGER receita_detalha_insert_trg
AFTER INSERT ON audit.ins_pedido
EXECUTE PROCEDURE audit.ins_Receita_Detalhada_func();

/*****
Atualiza as tabela de fatos com base nas inserções realizadas.

As tabelas pedido e pedido item devem ser atualizadas.
******/
CREATE OR REPLACE FUNCTION audit.ins_Receita_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
v_new_data := ROW(NEW.*);
INSERT INTO cho.Receita (
    TransacaoID, 
    PedidoID, 
    Valor, 
    EnderecoKEY,
    CalendarioKEY,
    FilialKEY,
    ClienteKEY,
    TipoSangKEY
)
SELECT 
    gen_random_uuid(),
    p.PedidoID,
    SUM(pi.Quantidade * i.ItemPrecoVenda),
    ed.EnderecoKEY,
    cad.CalendarioKEY,
    fd.FilialKEY,
    cd.ClienteKEY,
    tsd.TipoSangKEY
FROM 
    cho_db.Pedido p
JOIN 
    cho_db.PedidoItem pi ON p.PedidoID = pi.PedidoID
JOIN 
    cho_db.Item i ON pi.ItemID = i.ItemID
JOIN
    cho_db.Cliente c ON c.ClienteID = p.ClienteID
JOIN
    cho.ClienteDimension cd ON c.ClienteID = cd.ClienteID
JOIN
    cho.FilialDimension fd ON p.FilialID = fd.FilialID
JOIN
    cho.CalendarioDimension cad ON p.PedidoData = cad.DataCompleta
JOIN
    cho.TipoSangClienteDimension tsd ON c.ClienteTipoSang = tsd.ClienteTipoSang
JOIN
    cho.EndereçoClienteDimension ed ON c.ClienteRua = ed.Logradouro 
    AND c.ClienteBairro = ed.Bairro
    AND c.ClienteMunicipio = ed.Municipio
    AND c.ClienteEstado = ed.Estado
WHERE 
    p.PedidoID NOT IN (SELECT PedidoID FROM cho.Receita)
GROUP BY 
    p.PedidoID, cd.ClienteKEY, fd.FilialKEY, cad.CalendarioKEY, tsd.TipoSangKEY, ed.EnderecoKEY;

RETURN NEW;
else
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
RETURN NULL;
end if;

EXCEPTION
WHEN data_exception THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN unique_violation THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
WHEN others THEN
RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Trigger para que quando novos pedidos forem adicionados, atualizarmos a tabela de fatos detalhados.
CREATE TRIGGER Receita_insert_trg
AFTER INSERT ON audit.ins_pedido
EXECUTE PROCEDURE audit.ins_Receita_func();

-- Aqui, apagamos os dados das tabelas que guardam as alterações. Isso é necessário para que não haja repetição na atualização dos dados.
TRUNCATE TABLE audit.ins_filial;
TRUNCATE TABLE audit.ins_cliente;
TRUNCATE TABLE audit.ins_pedido;
TRUNCATE TABLE audit.ins_item;

-- Carregar a extensão pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Criar a função que atualiza o faturamento esperado
CREATE OR REPLACE FUNCTION atualiza_FaturamentoEsperado()
RETURNS VOID AS $$
BEGIN
    INSERT INTO cho.FaturamentoEsperado (Data, FaturamentoID, FaturamentoEsperado, FaturamentoReal, Porcentagem, FilialKEY)
    SELECT
        COALESCE(cartao.TransacaoData, nossa_base.PedidoData) AS Data,
        gen_random_uuid() AS FaturamentoID,
        cartao.ValorEsperado AS FaturamentoEsperado,
        nossa_base.TotalGasto AS FaturamentoReal,
        ROUND((nossa_base.TotalGasto::numeric / cartao.ValorEsperado::numeric)::numeric, 2) AS Porcentagem,
        COALESCE(cartao.FilialKEY, nossa_base.FilialKEY) AS FilialKEY
    FROM
        (-- CARTÃO DE CRÉDITO
            SELECT
                tcd.TransacaoData,
                fd.FilialKEY,
                ROUND(SUM(tcd.TransacaoValor) * msf.MarketShare, 2) AS ValorEsperado
            FROM
                cc_bd.TransacaoCartaoDeCredito tcd
            JOIN
                cho.FilialDimension fd ON tcd.TransacaoCidade = fd.FilialMunicipio
                AND tcd.TransacaoBairro = fd.FilialBairro
            JOIN
                cc_bd.MarketShareFilial msf ON fd.FilialID = msf.FilialID
            WHERE
                tcd.TransacaoSegmento = 'Alimentação'
            GROUP BY
                fd.FilialKEY, tcd.TransacaoData, msf.MarketShare) AS cartao
            JOIN
            (-- NOSSA BASE
            SELECT
                CAST(p.PedidoData AS DATE) AS PedidoData,
                fd.FilialKEY,
                SUM(pi.Quantidade * i.ItemPrecoVenda) AS TotalGasto
            FROM
                cho.FilialDimension fd
            JOIN
                cho_db.Pedido p ON p.FilialID = fd.FilialID
            JOIN
                cho_db.PedidoItem pi ON p.PedidoID = pi.PedidoID
            JOIN
                cho_db.Item i ON pi.ItemID = i.ItemID
            GROUP BY
                CAST(p.PedidoData AS DATE), fd.FilialKEY) AS nossa_base
            ON
            cartao.TransacaoData = nossa_base.PedidoData
            AND cartao.FilialKEY = nossa_base.FilialKEY
    WHERE
        (COALESCE(cartao.FilialKEY, nossa_base.FilialKEY), COALESCE(cartao.TransacaoData, nossa_base.PedidoData)) NOT IN (SELECT FilialKEY, Data FROM cho.FaturamentoEsperado);
END;
$$ LANGUAGE plpgsql;

-- Agendar a execução da função para rodar todos os dias à meia-noite
SELECT cron.schedule('0 0 * * *', 'SELECT atualiza_FaturamentoEsperado();');