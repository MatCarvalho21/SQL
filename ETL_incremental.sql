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
CREATE TRIGGER audit_trg_filial
AFTER INSERT OR UPDATE OR DELETE ON cho_db.filial
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Cria a tabela audit.ins_Filial como uma cópia vazia de cho_db.filial
Create table audit.ins_Filial as select * from cho_db.filial where 1=0; 

/***
Trigger para salvar inserções
***/
-- Função para capturar inserções na tabela Filial e salvá-las na tabela audit.ins_Filial
CREATE OR REPLACE FUNCTION audit.ins_Filial_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_Filial VALUES (NEW.filialid, NEW.filialrua, NEW.filialbairro, NEW.filialmunicipio, NEW.filialestado);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_Filial_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_Filial_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_Filial_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_Filial_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Filial e chamar a função audit.ins_Filial_func
DROP TRIGGER IF EXISTS Filial_insert_trg ON cho_db.filial;
CREATE TRIGGER Filial_insert_trg
AFTER INSERT ON cho_db.filial
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Filial_func();


/***
CLIENTE
***/
CREATE TRIGGER audit_trg_cliente
AFTER INSERT OR UPDATE OR DELETE ON cho_db.cliente
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Cria a tabela audit.ins_Cliente como uma cópia vazia de cho_db.cliente
Create table audit.ins_Cliente as select * from cho_db.cliente where 1=0; 

/***
Trigger para salvar inserções
***/
-- Função para capturar inserções na tabela Cliente e salvá-las na tabela audit.ins_Cliente
CREATE OR REPLACE FUNCTION audit.ins_Cliente_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_Cliente VALUES (NEW.clienteid, NEW.clientenome, NEW.clientesobrenome, NEW.clientetiposang, 
                                              NEW.clienterua, NEW.clientebairro, NEW.clientemunicipio, NEW.clienteestado, 
                                              NEW.clientecpf, NEW.clientedatanasc);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_Cliente_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_Cliente_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_Cliente_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_Cliente_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Cliente e chamar a função audit.ins_Cliente_func
DROP TRIGGER IF EXISTS Cliente_insert_trg ON cho_db.cliente;
CREATE TRIGGER Cliente_insert_trg
AFTER INSERT ON cho_db.cliente
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Cliente_func();


/***
PEDIDO
***/
CREATE TRIGGER audit_trg_pedido
AFTER INSERT OR UPDATE OR DELETE ON cho_db.pedido
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

-- Cria a tabela audit.ins_Pedido como uma cópia vazia de cho_db.pedido
Create table audit.ins_Pedido as select * from cho_db.pedido where 1=0; 

/***
Trigger para salvar inserções
***/
-- Função para capturar inserções na tabela Pedido e salvá-las na tabela audit.ins_Pedido
CREATE OR REPLACE FUNCTION audit.ins_Pedido_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_Pedido VALUES (NEW.pedidoid, NEW.pedidodata, NEW.clienteid, NEW.filialid);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_Pedido_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_Pedido_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_Pedido_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_Pedido_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Pedido e chamar a função audit.ins_Pedido_func
DROP TRIGGER IF EXISTS Pedido_insert_trg ON cho_db.pedido;
CREATE TRIGGER Pedido_insert_trg
AFTER INSERT ON cho_db.pedido
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Pedido_func();


/***
PEDIDO ITEM
***/
-- Para salvar as modificações na tabela PedidoItem
CREATE TRIGGER audit_trg_pedidoitem
AFTER INSERT OR UPDATE OR DELETE ON cho_db.pedidoitem
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

/***
Trigger para salvar inserções
***/
-- Cria a tabela audit.ins_PedidoItem como uma cópia vazia de cho_db.pedidoitem
Create table audit.ins_PedidoItem as select * from cho_db.pedidoitem where 1=0; 

-- Função para capturar inserções na tabela PedidoItem e salvá-las na tabela audit.ins_PedidoItem
CREATE OR REPLACE FUNCTION audit.ins_PedidoItem_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_PedidoItem VALUES (NEW.quantidade, NEW.pedidoid, NEW.itemid);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_PedidoItem_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_PedidoItem_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_PedidoItem_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_PedidoItem_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela PedidoItem e chamar a função audit.ins_PedidoItem_func
DROP TRIGGER IF EXISTS PedidoItem_insert_trg ON cho_db.pedidoitem;
CREATE TRIGGER PedidoItem_insert_trg
AFTER INSERT ON cho_db.pedidoitem
FOR EACH ROW EXECUTE PROCEDURE audit.ins_PedidoItem_func();



/***
ITEM
***/
-- Para salvar as modificações na tabela Item
CREATE TRIGGER audit_trg_item
AFTER INSERT OR UPDATE OR DELETE ON cho_db.item
FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();

/***
Trigger para salvar inserções
***/
-- Cria a tabela audit.ins_Item como uma cópia vazia de cho_db.item
Create table audit.ins_Item as select * from cho_db.item where 1=0; 

-- Função para capturar inserções na tabela Item e salvá-las na tabela audit.ins_Item
CREATE OR REPLACE FUNCTION audit.ins_Item_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := ROW(NEW.*);
        INSERT INTO audit.ins_Item VALUES (NEW.itemid, NEW.itemnome, NEW.itemcategoria, NEW.itemprecovenda);
        RETURN NEW;
    ELSE
        RAISE WARNING '[audit.ins_Item_func] - Other action occurred: %, at %', TG_OP, now();
        RETURN NULL;
    END IF;
EXCEPTION
    WHEN data_exception THEN
        RAISE WARNING '[audit.ins_Item_func] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN unique_violation THEN
        RAISE WARNING '[audit.ins_Item_func] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING '[audit.ins_Item_func] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, audit;

-- Cria um trigger para capturar inserções na tabela Item e chamar a função audit.ins_Item_func
DROP TRIGGER IF EXISTS Item_insert_trg ON cho_db.item;
CREATE TRIGGER Item_insert_trg
AFTER INSERT ON cho_db.item
FOR EACH ROW EXECUTE PROCEDURE audit.ins_Item_func();



-- 
-- PASSO 4: Atualizar as dimensões no Datawarehouse
--

/*****
Atualiza a dimensão do calendário na tabela cho.CalendarioDimension com base nos dados da tabela audit.ins_Pedido. 
A função audit.ins_dim_Calendario_func captura inserções na tabela audit.ins_Pedido e, para cada nova inserção,
insere uma nova linha na tabela cho.CalendarioDimension se a data correspondente não estiver presente. 
Os campos são gerados a partir da data do pedido, incluindo chave do calendário, data completa, dia da semana, dia, mês, trimestre e ano.
Depois de inserir os dados, a tabela audit.ins_Filial é truncada para preparação de novas inserções.
******/
CREATE OR REPLACE FUNCTION audit.ins_dim_Calendario_func() RETURNS trigger AS $body$
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
    audit.ins_Pedido p 
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
CREATE TRIGGER dim_Calendario_insert_trg
AFTER INSERT ON audit.ins_Pedido
EXECUTE PROCEDURE audit.ins_dim_Calendario_func();


/*****
Atualiza a dimensão de filial na tabela cho.FilialDimension com base nas inserções na tabela audit.ins_Filial.
A função audit.ins_dim_Filial_func captura inserções na tabela audit.ins_Filial e insere os dados correspondentes na tabela cho.FilialDimension.
Depois de inserir os dados, a tabela audit.ins_Filial é truncada para preparação de novas inserções.
******/
CREATE OR REPLACE FUNCTION audit.ins_dim_Filial_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
BEGIN
if (TG_OP = 'INSERT') then
v_new_data := ROW(NEW.*);
INSERT INTO cho.FilialDimension (FilialKEY, FilialID, FilialRua, FilialBairro, FilialMunicipio, FilialEstado) SELECT gen_random_uuid(), filialid, filialrua, filialbairro, filialmunicipio, filialestado FROM audit.ins_Filial;

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
CREATE TRIGGER dim_Filial_insert_trg
AFTER INSERT ON audit.ins_Filial
EXECUTE PROCEDURE audit.ins_dim_Filial_func();


/*****
Atualiza a dimensão de itens na tabela cho.TipoPratoDimension com base nas inserções na tabela audit.ins_Item.
A função audit.ins_dim_Item_func captura inserções na tabela audit.ins_Item e insere os dados correspondentes na tabela cho.TipoPratoDimension.
Depois de inserir os dados, a tabela audit.ins_Item é truncada para preparação de novas inserções.
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
FROM audit.ins_Item;

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
CREATE TRIGGER dim_Item_insert_trg
AFTER INSERT ON audit.ins_Item
EXECUTE PROCEDURE audit.ins_dim_Item_func();


/*****
Atualiza as dimensões relacionadas ao cliente nas tabelas cho.ClienteDimension, cho.EndereçoClienteDimension e cho.TipoSangClienteDimension com base nas inserções na tabela audit.ins_Cliente.
A função audit.ins_dim_Cliente_func captura inserções na tabela audit.ins_Cliente e insere os dados correspondentes nas tabelas de dimensão de cliente, endereço e tipo sanguíneo.
Depois de inserir os dados, a tabela audit.ins_Cliente é truncada para preparação de novas inserções.
******/
CREATE OR REPLACE FUNCTION audit.ins_dim_Cliente_func() RETURNS trigger AS $body$
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
  audit.ins_Cliente;

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
  audit.ins_Cliente;

INSERT INTO cho.TipoSangClienteDimension (
    TipoSangKEY,
    ClienteTipoSang
)
SELECT
    gen_random_uuid(),
    clientetiposang
FROM
    audit.ins_Cliente;

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

-- quando um novo cliente for adicionado atualiza a dimensão cliente 
CREATE TRIGGER dim_Cliente_insert_trg
AFTER INSERT ON audit.ins_Cliente
EXECUTE PROCEDURE audit.ins_dim_Cliente_func();


/*****

Atualizar fatos Receita Detalhada

repetindo a mesma instrução da carga inicial para audit.ins_Compra

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

-- quando uma nova compra ocorrer atualiza fatos de Venda detalhados
CREATE TRIGGER receita_detalha_insert_trg
AFTER INSERT ON audit.ins_Pedido
EXECUTE PROCEDURE audit.ins_Receita_Detalhada_func();

/*****

Atualizar fatos Receita

repetindo a mesma instrução da carga inicial para audit.ins_Compra

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

-- quando uma nova compra ocorrer atualiza fatos Agregados de Venda
CREATE TRIGGER Receita_insert_trg
AFTER INSERT ON audit.ins_Pedido
EXECUTE PROCEDURE audit.ins_Receita_func();

TRUNCATE TABLE audit.ins_Filial;
TRUNCATE TABLE audit.ins_Cliente;
TRUNCATE TABLE audit.ins_Pedido;
TRUNCATE TABLE audit.ins_Item;