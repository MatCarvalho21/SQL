set search_path=cho;

truncate table Receita;
truncate table ReceitaDetalhada;
delete from FilialDimension;
delete from ClienteDimension;
delete from EndereçoClienteDimension;
delete from TipoSangClienteDimension;
delete from TipoPratoDimension;
delete from CalendarioDimension;

set search_path=cho_db;

INSERT INTO cho.FilialDimension
select
    gen_random_uuid(),
    f.FilialID,
    f.FilialRua,
    f.FilialBairro,
    f.FilialMunicipio,
    f.FilialEstado
from 
    Filial f;

INSERT INTO cho.ClienteDimension
SELECT
    gen_random_uuid(),
    c.ClienteID,
    CONCAT(c.ClienteNome, ' ', c.ClienteSobrenome),
    c.ClienteCPF
FROM 
    Cliente c;

INSERT INTO cho.EndereçoClienteDimension
select
    gen_random_uuid(),
    c.ClienteRua,
    c.ClienteBairro,
    c.ClienteMunicipio,
    c.ClienteEstado
from 
    Cliente c
GROUP BY
    c.ClienteRua,
    c.ClienteBairro,
    c.ClienteMunicipio,
    c.ClienteEstado;


INSERT INTO cho.TipoSangClienteDimension
select
    gen_random_uuid(),
    c.ClienteTipoSang
from 
    Cliente c
GROUP BY
    c.ClienteTipoSang;

INSERT INTO cho.TipoPratoDimension
select
    gen_random_uuid(),
    i.ItemID,
    i.ItemCategoria
from 
    Item i;

INSERT INTO cho.CalendarioDimension
select
    gen_random_uuid(),
	a.datacompleta,
	a.diasemana,
	a.dia,
	a.mes,
	a.trimestre,
	a.ano
from (
select distinct
	cast(p.PedidoData as TIMESTAMP) as datacompleta,
	to_char(p.PedidoData, 'DY') as diasemana,
	extract(day from p.PedidoData) as dia,
	to_char(p.PedidoData, 'MM') as mes,
	cast(to_char(p.PedidoData, 'Q')as int) as trimestre,
	extract(year from p.PedidoData) as ano
from 
	Pedido p 
where cast(p.PedidoData as TIMESTAMP) not in (select DataCompleta from cho.CalendarioDimension)
	) as a;

INSERT INTO cho.Receita
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
    Pedido p
JOIN 
    PedidoItem pi ON p.PedidoID = pi.PedidoID
JOIN 
    Item i ON pi.ItemID = i.ItemID
JOIN
    Cliente c ON c.ClienteID = p.ClienteID
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
GROUP BY 
    p.PedidoID, cd.ClienteKEY, fd.FilialKEY, cad.CalendarioKEY, tsd.TipoSangKEY, ed.EnderecoKEY;

INSERT INTO cho.ReceitaDetalhada
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
    Pedido p
JOIN 
    PedidoItem pi ON p.PedidoID = pi.PedidoID
JOIN 
    Item i ON pi.ItemID = i.ItemID
JOIN
    Cliente c ON c.ClienteID = p.ClienteID
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
GROUP BY 
    p.PedidoID, cd.ClienteKEY, fd.FilialKEY, cad.CalendarioKEY, tdp.TipoPratoKEY, 
    tsd.TipoSangKEY, ed.EnderecoKEY, i.ItemPrecoVenda, pi.Quantidade, p.PedidoData, i.ItemID;

INSERT INTO cho.FaturamentoEsperado
SELECT
    COALESCE(cartao.TransacaoData, nossa_base.PedidoData),
    gen_random_uuid(),
    cartao.ValorEsperado,
    nossa_base.TotalGasto,
    ROUND((nossa_base.TotalGasto::numeric/ cartao.ValorEsperado::numeric)::numeric, 2),
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
        Pedido p ON p.FilialID = fd.FilialID
    JOIN
        PedidoItem pi ON p.PedidoID = pi.PedidoID
    JOIN
        Item i ON pi.ItemID = i.ItemID
    GROUP BY
        CAST(p.PedidoData AS DATE), fd.FilialKEY) AS nossa_base
ON
    cartao.TransacaoData = nossa_base.PedidoData
    AND cartao.FilialKEY = nossa_base.FilialKEY;

