Drop schema if exists cho cascade;
create schema cho;

set search_path=cho;

CREATE TABLE FilialDimension
(
  FilialKEY VARCHAR NOT NULL,
  FilialID INT NOT NULL,
  FilialRua VARCHAR NOT NULL,
  FilialBairro VARCHAR NOT NULL,
  FilialMunicipio VARCHAR NOT NULL,
  FilialEstado CHAR(2) NOT NULL,
  PRIMARY KEY (FilialKEY)
);

CREATE TABLE ClienteDimension
(
  ClienteKEY VARCHAR NOT NULL,
  ClienteID INT NOT NULL,
  ClienteNomeCompleto VARCHAR NOT NULL,
  ClienteCPF CHAR(11) NOT NULL,
  PRIMARY KEY (ClienteKEY),
  UNIQUE (ClienteCPF)
);

CREATE TABLE EndereçoClienteDimension
(
  EnderecoKEY VARCHAR NOT NULL,
  Logradouro VARCHAR NOT NULL,
  Bairro VARCHAR NOT NULL,
  Municipio VARCHAR NOT NULL,
  Estado CHAR(2) NOT NULL,
  PRIMARY KEY (EnderecoKEY)
);

CREATE TABLE TipoSangClienteDimension
(
  TipoSangKEY VARCHAR NOT NULL,
  ClienteTipoSang VARCHAR NOT NULL,
  PRIMARY KEY (TipoSangKEY)
);

CREATE TABLE TipoPratoDimension
(
  TipoPratoKEY VARCHAR NOT NULL,
  ItemID INT NOT NULL,
  ItemCategoria VARCHAR NOT NULL,
  PRIMARY KEY (TipoPratoKEY)
);

CREATE TABLE CalendarioDimension
(
  CalendarioKEY VARCHAR NOT NULL,
  DataCompleta TIMESTAMP NOT NULL,
  DiaDaSemana VARCHAR NOT NULL,
  Dia VARCHAR(2) NOT NULL,
  Mes VARCHAR(2) NOT NULL,
  Trimestre CHAR(1) NOT NULL,
  Ano CHAR(4) NOT NULL,
  PRIMARY KEY (CalendarioKEY)
);

CREATE TABLE Receita
(
  TransacaoID VARCHAR NOT NULL,
  PedidoID INT NOT NULL,
  Valor FLOAT NOT NULL,
  EnderecoKEY VARCHAR NOT NULL,
  CalendarioKEY VARCHAR NOT NULL,
  FilialKEY VARCHAR NOT NULL,
  ClienteKEY VARCHAR NOT NULL,
  TipoSangKEY VARCHAR NOT NULL,
  PRIMARY KEY (TransacaoID),
  FOREIGN KEY (EnderecoKEY) REFERENCES EndereçoClienteDimension(EnderecoKEY),
  FOREIGN KEY (CalendarioKEY) REFERENCES CalendarioDimension(CalendarioKEY),
  FOREIGN KEY (FilialKEY) REFERENCES FilialDimension(FilialKEY),
  FOREIGN KEY (ClienteKEY) REFERENCES ClienteDimension(ClienteKEY),
  FOREIGN KEY (TipoSangKEY) REFERENCES TipoSangClienteDimension(TipoSangKEY)
);

CREATE TABLE ReceitaDetalhada
(
  TransacaoID VARCHAR NOT NULL,
  PedidoID INT NOT NULL,
  ItemID INT NOT NULL,
  ValorPrato FLOAT NOT NULL,
  Quantidade INT NOT NULL,
  PedidoHora VARCHAR NOT NULL,
  FilialKEY VARCHAR NOT NULL,
  ClienteKEY VARCHAR NOT NULL,
  EnderecoKEY VARCHAR NOT NULL,
  CalendarioKEY VARCHAR NOT NULL,
  TipoPratoKEY VARCHAR NOT NULL,
  TipoSangKEY VARCHAR NOT NULL,
  PRIMARY KEY (TransacaoID),
  FOREIGN KEY (FilialKEY) REFERENCES FilialDimension(FilialKEY),
  FOREIGN KEY (ClienteKEY) REFERENCES ClienteDimension(ClienteKEY),
  FOREIGN KEY (EnderecoKEY) REFERENCES EndereçoClienteDimension(EnderecoKEY),
  FOREIGN KEY (CalendarioKEY) REFERENCES CalendarioDimension(CalendarioKEY),
  FOREIGN KEY (TipoPratoKEY) REFERENCES TipoPratoDimension(TipoPratoKEY),
  FOREIGN KEY (TipoSangKEY) REFERENCES TipoSangClienteDimension(TipoSangKEY)
);

CREATE TABLE FaturamentoEsperado
(
  Data DATETIME NOT NULL,
  FaturamentoID VARCHAR NOT NULL,
  FaturamentoEsperado DECIMAL(10,2) NOT NULL,
  FaturamentoReal DECIMAL(10,2) NOT NULL,
  Diferenca DECIMAL(10,2) NOT NULL,
  FilialKEY VARCHAR NOT NULL,
  PRIMARY KEY (FaturamentoID),
  FOREIGN KEY (FilialKEY) REFERENCES Filial_Dimension(FilialKEY)
);
