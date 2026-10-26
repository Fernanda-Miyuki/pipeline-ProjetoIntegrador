-- ============================================================
-- Projeto: Pipeline IoT
-- Banco: fernanda_pipeline_iot
-- Esquema das tabelas utilizadas pelo pipeline
--
-- A coleção MongoDB iot.sensores é a origem compartilhada
-- e deve permanecer somente para leitura.
-- ============================================================

CREATE TABLE IF NOT EXISTS fernanda_series (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    no VARCHAR(100) NOT NULL,
    instante DATETIME NOT NULL,
    metrica VARCHAR(100) NOT NULL,
    valor DOUBLE NOT NULL,
    pacote_id VARCHAR(100) NOT NULL,
    recebido_em DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_fernanda_series_pacote (
        no, pacote_id, metrica
    ),
    KEY idx_fernanda_series_consulta (
        no, metrica, instante
    )
);

CREATE TABLE IF NOT EXISTS fernanda_agregados (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    no VARCHAR(100) NOT NULL,
    metrica VARCHAR(100) NOT NULL,
    periodo ENUM('hora', 'dia') NOT NULL,
    inicio_periodo DATETIME NOT NULL,
    valor_minimo DOUBLE NOT NULL,
    valor_maximo DOUBLE NOT NULL,
    valor_medio DOUBLE NOT NULL,
    quantidade INT NOT NULL,
    atualizado_em DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_fernanda_agregado (
        no, metrica, periodo, inicio_periodo
    )
);

CREATE TABLE IF NOT EXISTS fernanda_quarentena (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    pacote_id VARCHAR(100) NOT NULL,
    motivo VARCHAR(100) NOT NULL,
    detalhes TEXT,
    recebido_em DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_fernanda_quarentena (
        pacote_id, motivo
    )
);

CREATE TABLE IF NOT EXISTS fernanda_controle (
    chave VARCHAR(100) NOT NULL,
    valor TEXT,
    atualizado_em DATETIME NOT NULL,
    PRIMARY KEY (chave)
);

CREATE TABLE IF NOT EXISTS fernanda_processados (
    mongo_id CHAR(24) NOT NULL,
    resultado VARCHAR(50) NOT NULL,
    processado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mongo_id)
);
