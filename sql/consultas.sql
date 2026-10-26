-- ============================================================
-- Projeto: Pipeline IoT
-- Banco: fernanda_pipeline_iot
-- Consultas para análise das séries temporais
-- ============================================================


-- CONSULTA 1
-- Média diária por nó e métrica.
-- Objetivo: comparar os valores médios e a quantidade de
-- observações registradas em cada dia.

SELECT
    no,
    inicio_periodo AS dia,
    metrica,
    valor_medio,
    quantidade
FROM fernanda_agregados
WHERE periodo = 'dia'
  AND no = '!8fa724e8'
ORDER BY inicio_periodo DESC, metrica;


-- CONSULTA 2
-- Mínimo, máximo e média por hora.
-- Objetivo: observar a variação dos valores ao longo de um dia.
-- A data abaixo corresponde ao dia usado nos testes anteriores.

SELECT
    no,
    inicio_periodo AS hora,
    metrica,
    valor_minimo,
    valor_maximo,
    valor_medio,
    quantidade
FROM fernanda_agregados
WHERE periodo = 'hora'
  AND no = '!8fa724e8'
  AND inicio_periodo >= '2026-10-24 00:00:00'
  AND inicio_periodo <  '2026-10-25 00:00:00'
ORDER BY inicio_periodo, metrica;


-- CONSULTA 3
-- Tempo desde o último recebimento registrado na série limpa.
-- Objetivo: identificar nós sem registros recentes.
-- Atenção: silêncio não comprova que o dispositivo esteja desligado.

SELECT
    no,
    MAX(recebido_em) AS ultimo_recebimento,
    TIMESTAMPDIFF(
        MINUTE,
        MAX(recebido_em),
        NOW()
    ) AS minutos_sem_registro
FROM fernanda_series
GROUP BY no
ORDER BY minutos_sem_registro DESC;
