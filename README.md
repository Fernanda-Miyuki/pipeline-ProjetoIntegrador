# Pipeline — Do Pacote Bruto ao Dado Útil

Pipeline de dados para leitura, validação, deduplicação e transformação de mensagens, com armazenamento de séries temporais e agregações no MariaDB, consulta rápida do último valor no Redis e registro de mensagens rejeitadas em quarentena.

**Projeto acadêmico:** Big Data no Agronegócio — Fatec Pompeia Shunji Nishimura
**Professor:** https://github.com/siriani  
**Aluna:** Fernanda Miyuki

## 1. Objetivo

O objetivo do projeto é transformar mensagens IoT brutas em informações estruturadas e consultáveis. A solução lê os documentos da coleção MongoDB `iot.sensores`, utilizada neste projeto como fonte equivalente à coleção `pacotes_brutos` da atividade, processa os pacotes e distribui os resultados entre MariaDB e Redis.

A coleção de origem é compartilhada e permanece somente para leitura. Todos os objetos criados para o projeto utilizam o prefixo `fernanda_` nas tabelas relacionais e o prefixo `fernanda:` nas chaves Redis.

## 2. Arquitetura

### Tecnologias

- **Python:** lógica de leitura, validação e transformação dos pacotes.
- **MongoDB / PyMongo:** leitura da fonte bruta.
- **MariaDB / MySQL Connector:** persistência das séries, agregados, quarentena e controles de processamento.
- **Redis:** armazenamento do último valor disponível por nó e métrica.
- **Docker:** empacotamento e execução do pipeline.
- **python-dotenv:** carregamento das configurações do arquivo `.env`.

## 3. Fluxo de processamento

1. A aplicação consulta periodicamente a coleção `iot.sensores`.
2. Os documentos ainda não registrados na tabela de controle são selecionados para processamento.
3. O conteúdo da mensagem é interpretado para identificar o nó de origem, o identificador do pacote, o instante e as métricas disponíveis.
4. Os pacotes são verificados para evitar duplicações e identificar timestamps inválidos ou mensagens sem métricas reconhecidas.
5. Os dados válidos são gravados na série temporal e utilizados para atualizar os agregados por hora e dia.
6. O Redis é atualizado com o valor mais recente de cada nó e métrica.
7. Os pacotes rejeitados são registrados na quarentena quando aplicável, com o motivo correspondente.
8. A tabela de controle registra o estado de processamento para reduzir o risco de reprocessamento após uma reinicialização.

O pipeline utiliza consultas periódicas porque a instância MongoDB de origem não disponibiliza change streams por não operar como replica set.

## 4. Modelagem dos dados

### 4.1. Série temporal — `fernanda_series`

Armazena os registros válidos em formato longo, com uma linha para cada combinação de nó, pacote e métrica.

| Campo | Finalidade |
|---|---|
| `id` | Identificador interno do registro |
| `no` | Identificação do nó de origem |
| `instante` | Instante atribuído à medição |
| `metrica` | Nome ou código da métrica |
| `valor` | Valor numérico da métrica |
| `pacote_id` | Identificador do pacote de origem |
| `recebido_em` | Instante de recebimento registrado |

**Justificativa:** o formato longo permite armazenar métricas diferentes sem criar uma coluna nova para cada variável. Isso facilita a inclusão de novas métricas e a realização de filtros por nó, período e tipo de medição.

A restrição de unicidade por nó, pacote e métrica também ajuda a impedir que a mesma medição seja gravada repetidamente.

### 4.2. Agregados — `fernanda_agregados`

Armazena estatísticas agrupadas por nó, métrica, período e início da janela temporal.

| Campo | Finalidade |
|---|---|
| `no` | Nó monitorado |
| `metrica` | Métrica agregada |
| `periodo` | Janela de agregação: hora ou dia |
| `inicio_periodo` | Início da janela |
| `valor_minimo` | Menor valor da janela |
| `valor_maximo` | Maior valor da janela |
| `valor_medio` | Média da janela |
| `quantidade` | Quantidade de registros considerados |
| `atualizado_em` | Última atualização do agregado |

**Justificativa:** as agregações por hora e dia permitem analisar tendências sem precisar recalcular todas as estatísticas a partir da série original em cada consulta. A janela horária oferece uma visão operacional e a diária facilita comparações ao longo do tempo. Essas janelas foram escolhidas como compromisso entre detalhe temporal e simplicidade de consulta; janelas menores, como 15 minutos, poderiam ser consideradas em uma evolução do projeto.

### 4.3. Último valor — Redis

O Redis mantém o valor mais recente de cada combinação de nó e métrica, utilizando o prefixo obrigatório `fernanda:` nas chaves.

**Justificativa:** consultas recorrentes sobre o estado mais recente podem ser atendidas diretamente pelo Redis, sem executar repetidamente uma busca e ordenação na série temporal do MariaDB. O pipeline deve evitar substituir um valor mais recente por outro com timestamp anterior.

### 4.4. Quarentena — `fernanda_quarentena`

Registra pacotes rejeitados, com identificador, motivo, detalhes e instante de recebimento.

**Justificativa:** a quarentena mantém rastreabilidade sobre os dados que não puderam ser aceitos. Isso permite investigar problemas sem alterar nem excluir os documentos originais.

### 4.5. Controle e idempotência — `fernanda_processados` e `fernanda_controle`

A tabela `fernanda_processados` registra o identificador MongoDB de cada documento tratado e seu resultado. A tabela `fernanda_controle` mantém contadores e informações auxiliares do pipeline.

**Justificativa:** o controle de processamento permite identificar documentos já tratados e recuperar operações pendentes, reduzindo duplicações e facilitando a retomada após interrupções. A consistência entre MariaDB e Redis é tratada com um estado pendente para tentar novamente uma atualização Redis que falhou após a gravação SQL.

## 5. Problemas tratados

### Duplicatas

O pipeline utiliza o nó de origem e o identificador do pacote para identificar mensagens repetidas. O controle de documentos processados e as restrições de unicidade no MariaDB ajudam a evitar duplicação na persistência.

### Timestamps inválidos

Timestamps ausentes, inválidos ou incompatíveis com o instante de recebimento são identificados. A implementação atual utiliza um limite de diferença de 300 segundos; pacotes fora do limite são rejeitados ou encaminhados à quarentena conforme a regra aplicada.

### Buracos e atrasos

A consulta de silêncio mede o tempo transcorrido desde o último recebimento armazenado por nó. Ela ajuda a investigar interrupções e atrasos, mas não prova isoladamente que um dispositivo parou de transmitir: também pode haver interrupção no pipeline ou diferença de relógio.

O pipeline não deve inventar medições para preencher períodos sem dados.

### Mudanças de esquema

A implementação reconhece as métricas `t` e `u` observadas nos documentos da fonte. Mensagens sem métricas reconhecidas são contabilizadas separadamente. Isso evita tratar automaticamente todos os tipos de mensagem como se tivessem a mesma estrutura.

O suporte a novos tipos de pacote e métricas exige ampliar a decodificação e a validação. A semântica e as unidades de cada campo precisam ser confirmadas na documentação da fonte antes de atribuir nomes como temperatura ou umidade.

## 6. Contadores de processamento

Os contadores são mantidos na tabela `fernanda_controle`. O último snapshot registrado durante os testes apresentou os seguintes valores:

| Contador | Valor registrado |
|---|---:|
| Duplicatas descartadas | 1.290 |
| Mensagens sem métricas reconhecidas | 119 |
| Pacotes em quarentena | 10 |
| Pacotes válidos | 650 |
| Timestamps inválidos | 10 |

Esses números representam um snapshot anterior da execução, não uma garantia de que sejam os valores finais no momento da entrega. Devem ser consultados novamente antes da publicação.

Os timestamps inválidos são parte do tratamento dos pacotes em quarentena e não devem ser somados novamente como uma categoria independente. Os contadores também podem refletir momentos diferentes de atualização.

## 7. Consultas SQL

As consultas estão disponíveis no arquivo `sql/consultas.sql`. Elas utilizam os dados processados no MariaDB.

### Consulta 1 — Estatísticas diárias

Recupera mínimo, máximo e média por métrica e dia, além da quantidade de registros considerados.

```sql
SELECT
    no,
    inicio_periodo AS dia,
    metrica,
    valor_minimo,
    valor_maximo,
    valor_medio,
    quantidade
FROM fernanda_agregados
WHERE no = '!8fa724e8'
  AND periodo = 'dia'
ORDER BY inicio_periodo DESC, metrica;
```

**Exemplo de resultado registrado em uma consulta anterior:**

| Dia | Métrica | Média | Quantidade |
|---|---|---:|---:|
| 24/10/2026 | `t` | 20,40 | 208 |
| 24/10/2026 | `u` | 15,68 | 208 |
| 23/10/2026 | `t` | 20,59 | 253 |
| 23/10/2026 | `u` | 15,55 | 253 |
| 22/10/2026 | `t` | 22,27 | 427 |
| 22/10/2026 | `u` | 14,95 | 427 |

Os campos `t` e `u` são apresentados como identificadores observados na fonte, sem atribuição definitiva de unidade ou significado físico.

### Consulta 2 — Estatísticas horárias

Apresenta mínimo, máximo, média e quantidade por hora para um nó e uma data.

```sql
SELECT
    no,
    inicio_periodo AS hora,
    metrica,
    valor_minimo,
    valor_maximo,
    valor_medio,
    quantidade
FROM fernanda_agregados
WHERE no = '!8fa724e8'
  AND periodo = 'hora'
  AND inicio_periodo >= '2026-10-24 00:00:00'
  AND inicio_periodo <  '2026-10-25 00:00:00'
ORDER BY inicio_periodo, metrica;
```

**Exemplo de resultado registrado em uma consulta anterior:**

| Início da janela | Métrica | Mínimo | Máximo | Média | Quantidade |
|---|---|---:|---:|---:|---:|
| 24/10/2026 15:00 | `t` | 22 | 23 | 22,17 | 12 |
| 24/10/2026 15:00 | `u` | 14 | 15 | 14,83 | 12 |
| 24/10/2026 16:00 | `t` | 23 | 23 | 23,00 | 12 |
| 24/10/2026 16:00 | `u` | 14 | 14 | 14,00 | 12 |
| 24/10/2026 17:00 | `t` | 23 | 23 | 23,00 | 4 |
| 24/10/2026 17:00 | `u` | 14 | 14 | 14,00 | 4 |

As quantidades e os valores são exemplos de uma consulta anterior e devem ser confirmados antes da publicação.

### Consulta 3 — Tempo desde o último recebimento

Identifica o último recebimento armazenado para cada nó e calcula o tempo transcorrido em minutos.

```sql
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
ORDER BY ultimo_recebimento ASC;
```

**Exemplo de resultado registrado anteriormente:** o nó `!8fa724e8` apresentou último recebimento em `2026-10-24 17:21:42`, com aproximadamente 2.721 minutos sem novo registro no momento daquela consulta. Esse número muda com o tempo e precisa ser recalculado antes da publicação.

## 8. Instalação e execução

### Pré-requisitos

- Python 3.10 ou compatível.
- MongoDB acessível.
- MariaDB com o banco e as tabelas criados.
- Redis acessível.
- Docker instalado, caso se opte pela execução em contêiner.

### Configuração local

Clone o repositório e entre na pasta:

```bash
git clone <URL_DO_REPOSITORIO>
cd pipeline-iot
```

Crie e ative um ambiente virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

Crie o arquivo `.env` com base no exemplo:

```bash
cp .env.example .env
nano .env
```

Preencha as variáveis com os endereços e as credenciais dos serviços disponíveis no ambiente. Não publique o arquivo `.env`.

### Executar uma vez para teste

```bash
python src/pipeline.py --once
```

### Executar continuamente no ambiente local

```bash
python src/pipeline.py
```

### Executar com Docker

Construa a imagem:

```bash
docker build -t fernanda-pipeline-iot .
```

No ambiente Ubuntu em que o MariaDB, o MongoDB e o Redis estão disponíveis localmente, a execução testada utiliza a rede do hospedeiro:

```bash
sudo docker run --rm \
  --network host \
  --env-file .env \
  fernanda-pipeline-iot
```

Para acompanhar os logs, mantenha o terminal aberto. A configuração dos endereços no `.env` deve corresponder ao modo de rede utilizado.

## 9. Estrutura do repositório

```text
pipeline-iot/
├── src/
│   └── pipeline.py
├── sql/
│   ├── consultas.sql
│   └── schema.sql
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```
