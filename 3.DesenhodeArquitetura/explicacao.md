# Arquitetura - MySQL (CDC)

A solução da arquitetura utiliza serviços gerenciados da AWS para realizar a captura de alterações (CDC) de um banco MySQL,
armazenando os dados em um Data Lake baseado na arquitetura medalhão (Bronze, Silver e Gold).

## Fluxo:

```
MySQL → AWS DMS → S3 Bronze → Glue Catalog → Glue ETL → Silver → Glue ETL → Gold → Athena → QuickSight
```

O controle de acesso aos dados é realizado através do AWS Lake Formation.

## Camada de Ingestão

### AWS Database Migration Service (DMS)

O AWS DMS é utilizado para realizar CDC (Change Data Capture), capturando inserções, atualizações e exclusões ocorridas no banco MySQL.

As alterações são enviadas continuamente para a camada Bronze do Data Lake sem impacto significativo na aplicação transacional.

## Camada Bronze

### Amazon S3 (Bronze)

A camada Bronze armazena os dados brutos recebidos do DMS.

Características:

- Dados armazenados sem transformações de negócio.
- Preservação completa do histórico recebido do sistema de origem.
- Possibilidade de reprocessamento em caso de falhas nas camadas superiores.
- Particionamento por data de processamento.

### AWS Glue Crawler

O Glue Crawler realiza a identificação dos schemas presentes na camada Bronze e registra as tabelas no Glue Data Catalog.

### AWS Glue Data Catalog

Responsável pelo catálogo centralizado de metadados das tabelas do Data Lake.

Permite que serviços como Athena e Glue consultem os dados de forma estruturada.

## Camada Silver

### AWS Glue ETL

Jobs Spark executados no AWS Glue realizam a transformação dos dados da Bronze para Silver.

Principais funções:

- Deduplicação de registros
- Padronização de formatos
- Tratamento de dados inválidos
- Enriquecimento dos dados

A camada Silver contém dados limpos e confiáveis para consumo analítico.

## Camada Gold

### AWS Glue ETL

Jobs Spark que processam os dados da Silver para geração de datasets analíticos.

A camada Gold é otimizada para consumo por analistas, cientistas de dados e ferramentas de BI.

## Consumo dos Dados

### Amazon Athena

Permite consultar diretamente sobre os dados armazenados no S3 sem necessidade de provisionamento de infraestrutura.

### Amazon QuickSight

Ferramenta de BI para construção de dashboards.

Os dados consumidos pelo QuickSight são obtidos a partir das tabelas da camada Gold.

## Governança e Segurança

### AWS Lake Formation

O AWS Lake Formation é responsável pela governança centralizada do Data Lake.

Vai permitir que possamos controlar o acesso a nível de:

- Grupo
- Usuário
- Banco de dados
- Tabela
- Coluna

Exemplo:

- Grupo Marketing: acesso às tabelas de campanhas e clientes.
- Grupo Financeiro: acesso às tabelas financeiras.
- Grupo Comercial: acesso às tabelas de vendas.

Dessa forma, cada área possui acesso apenas aos dados necessários para suas atividades, garantindo segurança, rastreabilidade e conformidade com políticas corporativas.