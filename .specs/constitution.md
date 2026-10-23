# Constitution — Estudo PySpark + Delta

Versão: 0.1\
Data: 23/10/2026\
Escopo: pasta `C:\webservice\estudo_pyspark`, fora de todos os repositórios de produção.

## Para que existe

Duas coisas ao mesmo tempo, e a segunda decide o desenho:

1. **Estudo** de PySpark e Delta Lake sobre dado real da operação (Denver), com
   os mesmos problemas que o pipeline de produção já enfrenta.
2. **Teste de uso**: medir se Spark + Delta servem à Meggal em algum ponto do
   pipeline atual (Python + MySQL + dbt). Um teste de uso só vale se o resultado
   **bater com o de produção** e se o ganho (ou a perda) for **medido**, não
   suposto.

## Regras que não mudam sem decisão do dono

- **Fonte só leitura.** O único acesso ao DW é `SELECT` em sessão
  `READ ONLY`, pelo script de exportação, que roda na máquina e usa a conexão já
  configurada do `webservices_flask`. Nada aqui escreve em MySQL, ERP ou DW.
- **Dado real não sai da máquina.** Nada de dado de cliente em GitHub,
  Databricks Free Edition ou qualquer nuvem. Para a nuvem, só amostra sintética
  ou anonimizada, gerada de propósito e revisada.
- **Nenhum segredo no código ou nesta pasta.** Credenciais vêm da configuração
  do `webservices_flask`, que já existe; esta pasta não tem `.env` com senha.
- **Reconciliar sempre.** Toda camada fecha a conta com a anterior
  (`entrada = saída + quarentena + duplicatas`), e o resultado final é comparado
  com o número que a produção calcula pelo SQL, linha a linha quando possível.
- **Quarentena, não descarte.** Linha que não passa na validação vai para a
  quarentena com o motivo; nada some em silêncio.
- **Idempotência.** Rodar a mesma fatia duas vezes sobre o mesmo arquivo não
  muda o resultado.
- **Medir antes de afirmar ganho.** Tempo de cada etapa, com o mesmo dado, ao
  lado do tempo da produção.

## Método (SDD)

`spec.md` (o quê e o critério de aceite) → `plan.md` (como) → `tasks.md`
(tarefas com evidência) → `checkpoints.md` (o que foi entregue, com números).
Mudança de escopo entra primeiro na spec.
