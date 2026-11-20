# PySpark na Prática

Material gratuito do QG da Engenharia de Dados. Você recebeu porque comentou **SPARK**.

Um processamento de vendas com as operações que mais aparecem no dia a dia.

## Como usar este material

**Opção 1: Google Colab (mais fácil, nada para instalar)**

1. Abra [colab.research.google.com](https://colab.research.google.com) → novo notebook
2. Na primeira célula → `!pip install pyspark`
3. Cole o conteúdo de [vendas_spark.py](vendas_spark.py) em outra célula → rode

**Opção 2: no seu computador**

1. Instale o Java 17 → [adoptium.net](https://adoptium.net)
2. Instale e rode:

```bash
pip install -r requirements.txt
python vendas_spark.py
```

No Windows, gravar arquivos com Spark pede configuração extra (winutils). Para estudar, prefira Colab, WSL ou Databricks Free Edition.

## O fluxo

```
vendas + clientes → limpeza → join → agregação → ranking → parquet particionado
```

## O que o script mostra

| Etapa | Funções |
|---|---|
| Criar DataFrame | `spark.createDataFrame` |
| Limpeza | `dropDuplicates`, `filter`, `withColumn`, `F.trim`, `F.initcap` |
| Join | `join(..., "cliente_id", "left")` |
| Agregação | `groupBy().agg(F.sum, F.count)` |
| Window | `Window.partitionBy().orderBy()` + `F.row_number` |
| Gravar | `write.partitionBy("mes").parquet()` |

## Conceitos em uma linha

**Transformação** → monta o plano, não executa (`filter`, `join`, `select`)
**Ação** → executa o plano (`show`, `count`, `write`)
**Partição** → pedaço do dado processado em paralelo
**Lazy evaluation** → o Spark espera a ação para otimizar o plano inteiro

## Material completo

[Guia-Pyspark](https://github.com/Walterbiel/Guia-Pyspark)

## QG da Engenharia de Dados

Este material é uma amostra do que você encontra no QG.

→ Trilha completa do zero ao sênior
→ Projetos reais para o portfólio
→ Comunidade para tirar dúvidas

Acesse → [qg-engenharia-de-dados.com.br](https://qg-engenharia-de-dados.com.br)
YouTube → [@gonzagadosdados](https://www.youtube.com/@gonzagadosdados)
