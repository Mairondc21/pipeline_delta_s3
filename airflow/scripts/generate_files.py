import json
import random

import pandas as pd


random.seed(42)

path_output = "data/"

# ─────────────────────────────────────────
# CLIENTES.CSV
# ─────────────────────────────────────────
clientes_csv = {
    "cliente_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "nome": [
        "JOAO SILVA", "maria souza", "Carlos Oliveira", "ANA LIMA", "Pedro Costa",
        "FERNANDA ROCHA", "lucas mendes", "Beatriz Alves", "RODRIGO NUNES", "juliana castro"
    ],
    "cpf": [
        "123.456.789-00", "98765432100", "321.654.987-11", "11122233344", "555.666.777-88",
        "99988877766", "444.333.222-11", "12312312312", "777.888.999-00", "00011122233"
    ],
    "data_nascimento": [
        "1990-05-14", "22/08/1985", "1978-11-30", "05/03/1995", "1988-07-19",
        "1992-12-01", "15/06/2000", "1975-04-22", "1983-09-08", "28/01/1998"
    ],
    "genero": [
        "M", "Feminino", "male", "F", "Masculino",
        "female", "M", None, "MASCULINO", "F"
    ],
    "email": [
        "joao@email.com", "maria@email.com", "carlos_invalido", "ana@email.com", "pedro@email.com",
        "fernanda@email.com", "lucas", "beatriz@email.com", "rodrigo@email.com", "juliana@email.com"
    ],
    "telefone": [
        "(31) 99999-0001", "31988880002", "(11) 97777-0003", "21966660004", "(41) 95555-0005",
        "51944440006", "(71) 93333-0007", "85922220008", "(62) 91111-0009", "48900000010"
    ],
    "cidade": [
        "Belo Horizonte", "São Paulo", "Rio de Janeiro", "Curitiba", "Porto Alegre",
        "Salvador", "Fortaleza", "Recife", "Goiânia", "Florianópolis"
    ],
    "estado": ["MG", "SP", "RJ", "PR", "RS", "BA", "CE", "PE", "GO", "SC"],
    "renda_mensal": [
        5000.00, 8500.00, -300.00, 12000.00, 3200.00,
        7600.00, 2100.00, 15000.00, 4400.00, 9800.00
    ],
    "score_credito": [720, 850, 410, 930, 610, 780, 510, 990, 670, 800],
    "data_cadastro": [
        "2020-01-10 08:00:00", "2019-06-15 10:30:00", "2021-03-22 14:00:00",
        "2018-11-05 09:15:00", "2022-07-01 11:45:00", "2020-09-18 16:00:00",
        "2023-02-28 08:30:00", "2017-04-10 13:00:00", "2021-08-14 10:00:00",
        "2022-12-01 15:30:00"
    ],
    "status_cliente": [
        "ativo", "ativo", "bloqueado", "ativo", "inativo",
        "ativo", "ativo", "ativo", "bloqueado", "ativo"
    ]
}

df_clientes = pd.DataFrame(clientes_csv)
df_clientes.to_csv(f"{path_output}/clientes.csv", index=False)
print("✅ clientes.csv gerado")

# ─────────────────────────────────────────
# CONTAS.PARQUET
# ─────────────────────────────────────────
contas_parquet = {
    "conta_id":          [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
    "cliente_id":        [1,   2,   3,   4,   5,   6,   7,   8,   9,  10],
    "tipo_conta":        ["corrente","poupança","corrente","investimento","poupança",
                          "corrente","corrente","investimento","poupança","corrente"],
    "agencia":           ["0001","0042","0015","0001","0033","0042","0010","0001","0055","0020"],
    "numero_conta":      [100001,100002,100003,100004,100005,100006,100007,100008,100009,100010],
    "saldo_atual":       [1200.50, 8750.00, 0.00, 45000.00, 320.75,
                          5100.00, 980.20, 120000.00, 430.00, 6700.90],
    "limite_credito":    [3000.00, 5000.00, None, 20000.00, 1000.00,
                          4000.00, 1500.00, 50000.00, None, 8000.00],
    "data_abertura":     ["2020-01-10","2019-06-15","2021-03-22","2018-11-05","2022-07-01",
                          "2020-09-18","2023-02-28","2017-04-10","2021-08-14","2022-12-01"],
    "data_encerramento": [None, None, "2023-06-01", None, None,
                          None, None, None, None, None],
    "status_conta":      ["ativa","ativa","encerrada","ativa","ativa",
                          "ativa","ativa","ativa","bloqueada","ativa"],
    "banco_codigo":      ["341","033","001","341","104",
                          "033","001","341","237","104"]
}

df_contas = pd.DataFrame(contas_parquet)
df_contas.to_parquet(f"{path_output}/contas.parquet", index=False)
print("✅ contas.parquet gerado")

# ─────────────────────────────────────────
# TRANSACOES.JSON
# ─────────────────────────────────────────
transacoes_json = [
    {"transacao_id": "a1b2c3d4-0001", "conta_id": 101, "tipo_transacao": "pix",      "valor": 250.00,    "data_transacao": "2024-01-15T10:30:00-03:00", "descricao": "Pagamento aluguel",     "categoria": "moradia",       "status_transacao": "concluida", "canal": "app",               "conta_destino_id": 102, "cidade_transacao": "Belo Horizonte"},
    {"transacao_id": "a1b2c3d4-0002", "conta_id": 102, "tipo_transacao": "ted",      "valor": 1500.00,   "data_transacao": "2024-01-16T14:00:00-03:00", "descricao": "Transferência",          "categoria": None,            "status_transacao": "concluida", "canal": "internet_banking",  "conta_destino_id": 105, "cidade_transacao": "São Paulo"},
    {"transacao_id": "a1b2c3d4-0003", "conta_id": 103, "tipo_transacao": "debito",   "valor": 0.00,      "data_transacao": "2024-01-17T09:15:00-03:00", "descricao": None,                     "categoria": "alimentação",   "status_transacao": "ERRO",      "canal": "caixa_eletronico",  "conta_destino_id": None, "cidade_transacao": "Rio de Janeiro"},
    {"transacao_id": "a1b2c3d4-0004", "conta_id": 104, "tipo_transacao": "credito",  "valor": 5000.00,   "data_transacao": "2024-01-18T16:45:00-03:00", "descricao": "Salário",               "categoria": "receita",       "status_transacao": "concluida", "canal": "app",               "conta_destino_id": None, "cidade_transacao": "Curitiba"},
    {"transacao_id": "a1b2c3d4-0005", "conta_id": 101, "tipo_transacao": "saque",    "valor": -200.00,   "data_transacao": "2024-01-19T11:00:00-03:00", "descricao": "Saque ATM",              "categoria": "saque",         "status_transacao": "concluida", "canal": "caixa_eletronico",  "conta_destino_id": None, "cidade_transacao": "Belo Horizonte"},
    {"transacao_id": "a1b2c3d4-0006", "conta_id": 105, "tipo_transacao": "pix",      "valor": 89.90,     "data_transacao": "2024-01-20T08:30:00-03:00", "descricao": "iFood",                 "categoria": "alimentação",   "status_transacao": "concluida", "canal": "app",               "conta_destino_id": None, "cidade_transacao": "Porto Alegre"},
    {"transacao_id": "a1b2c3d4-0007", "conta_id": 106, "tipo_transacao": "doc",      "valor": 3200.00,   "data_transacao": "2024-01-21T13:00:00-03:00", "descricao": "Pagamento fornecedor",  "categoria": "negócios",      "status_transacao": "pendente",  "canal": "internet_banking",  "conta_destino_id": 108, "cidade_transacao": "Salvador"},
    {"transacao_id": "a1b2c3d4-0008", "conta_id": 107, "tipo_transacao": "debito",   "valor": 45.00,     "data_transacao": "2024-01-22T19:00:00-03:00", "descricao": "Uber",                  "categoria": "transporte",    "status_transacao": "concluida", "canal": "app",               "conta_destino_id": None, "cidade_transacao": "Fortaleza"},
    {"transacao_id": "a1b2c3d4-0009", "conta_id": 108, "tipo_transacao": "ted",      "valor": 10000.00,  "data_transacao": "2024-01-23T10:00:00-03:00", "descricao": "Investimento CDB",      "categoria": "investimento",  "status_transacao": "concluida", "canal": "internet_banking",  "conta_destino_id": None, "cidade_transacao": "Recife"},
    {"transacao_id": "a1b2c3d4-0010", "conta_id": 109, "tipo_transacao": "pix",      "valor": 120.00,    "data_transacao": "2024-01-24T12:30:00-03:00", "descricao": None,                     "categoria": None,            "status_transacao": "cancelada", "canal": "app",               "conta_destino_id": 101, "cidade_transacao": "Goiânia"},
    {"transacao_id": "a1b2c3d4-0011", "conta_id": 110, "tipo_transacao": "credito",  "valor": 8500.00,   "data_transacao": "2024-01-25T09:00:00-03:00", "descricao": "Salário",               "categoria": "receita",       "status_transacao": "concluida", "canal": "internet_banking",  "conta_destino_id": None, "cidade_transacao": "Florianópolis"},
    {"transacao_id": "a1b2c3d4-0012", "conta_id": 102, "tipo_transacao": "debito",   "valor": 199.90,    "data_transacao": "2024-01-26T15:00:00-03:00", "descricao": "Mercado",               "categoria": "alimentação",   "status_transacao": "concluida", "canal": "app",               "conta_destino_id": None, "cidade_transacao": "São Paulo"},
    {"transacao_id": "a1b2c3d4-0006", "conta_id": 105, "tipo_transacao": "pix",      "valor": 89.90,     "data_transacao": "2024-01-20T08:30:00-03:00", "descricao": "iFood",                 "categoria": "alimentação",   "status_transacao": "concluida", "canal": "app",               "conta_destino_id": None, "cidade_transacao": "Porto Alegre"},
]

with open(f"{path_output}/transacoes.json", "w", encoding="utf-8") as f:
    json.dump(transacoes_json, f, ensure_ascii=False, indent=2)
print("✅ transacoes.json gerado")

# ─────────────────────────────────────────
# CARTOES.CSV
# ─────────────────────────────────────────
cartoes_csv = {
    "cartao_id":        [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
    "conta_id":         [101,  102,  104,  105,  106,  107,  108,  110],
    "tipo_cartao":      ["multiplo","credito","credito","debito","multiplo","debito","credito","multiplo"],
    "bandeira":         ["visa","mastercard","elo","visa","mastercard","elo","visa","mastercard"],
    "limite_total":     [3000.00, 5000.00, 20000.00, 1000.00, 4000.00, 1500.00, 50000.00, 8000.00],
    "limite_utilizado": [1200.00, 4800.00, 21000.00, 200.00, 3950.00, 0.00, 12000.00, 7500.00],
    "data_emissao":     ["2020-01-10","2019-06-15","2018-11-05","2022-07-01",
                         "2020-09-18","2023-02-28","2017-04-10","2022-12-01"],
    "data_validade":    ["2025-01-01","2024-06-01","2026-11-01","2027-07-01",
                         "2025-09-01","2028-02-01","2025-04-01","2027-12-01"],
    "status_cartao":    ["ativo","ativo","bloqueado","ativo","ativo","ativo","ativo","ativo"],
    "internacional":    ["true", "false", "true", "True", "false", "FALSE", "true", "1"]
}

df_cartoes = pd.DataFrame(cartoes_csv)
df_cartoes.to_csv(f"{path_output}/cartoes.csv", index=False)
print("✅ cartoes.csv gerado")

print("\n📦 Arquivos gerados:")
print(f"  clientes.csv   → {len(df_clientes)} linhas")
print(f"  contas.parquet → {len(df_contas)} linhas")
print(f"  transacoes.json→ {len(transacoes_json)} registros (1 duplicata intencional)")
print(f"  cartoes.csv    → {len(df_cartoes)} linhas")
print("\n⚠️  Dados sujos embutidos:")
print("  - clientes: CPF em formatos mistos, datas inconsistentes, gênero variado, email inválido, renda negativa")
print("  - contas: limite_credito com nulls")
print("  - transacoes: valor 0 e negativo, status 'ERRO', categoria null, 1 duplicata, timezone no timestamp")
print("  - cartoes: limite_utilizado > limite_total (linhas 1003, 1005), internacional como string mista")