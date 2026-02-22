import httpx

print("Conectando à API do Tesouro Nacional...")

resposta = httpx.get(
    "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo",
    params={
        "an_exercicio": 2024,
        "nr_periodo": 1,
        "co_tipo_demonstrativo": "RREO",
        "no_uf": "PB",
        "co_poder": "M"
    }
)

print(f"Status da resposta: {resposta.status_code}")

dados = resposta.json()
registros = dados.get("items", [])

print(f"Total de registros retornados: {len(registros)}")

if registros:
    print("\nPrimeiro registro (exemplo):")
    primeiro = registros[0]
    for chave, valor in list(primeiro.items())[:6]:
        print(f"  {chave}: {valor}")
