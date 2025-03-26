import sqlite3
import pandas as pd

# Criar conexão SQLite com extensão correta
conn = sqlite3.connect("database.db")

# Testar conexão
if conn:
    print("✅ Conexão com SQLite estabelecida!")
else:
    print("❌ Erro ao conectar ao SQLite!")

def create_example():
    client_info = pd.DataFrame({
        "client_id": [1, 2, 3, 4, 5, 6, 7],
        "client_name": ["ronaldo", "david", "fernanda", "glauco", "joão", "marta", "jair"],
        "cnh": ["12345678900", "98765432101", "11223344556", "99887766554", "22334455667", "66554433221", "33445566788"]
    })

    client_numbers = pd.DataFrame({
        "clients_id": [1, 2, 2, 4, 6, 6, 7],
        "numbers": ["(11) 98765-4321", "(21) 99876-5432", "(31) 92345-6789", "(41) 93456-7890", "(51) 91234-5678", "(61) 94567-8901", "(71) 98765-1234"]
    })

    cars_info = pd.DataFrame({
        "clients_id": [1, 2, 3, 4, 5, 6, 7],
        "car_id": [1, 2, 3, 4, 5, 6, 7],
        "brand": ["toyota corolla", "chevrolet camaro", "ford mustang", "BMW X5", "Volkswagem golf", "Fiat argo", "renault kwid"],
        "mileage": [90000, 12000, 40000, 129000, 47820, 124009, 0],
        "price": [119999, 84999, 94999, 134999, 64999, 44999, 29000]
    })

    subsidiaryAdress_info = pd.DataFrame({
        "subsidiary_id": [1, 2, 3, 4, 5, 6, 7],
        "subsidiary_Adress": ["recife", "salvador", "rio de janeiro", "santa catarina", "florianopólis", "são luiz", "alagoas"],
    })

    subsidiarystock_info = pd.DataFrame({
        "subsidiaryAdress_id": [1, 2, 3, 4, 5, 6, 7],
        "stock_id": [1, 2, 3, 4, 5, 6, 7],
        "subsidiary_stockCars": [90, 99, 192, 219, 109, 132, 89]
    })

    return {
        "client": client_info,
        "client_numbers": client_numbers,
        "cars": cars_info,
        "subsidiary_Adress": subsidiaryAdress_info,
        "subsidiary_stock": subsidiarystock_info
    }

# Criando DataFrames
dataframes = create_example()

# Salvar DataFrames no banco SQLite
for table_name, df in dataframes.items():
    df.to_sql(table_name, conn, index=False, if_exists="replace")

# INNER JOIN - Clientes e seus carros
query_inner_client_cars = """
SELECT client.client_id, client.client_name, cars.brand, cars.mileage, cars.price
FROM client
INNER JOIN cars ON client.client_id = cars.clients_id
"""
client_cars = pd.read_sql(query_inner_client_cars, conn)

# LEFT JOIN - Clientes e seus números de telefone
query_left_client_numbers = """
SELECT client.client_id, client.client_name, client.cnh, client_numbers.numbers
FROM client
LEFT JOIN client_numbers ON client.client_id = client_numbers.clients_id
"""
client_numbers_info = pd.read_sql(query_left_client_numbers, conn)

# INNER JOIN - Filiais e seus estoques
query_inner_Adress_stock = """
SELECT subsidiary_Adress.subsidiary_id, subsidiary_Adress.subsidiary_Adress, subsidiary_stock.subsidiary_stockCars
FROM subsidiary_Adress
INNER JOIN subsidiary_stock ON subsidiary_Adress.subsidiary_id = subsidiary_stock.subsidiaryAdress_id
"""
subsidiary_info = pd.read_sql(query_inner_Adress_stock, conn)

# Exibir resultados
print("\n-----------------------------------------------------")
print("Clientes e seus números de telefone:")
print("-----------------------------------------------------")
print(client_numbers_info)

print("\n-----------------------------------------------------")
print("Preços dos carros:")
print("-----------------------------------------------------")
print(dataframes["cars"][["car_id", "brand", "price"]])

print("\n-----------------------------------------------------")
print("Informações das filiais e estoque:")
print("-----------------------------------------------------")
print(subsidiary_info)

print("\n-----------------------------------------------------")
print("Clientes e seus carros:")
print("-----------------------------------------------------")
print(client_cars)

# Fechar conexão
conn.close()