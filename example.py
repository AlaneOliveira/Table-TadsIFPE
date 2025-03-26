import sqlite3
import pandas as pd

conn = sqlite3.connect("database.db")
def create_biblioteca_database():

    Livro_df = pd.DataFrame({
        'LivroID': [1, 2, 3],
        'Titulo': ["Jantar secreto", "Use a cabeça Java", "Arquitetura de computadores"],
        'Autor': ["Rafael Montes", "Bert Bates", "Guilherme Arroz"],
        'Ano': [2016, 2007, 2009],
        'SecaoID': [19, 2, 7]
    })

    LivroSecao_df = pd.DataFrame({
        'SecaoID': [19, 2, 7],
        'NomeSecao': ["Terror", "Programação", "Computação"],  
        'Disponibilidade': [True, False, True]  
    })

    Emprestimo_df = pd.DataFrame({
        'UsuarioID': [11, 22, 33],
        'LivroID': [1, 2, 3],
        'FuncionariosID': [12, 13, 14],
        'DataEmprestimo': ["2024-03-01", "2024-03-10", "2024-03-15"],
        'DataDevolucao': ["2024-03-10", "2024-03-20", None]  # Nenhuma devolução para um usuário
    })

    DadosPessoais_df = pd.DataFrame({
        'Nome': ["Alane", "Eduarda", "Glauco", "João Pedro"],
        'Email': ["Alane@gmail.com", "duda@hotmail.com", "Glaucotelino@outlook.com", "joaop@gmail.com"],
        'Telefone': [123456789, 987654321, 456789123, 321654789],
        'DadosPessoaisID': [123, 456, 789, 891]
    })

    Usuario_df = pd.DataFrame({
        'UsuarioID': [11, 22, 33, 44],
        'DadosPessoaisID': [123, 456, 789, 891]  
    })

    Funcionario_df = pd.DataFrame({
        'FuncionariosID': [12, 13, 14],
        'Nome': ["Carlos", "Mariana", "Roberto"],
        'Cargo': ["Bibliotecário", "Atendente", "Gerente"]
    })

    Livro_df = Livro_df.merge(LivroSecao_df[['SecaoID', 'Disponibilidade']], on='SecaoID', how='left')

    DadosPessoais_df['DominioEmail'] = DadosPessoais_df['Email'].apply(lambda x: x.split('@')[-1])

    Emprestimo_df['DataEmprestimo'] = pd.to_datetime(Emprestimo_df['DataEmprestimo'])
    Emprestimo_df['DataDevolucao'] = pd.to_datetime(Emprestimo_df['DataDevolucao'])
    Emprestimo_df['Status'] = Emprestimo_df['DataDevolucao'].apply(lambda x: 'Pendente' if pd.isna(x) else 'Devolvido')

    Bibliotecarios_df = Funcionario_df[Funcionario_df['Cargo'] == "Bibliotecário"]

    Livro_df.to_sql("Livro", conn, index=False, if_exists="replace")
    LivroSecao_df.to_sql("LivroSecao", conn, index=False, if_exists="replace")
    Emprestimo_df.to_sql("Emprestimo", conn, index=False, if_exists="replace")
    DadosPessoais_df.to_sql("DadosPessoais", conn, index=False, if_exists="replace")
    Usuario_df.to_sql("Usuario", conn, index=False, if_exists="replace")
    Funcionario_df.to_sql("Funcionario", conn, index=False, if_exists="replace")

    conn.close()

