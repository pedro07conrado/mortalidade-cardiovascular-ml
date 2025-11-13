import sidrapy
import pandas as pd
from tqdm import tqdm
import time

def baixar_populacao_ibge(anos):
    print("📊 Iniciando download via sidrapy...")
    dfs = []

    for ano in tqdm(anos):
        try:
            # Lógica para escolher a tabela certa dependendo do ano
            # Tabela 6579: Estimativas (2001-2021)
            # Tabela 4714: Censo 2022
            # Tabela 202: Censo 2010 (se quiser o dado do censo e não estimativa)
            
            tabela = "6579" # Padrão Estimativas
            if ano == 2022 or ano == 2023:
                tabela = "4714" # Censo 2022
            elif ano < 2001:
                print(f"⚠️ O ano {ano} não está na tabela de estimativas (6579). Ignorando.")
                continue

            # Parâmetros da API do SIDRA
            data = sidrapy.get_table(
                table_code=tabela,
                territorial_level="6", # 6 = Município
                ibge_territorial_code="all", # Todos os municípios
                period=str(ano),
                variable="93" if tabela == "4714" else "9324" # Variável muda conforme a tabela
            )

            # O sidrapy retorna a primeira linha como cabeçalho, precisamos limpar
            if data.empty or len(data) <= 1:
                print(f" ❌ Sem dados para {ano}")
                continue

            # Ajustar cabeçalho
            data.columns = data.iloc[0]
            data = data.iloc[1:]

            # Selecionar e renomear colunas úteis
            # O nome das colunas vem da API, geralmente: 'Município (Código)', 'Ano', 'Valor'
            df_ano = data[['Município (Código)', 'Município', 'Ano', 'Valor']].copy()
            df_ano.columns = ['codmun', 'nome_municipio', 'ano', 'populacao']
            
            # Limpeza crítica: Tratar o "..." e converter para número
            df_ano['populacao'] = pd.to_numeric(df_ano['populacao'], errors='coerce') # 'coerce' transforma erros (...) em NaN
            df_ano = df_ano.dropna(subset=['populacao']) # Remove linhas sem população
            df_ano['populacao'] = df_ano['populacao'].astype(int)
            
            dfs.append(df_ano)
            time.sleep(1) # Respeitar a API

        except Exception as e:
            print(f" ❌ Erro em {ano}: {e}")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.to_csv("populacao_municipios_ibge.csv", index=False)
        print(f"\n✅ Sucesso! {len(df_final)} registros salvos.")
        print(df_final.head())
    else:
        print("\n❌ Nenhum dado baixado.")

if __name__ == "__main__":
    # Note: Removi 1990-2000 pois exigem tabelas de Censo antigas (200, 475) que têm formatos diferentes
    anos_disponiveis = [2001, 2005, 2010, 2015, 2020, 2022] 
    baixar_populacao_ibge(anos_disponiveis)