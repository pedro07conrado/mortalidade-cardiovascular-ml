import sidrapy
import pandas as pd
import os
from pathlib import Path

# Salvar em processed
PROCESSED_PATH = Path("../data/processed")
os.makedirs(PROCESSED_PATH, exist_ok=True)

def baixar_pib_total():
    print("💰 Baixando PIB Total (IBGE - Tabela 5938 - Variável 37)...")
    
    try:
        # Tabela 5938: Produto Interno Bruto
        # Variável 37: Produto Interno Bruto a preços correntes
        pib = sidrapy.get_table(
            table_code="5938", 
            territorial_level="6", # Município
            ibge_territorial_code="all", 
            period="2007,2010,2015", 
            variable="37" # MUDANÇA: Usamos 37 (Total) em vez de 597 (Per Capita)
        )
        
        # Limpar Cabeçalho do SIDRA
        pib = pib.iloc[1:] 
        
        # Renomear e Selecionar
        # D1C = Código Município, D2N = Ano, V = Valor
        pib = pib.rename(columns={'D1C': 'codmun', 'D2N': 'ano', 'V': 'pib_total'})
        
        df_pib = pib[['codmun', 'ano', 'pib_total']].copy()
        
        # Tratamento Numérico
        df_pib['pib_total'] = pd.to_numeric(df_pib['pib_total'], errors='coerce')
        df_pib['ano'] = df_pib['ano'].astype(int)
        
        # Salvar
        arquivo_saida = PROCESSED_PATH / "pib_total.parquet"
        df_pib.to_parquet(arquivo_saida, index=False)
        
        print(f" PIB Total Baixado! {len(df_pib)} registros.")
        print(f"   Salvo em: {arquivo_saida}")
        print("   (Calcularemos o 'Per Capita' na etapa final dividindo pela população)")
        
    except Exception as e:
        print(f" Erro crítico: {e}")

if __name__ == "__main__":
    baixar_pib_total()