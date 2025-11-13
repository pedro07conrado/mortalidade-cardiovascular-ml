import pandas as pd
import requests
import os
import io

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
# URL espelho confiável do Atlas 2013 (Dados Brutos)
URL_CSV = "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/atlas2013_dadosbrutos_pt.csv"

OUTPUT_FILE = "data/processed/atlas_idhm_final.csv"
RAW_DIR = "data/raw/atlas"
RAW_FILE = os.path.join(RAW_DIR, "atlas_raw.csv")

# Período definido: 2000 a 2015
ANOS_CENSITARIOS = [2000, 2010]
ANOS_ALVO = [2000, 2005, 2010, 2015]

COLS_MAP = {
    'ANO': 'ano',
    'Codmun7': 'codmun',
    'Município': 'nome_municipio',
    'UF': 'uf',
    'IDHM': 'idhm',
    'IDHM_R': 'idhm_renda',
    'IDHM_E': 'idhm_educ',
    'IDHM_L': 'idhm_longevidade',
    'RDPC': 'renda_pc',
    'ESPVIDA': 'esp_vida',
    'T_ANALF15M': 'tx_analfabetismo'
}

def download_e_processar():
    print("🚀 Iniciando script de download e processamento...")

    # 1. Criar pasta se não existir
    os.makedirs(RAW_DIR, exist_ok=True)

    # 2. Download do Arquivo (se ainda não tiver baixado)
    if not os.path.exists(RAW_FILE):
        print(f"📥 Baixando arquivo bruto de: {URL_CSV}")
        try:
            response = requests.get(URL_CSV, timeout=30)
            response.raise_for_status() # Para se der erro 404
            
            with open(RAW_FILE, 'wb') as f:
                f.write(response.content)
            print("✅ Download concluído!")
        except Exception as e:
            print(f"❌ Erro no download: {e}")
            return
    else:
        print("✅ Arquivo bruto já existe localmente. Pulando download.")

    # 3. Carregar e Filtrar
    print("⚙️ Processando dados...")
    # O arquivo usa separador ';' e vírgula para decimais
    df = pd.read_csv(RAW_FILE, sep=';', encoding='utf-8', decimal=',', low_memory=False)

    # Filtrar anos 2000 e 2010
    df = df[df['ANO'].isin(ANOS_CENSITARIOS)].copy()
    
    # Selecionar e renomear colunas
    cols_existentes = [c for c in COLS_MAP.keys() if c in df.columns]
    df = df[cols_existentes].rename(columns=COLS_MAP)

    # Garantir código de 7 dígitos
    if 'codmun' in df.columns:
        df['codmun'] = df['codmun'].astype(str).str.zfill(7)

    print(f"   Dados censitários carregados: {len(df)} registros (2000 e 2010).")

    # 4. Interpolação (2000 -> 2005 -> 2010 -> 2015)
    print("🔄 Interpolando anos faltantes...")
    dfs_finais = []
    
    cols_numericas = ['idhm', 'idhm_renda', 'idhm_educ', 'idhm_longevidade', 
                      'renda_pc', 'esp_vida', 'tx_analfabetismo']

    # Agrupar por município para criar a linha do tempo
    for cod, dados_mun in df.groupby('codmun'):
        # Cria timeline 2000, 2005, 2010, 2015
        df_timeline = pd.DataFrame({'ano': ANOS_ALVO})
        
        # Junta com os dados existentes
        df_merged = pd.merge(df_timeline, dados_mun, on='ano', how='left')
        
        # Preenche fixos (Nome, UF, Codmun)
        cols_fixas = ['codmun', 'nome_municipio', 'uf']
        df_merged[cols_fixas] = df_merged[cols_fixas].ffill().bfill()

        # Matemática:
        # Interpolate linear preenche 2005 (média entre 00 e 10)
        df_merged[cols_numericas] = df_merged[cols_numericas].interpolate(method='linear')
        # Ffill preenche 2015 (repetindo 2010)
        df_merged[cols_numericas] = df_merged[cols_numericas].ffill()

        dfs_finais.append(df_merged)

    # 5. Salvar Final
    df_final = pd.concat(dfs_finais, ignore_index=True)
    df_final = df_final[df_final['ano'].isin(ANOS_ALVO)] # Garante só os anos alvo
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')

    print("="*60)
    print(f"✅ SUCESSO! Dataset pronto em: {OUTPUT_FILE}")
    print(f"   Total de linhas: {len(df_final)}")
    print("="*60)

if __name__ == "__main__":
    download_e_processar()