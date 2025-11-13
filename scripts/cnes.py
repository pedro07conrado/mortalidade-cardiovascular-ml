"""
Script para baixar e processar dados do CNES (Cadastro Nacional de Estabelecimentos de Saúde)
para as 100 maiores cidades brasileiras nos anos 2000, 2005, 2010, 2015

Instale antes de rodar:
pip install pysus pandas numpy requests beautifulsoup4
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# ============================================================
# ALTERNATIVA: Usar requests + BeautifulSoup para TabNet
# (pysus pode ter problemas com anos antigos)
# ============================================================

import requests
from io import StringIO
import time

def baixar_tabnet_cnes(tipo='leitos', ano=2015, mes=12):
    """
    Baixa dados do TabNet DATASUS usando requests
    
    Parâmetros:
    - tipo: 'leitos', 'profissionais', 'estabelecimentos'
    - ano: ano desejado
    - mes: mês desejado (use 12 para ter snapshot anual)
    """
    
    # URLs do TabNet por tipo de dado
    urls = {
        'leitos': 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def',
        'profissionais': 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/prid02br.def',
        'estabelecimentos': 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/estabbr.def'
    }
    
    print(f"⚠️ TabNet requer download manual. Acesse:")
    print(f"   {urls[tipo]}")
    print(f"   Configure: Linha=Município, Coluna=Ano/mês, Período={mes:02d}/{ano}")
    print(f"   Exporte como CSV e salve em: dados/cnes_{tipo}_{ano}.csv")
    
    return None


# ============================================================
# MÉTODO ALTERNATIVO: Processar arquivos já baixados manualmente
# ============================================================

def processar_csv_tabnet(caminho_csv, ano, tipo_dado):
    """
    Processa CSV exportado do TabNet DATASUS
    
    O CSV do TabNet tem um formato específico:
    - Cabeçalho nas primeiras linhas
    - Dados começam após "Município"
    - Total no final
    """
    
    try:
        # Ler arquivo pulando linhas de cabeçalho
        with open(caminho_csv, 'r', encoding='latin-1') as f:
            linhas = f.readlines()
        
        # Encontrar linha que começa com "Município" ou similar
        inicio_dados = 0
        for i, linha in enumerate(linhas):
            if 'Município' in linha or 'Municipio' in linha:
                inicio_dados = i
                break
        
        # Ler a partir da linha de dados
        df = pd.read_csv(
            caminho_csv, 
            skiprows=inicio_dados,
            encoding='latin-1',
            sep=';',
            thousands='.',
            decimal=','
        )
        
        # Limpar nome das colunas
        df.columns = df.columns.str.strip()
        
        # Remover linhas de total
        df = df[~df.iloc[:, 0].str.contains('Total', na=False)]
        
        # Adicionar ano
        df['ano'] = ano
        df['tipo_dado'] = tipo_dado
        
        print(f"✅ {tipo_dado} {ano}: {len(df)} municípios carregados")
        
        return df
        
    except Exception as e:
        print(f"❌ Erro ao processar {caminho_csv}: {e}")
        return None


# ============================================================
# PROCESSAR MÚLTIPLOS ANOS
# ============================================================

def consolidar_dados_cnes(anos=[2000, 2005, 2010, 2015]):
    """
    Consolida dados do CNES de múltiplos anos
    
    Você deve ter baixado manualmente e salvado como:
    - dados/cnes_leitos_2000.csv
    - dados/cnes_leitos_2005.csv
    - etc.
    """
    
    dfs_leitos = []
    dfs_profissionais = []
    dfs_estabelecimentos = []
    
    for ano in anos:
        print(f"\n📅 Processando ano {ano}...")
        
        # Leitos
        caminho_leitos = f'dados/cnes_leitos_{ano}.csv'
        if os.path.exists(caminho_leitos):
            df = processar_csv_tabnet(caminho_leitos, ano, 'leitos_sus')
            if df is not None:
                dfs_leitos.append(df)
        else:
            print(f"⚠️ Arquivo não encontrado: {caminho_leitos}")
        
        # Profissionais
        caminho_prof = f'dados/cnes_profissionais_{ano}.csv'
        if os.path.exists(caminho_prof):
            df = processar_csv_tabnet(caminho_prof, ano, 'profissionais')
            if df is not None:
                dfs_profissionais.append(df)
        
        # Estabelecimentos
        caminho_estab = f'dados/cnes_estabelecimentos_{ano}.csv'
        if os.path.exists(caminho_estab):
            df = processar_csv_tabnet(caminho_estab, ano, 'estabelecimentos')
            if df is not None:
                dfs_estabelecimentos.append(df)
    
    # Concatenar todos os anos
    df_leitos = pd.concat(dfs_leitos, ignore_index=True) if dfs_leitos else None
    df_prof = pd.concat(dfs_profissionais, ignore_index=True) if dfs_profissionais else None
    df_estab = pd.concat(dfs_estabelecimentos, ignore_index=True) if dfs_estabelecimentos else None
    
    return df_leitos, df_prof, df_estab


# ============================================================
# FILTRAR 100 MAIORES CIDADES E CALCULAR TAXAS
# ============================================================

def processar_cnes_100_cidades(df_leitos, df_prof, df_estab, df_populacao, top_100_cidades):
    """
    Filtra e processa dados CNES para as 100 maiores cidades
    Calcula taxas por 100k habitantes
    """
    
    # Criar dataset final
    df_cnes = pd.DataFrame()
    
    # Processar cada tipo de dado
    datasets = {
        'leitos': df_leitos,
        'profissionais': df_prof,
        'estabelecimentos': df_estab
    }
    
    for tipo, df in datasets.items():
        if df is not None:
            print(f"\n⚙️ Processando {tipo}...")
            
            # Identificar coluna de município (pode variar)
            col_municipio = [c for c in df.columns if 'munic' in c.lower()][0]
            
            # Extrair código IBGE do nome (formato: "123456 Nome da Cidade")
            df['codmun'] = df[col_municipio].str.extract(r'(\d{6,7})')[0].astype(int)
            
            # Filtrar apenas 100 maiores cidades
            df = df[df['codmun'].isin(top_100_cidades['codmun'])]
            
            # Identificar coluna de valores
            col_valores = [c for c in df.columns if c not in [col_municipio, 'ano', 'tipo_dado', 'codmun']][0]
            
            # Renomear coluna de valores
            if tipo == 'leitos':
                df.rename(columns={col_valores: 'leitos_sus'}, inplace=True)
            elif tipo == 'profissionais':
                df.rename(columns={col_valores: 'medicos'}, inplace=True)
            elif tipo == 'estabelecimentos':
                df.rename(columns={col_valores: 'unidades_saude'}, inplace=True)
            
            # Merge com população para calcular taxas
            df = df.merge(df_populacao[['codmun', 'ano', 'populacao']], on=['codmun', 'ano'], how='left')
            
            # Calcular taxa por 100k habitantes
            if tipo == 'leitos':
                df['leitos_sus_100k'] = (df['leitos_sus'] / df['populacao']) * 100000
            elif tipo == 'profissionais':
                df['medicos_100k'] = (df['medicos'] / df['populacao']) * 100000
            
            # Adicionar ao dataset final
            colunas_manter = ['codmun', 'ano'] + [c for c in df.columns if '100k' in c or c in ['unidades_saude']]
            
            if df_cnes.empty:
                df_cnes = df[colunas_manter]
            else:
                df_cnes = df_cnes.merge(df[colunas_manter], on=['codmun', 'ano'], how='outer')
    
    return df_cnes


# ============================================================
# DADOS DE ESF (Estratégia Saúde da Família)
# ============================================================

def obter_cobertura_esf():
    """
    Cobertura ESF está em sistema separado (e-Gestor AB)
    
    Para simplicidade, vamos criar uma aproximação baseada em:
    - Presença de Unidades Básicas de Saúde
    - Dados de estabelecimentos do CNES
    
    ALTERNATIVA: Baixar manualmente de:
    https://egestorab.saude.gov.br/paginas/acessoPublico/relatorios/relHistoricoCoberturaAB.xhtml
    """
    
    print("\n⚠️ COBERTURA ESF:")
    print("   Baixe manualmente de:")
    print("   https://egestorab.saude.gov.br/")
    print("   Ou use aproximação baseada em estabelecimentos do CNES")
    
    return None


# ============================================================
# PIPELINE COMPLETO
# ============================================================

def main():
    print("="*70)
    print("🏥 SCRIPT DE COLETA DADOS CNES")
    print("="*70)
    
    # Verificar se existem arquivos CSV baixados
    print("\n📂 Verificando arquivos necessários...")
    
    anos = [2000, 2005, 2010, 2015]
    tipos = ['leitos', 'profissionais', 'estabelecimentos']
    
    arquivos_faltantes = []
    for ano in anos:
        for tipo in tipos:
            caminho = f'dados/cnes_{tipo}_{ano}.csv'
            if not os.path.exists(caminho):
                arquivos_faltantes.append(caminho)
    
    if arquivos_faltantes:
        print("\n⚠️ ARQUIVOS FALTANTES:")
        print("   Você precisa baixar manualmente do TabNet DATASUS:")
        print()
        
        for tipo in tipos:
            if tipo == 'leitos':
                url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def'
                conteudo = 'Quantidade SUS'
            elif tipo == 'profissionais':
                url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/prid02br.def'
                conteudo = 'Quantidade'
            else:
                url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/estabbr.def'
                conteudo = 'Estabelecimentos'
            
            print(f"\n   📥 {tipo.upper()}:")
            print(f"      URL: {url}")
            print(f"      Configure: Linha=Município, Coluna=Ano/mês compet., Conteúdo={conteudo}")
            print(f"      Períodos: Dez/2000, Dez/2005, Dez/2010, Dez/2015")
            print(f"      Exporte como CSV e salve em: dados/cnes_{tipo}_XXXX.csv")
        
        print("\n" + "="*70)
        print("⏸️  PAUSE: Baixe os arquivos antes de continuar")
        print("="*70)
        return
    
    # Carregar dados de população e 100 maiores cidades
    print("\n📂 Carregando dados de população...")
    df_pop = pd.read_csv('dados/ibge_populacao.csv')
    
    # Você já deve ter isso do script anterior
    # Se não tiver, calcule novamente
    pop_media = df_pop.groupby('codmun')['populacao'].mean().reset_index()
    top_100 = pop_media.nlargest(100, 'populacao')[['codmun']]
    
    # Consolidar dados CNES
    print("\n⚙️ Processando dados CNES...")
    df_leitos, df_prof, df_estab = consolidar_dados_cnes(anos)
    
    # Processar e calcular taxas
    print("\n🔄 Calculando taxas por 100k habitantes...")
    df_cnes_final = processar_cnes_100_cidades(df_leitos, df_prof, df_estab, df_pop, top_100)
    
    # Adicionar PIB per capita (IBGE Sidra)
    print("\n💰 Adicionando PIB per capita...")
    # Você pode baixar do IBGE Sidra Tabela 5938
    # Por enquanto, vamos deixar como None
    df_cnes_final['pib_pc'] = None
    
    # Cobertura ESF (aproximação ou download manual)
    df_cnes_final['cobertura_esf'] = None
    
    # Validação
    print("\n" + "="*70)
    print("📊 VALIDAÇÃO DOS DADOS CNES")
    print("="*70)
    print(f"\n📈 Registros por ano:")
    print(df_cnes_final['ano'].value_counts().sort_index())
    
    print(f"\n🏙️ Cidades únicas: {df_cnes_final['codmun'].nunique()}")
    
    print(f"\n📋 Colunas disponíveis:")
    print(df_cnes_final.columns.tolist())
    
    print(f"\n❌ Valores faltantes:")
    print(df_cnes_final.isnull().sum())
    
    # Salvar
    print("\n💾 Salvando arquivo final...")
    df_cnes_final.to_csv('dados/cnes_100_cidades_2000_2015.csv', index=False)
    df_cnes_final.to_parquet('dados/cnes_100_cidades_2000_2015.parquet', index=False)
    
    print("\n✅ Processamento concluído!")
    print(f"📄 Arquivo salvo: cnes_100_cidades_2000_2015.csv")
    print(f"📊 Total de registros: {len(df_cnes_final)}")


# ============================================================
# INSTRUÇÕES PARA DOWNLOAD MANUAL
# ============================================================

def imprimir_instrucoes():
    """
    Imprime instruções detalhadas para download manual
    """
    print("""
╔════════════════════════════════════════════════════════════════════╗
║       INSTRUÇÕES PARA DOWNLOAD MANUAL DO CNES (TabNet)             ║
╚════════════════════════════════════════════════════════════════════╝

📥 LEITOS SUS
   1. Acesse: http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/leiintbr.def
   2. Configure:
      • Linha: Município
      • Coluna: Ano/mês compet.
      • Conteúdo: Quantidade SUS
   3. Períodos disponíveis: Marque Dez/2000, Dez/2005, Dez/2010, Dez/2015
   4. Clique em "Mostra" e depois "Arquivo CSV"
   5. Salve como: dados/cnes_leitos_XXXX.csv (um arquivo por ano)

📥 PROFISSIONAIS (MÉDICOS)
   1. Acesse: http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/prid02br.def
   2. Configure:
      • Linha: Município
      • Coluna: Ano/mês compet.
      • Conteúdo: Quantidade
      • CBO 2002: 2231 - Médicos (ou selecione "Todos")
   3. Períodos: Dez/2000, Dez/2005, Dez/2010, Dez/2015
   4. Salve como: dados/cnes_profissionais_XXXX.csv

📥 ESTABELECIMENTOS
   1. Acesse: http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/estabbr.def
   2. Configure:
      • Linha: Município
      • Coluna: Ano/mês compet.
      • Conteúdo: Estabelecimentos
   3. Períodos: Dez/2000, Dez/2005, Dez/2010, Dez/2015
   4. Salve como: dados/cnes_estabelecimentos_XXXX.csv

⚠️ OBSERVAÇÕES:
   • Dados de 2000 podem não estar disponíveis (CNES foi criado em 2003)
   • Se não houver dados de 2000, comece em 2005
   • Depois de baixar todos os arquivos, rode este script novamente

╚════════════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--instrucoes':
        imprimir_instrucoes()
    else:
        main()