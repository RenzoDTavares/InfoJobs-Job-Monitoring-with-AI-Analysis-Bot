import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote_plus
import time
import sqlite3
import logging
import re 
import os 
from datetime import datetime
import html 
import google.genai as genai 
from google.genai import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_VAGAS_NOME = "vagasINFO.db"
DB_CLIENTES_NOME = "clientes.db"
URL_BASE = "https://www.infojobs.com.br"
URL_TEMPLATE = URL_BASE + "/empregos.aspx?palabra="
TEMPO_ESPERA = 30 
VAGAS_LIMITE_POPULACAO = 10 
RETRY_DELAY = 2 
MAX_RETRIES = 3 

TELEGRAM_TOKEN = "YOUR_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

API_KEY = "YOUR_GEMINI_API_KEY"
gemini_client = None 

prompt_sistema = (
    "Você é um assistente de recrutamento e analista de QA. Sua tarefa é analisar a descrição de uma vaga de emprego "
    "e fornecer um resumo conciso e de alta qualidade (máximo 4 tópicos). "
    "O resumo deve destacar as responsabilidades principais, os requisitos obrigatórios (hard skills) "
    "e os benefícios/diferenciais que o candidato deve saber antes de se candidatar. "
    
    "A saída deve ser texto simples (RAW TEXT). Não use caracteres de formatação Markdown como * (asterisco), ** (negrito) ou # (cabeçalho). CADA TÓPICO DEVE SER SEPARADO POR DUAS QUEBRAS DE LINHA (\n\n)."
)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.google.com/',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}

def safe_escape(text):
    """Escapa texto para HTML (mantém quebras de linha como \n)."""
    if text is None:
        return ""
    return html.escape(str(text)).replace('\r', '')

def build_resumo_html(resumo_raw):
    """Transforma o resumo RAW da IA em um texto seguro para parse_mode=HTML.
       - Assume que tópicos são separados por '\n\n' (como no prompt).
       - Cada tópico vira um bullet (• )."""
    if not resumo_raw:
        return ""
    resumo = resumo_raw.strip()
    resumo = re.sub(r'Aqui está o resumo conciso[\s\S]*?\n\n', '', resumo, flags=re.IGNORECASE)
    resumo = resumo.replace('\r\n', '\n').replace('\r', '\n')
    topicos = [t.strip() for t in resumo.split('\n\n') if t.strip()]
    bullets = []
    for t in topicos:
        t_escaped = safe_escape(t)
        bullets.append("• " + t_escaped)
    return '\n\n'.join(bullets)


def send_telegram_message(message):
    """Envia uma mensagem formatada para o Telegram (HTML) com logs detalhados e truncamento."""
    if not message:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    TELEGRAM_MAX_LEN = 4096
    if len(message) > TELEGRAM_MAX_LEN:
        logging.warning("Mensagem excede 4096 caracteres — truncando antes do envio.")
        message = message[:TELEGRAM_MAX_LEN - 80] + "\n\n[...mensagem truncada...]"

    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML', 
        'disable_web_page_preview': True
    }
    response = None
    try:
        response = requests.post(url, data=payload, timeout=15)
        response.raise_for_status()
        logging.info(" [TELEGRAM] Mensagem enviada com sucesso!")
    except requests.exceptions.RequestException as e:
        resp_text = "<sem resposta>"
        try:
            if response is not None:
                resp_text = response.text
        except Exception:
            pass
        logging.error(f"❌ [TELEGRAM] Erro ao enviar mensagem: {e}. Resposta Telegram: {resp_text}")


def analisa_vaga_com_ia(client, descricao_completa):
    """Usa o Gemini para resumir a descrição da vaga com até 3 tentativas (Retry)."""
    
    if not client:
         logging.warning(" [IA] Chamada de IA real falhou por falta de cliente. Usando Mock simples.")
         return "Falha ao iniciar a IA. Verifique a chave."
         
    if not descricao_completa or len(descricao_completa) < 50:
        return "\n[Análise Gemini - Descrição não disponível para análise.]"
        
    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=[prompt_sistema, descricao_completa],
            )
            return "\n" + response.text.strip()
            
        except Exception as e:
            error_message = str(e)
            logging.error(f" [IA ERROR]: Falha na tentativa {attempt + 1}. Erro: {error_message}")

            if attempt == MAX_RETRIES - 1:
                return f"\n[Análise Gemini - FALHA CRÍTICA após {MAX_RETRIES} tentativas. Erro: {error_message}]"
            
            sleep_time = 2 ** attempt
            logging.info(f" [IA RETRY]: Modelo sobrecarregado. Aguardando {sleep_time}s antes de tentar novamente...")
            time.sleep(sleep_time)

    return "\n[Análise Gemini - FALHA INESPERADA. Verifique a chave ou o serviço.]"

def inicializa_db_vagas():
    """Cria a tabela de vagas com a chave composta (id + busca_termo)."""
    con = sqlite3.connect(DB_VAGAS_NOME)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vagas_encontradas (
            vagas_id TEXT NOT NULL, 
            busca_termo TEXT NOT NULL,
            titulo TEXT,
            empresa TEXT,
            localizacao TEXT,
            salario TEXT,
            modalidade TEXT,
            url_vaga TEXT,
            resumo_ia TEXT,
            data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (vagas_id, busca_termo)
        )
    """)
    con.commit()
    con.close()
    logging.info(f" [DB] Banco de dados '{DB_VAGAS_NOME}' inicializado.")

def salva_vaga_no_db(dados):
    """Salva a vaga no DB."""
    con = sqlite3.connect(DB_VAGAS_NOME)
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO vagas_encontradas (
                vagas_id, busca_termo, titulo, empresa, localizacao, salario, 
                modalidade, url_vaga, resumo_ia
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, dados)
        con.commit()
    except sqlite3.Error as e:
        logging.error(f" [DB] Erro ao salvar vaga: {e}")
    finally:
        con.close()

def limpa_vagas_por_termo(search_term):
    """Limpa todas as vagas para um termo específico."""
    con = sqlite3.connect(DB_VAGAS_NOME)
    cur = con.cursor()
    try:
        cur.execute("DELETE FROM vagas_encontradas WHERE busca_termo = ?", (search_term,))
        con.commit()
        logging.info(f" [DB LIMPEZA] Limpeza forçada concluída para o termo '{search_term}'.")
    except sqlite3.Error as e:
        logging.error(f" [DB LIMPEZA] Erro ao limpar vagas: {e}")
    finally:
        con.close()

def verifica_vaga_existe(vagas_id, busca_termo):
    """Verifica se a vaga já existe no DB para o termo de busca específico."""
    con = sqlite3.connect(DB_VAGAS_NOME)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM vagas_encontradas WHERE vagas_id = ? AND busca_termo = ?", (vagas_id, busca_termo))
    existe = cur.fetchone() is not None
    con.close()
    return existe

def has_data_for_term(search_term):
    """Checa se já há dados salvos para um termo de busca específico."""
    con = sqlite3.connect(DB_VAGAS_NOME)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM vagas_encontradas WHERE busca_termo = ?", (search_term,))
    count = cur.fetchone()[0]
    con.close()
    return count > 0

def fetch_clients():
    """Lê os termos de busca (role) do clientes.db. Cria mock se o DB não existir."""
    try:
        con = sqlite3.connect(DB_CLIENTES_NOME)
        cur = con.cursor()
        cur.execute("SELECT id, name, role FROM cliente") 
        clients = cur.fetchall()
        con.close()
        return clients
    except sqlite3.OperationalError:
        logging.warning(f" [DB CLIENTES] '{DB_CLIENTES_NOME}' não encontrado. Criando mock inicial.")
        
        try:
             con = sqlite3.connect(DB_CLIENTES_NOME)
             cur = con.cursor()
             cur.execute("CREATE TABLE IF NOT EXISTS cliente (id INTEGER PRIMARY KEY, name TEXT, role TEXT)")
             cur.execute("INSERT OR REPLACE INTO cliente (id, name, role) VALUES (?, ?, ?)", (1, "Renzo Tavares", "Teste")) 
             cur.execute("INSERT OR REPLACE INTO cliente (id, name, role) VALUES (?, ?, ?)", (2, "Teste Volume", "Recepcionista")) 
             con.commit()
             con.close()
             return fetch_clients() 
        except Exception as create_e:
             logging.error(f"Falha ao criar DB mock: {create_e}")
             return []

def extract_infojobs_id(link):
    """Extrai o ID numérico da vaga do final da URL Infojobs."""
    match = re.search(r'__(\d+)\.aspx', link) 
    if match: return match.group(1).strip()
    return 'N/A'

def realizar_tentativa_resgate(soup, tipo_tentativa):
    """Executa a coleta de links com a lógica resiliente."""
    links_encontrados = []
    
    if tipo_tentativa == 1:
        elementos = soup.find_all('div', attrs={'data-id': True})
        for elem in elementos:
            link_tag = elem.find('a', class_='text-decoration-none', href=True)
            if link_tag: links_encontrados.append(urljoin(URL_BASE, link_tag['href']))
    elif tipo_tentativa == 2:
        elementos = soup.find_all('a', class_='text-decoration-none', href=True)
        for link_tag in elementos:
            if '/vaga-de-' in link_tag['href']: links_encontrados.append(urljoin(URL_BASE, link_tag['href']))
    elif tipo_tentativa == 3:
        elementos = soup.find_all('h2', class_='h3 font-weight-bold text-body mb-2')
        for h2_tag in elementos:
            link_tag = h2_tag.find_parent('a', href=True)
            if link_tag and '/vaga-de-' in link_tag['href']: links_encontrados.append(urljoin(URL_BASE, link_tag['href']))

    return list(set(links_encontrados)) 

def fetch_page_infojobs(url, headers, retries=MAX_RETRIES):
    """Tenta acessar a página do Infojobs com loop de retry em caso de falha de rede/conexão."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status() 
            return response.text
        except requests.RequestException as e:
            logging.warning(f" [RETRY] Falha de conexão na tentativa {attempt + 1} para {url}: {str(e)}")
        
        if attempt < retries - 1:
             time.sleep(RETRY_DELAY)
             
    logging.error(f"❌ Falha crítica de rede/servidor após {retries} tentativas para {url}.")
    return None


def coletar_links_por_termo(search_term):
    """Busca links no Infojobs para um termo, usando lógica resiliente com retry."""
    
    encoded_term = quote_plus(search_term) 
    url_busca = URL_TEMPLATE + encoded_term
    
    time.sleep(RETRY_DELAY) 
    
    simple_search_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'Referer': 'https://www.google.com/',
        'Accept-Language': 'pt-BR,pt;q=0.9',
    }
    
    html_content = fetch_page_infojobs(url_busca, simple_search_headers, retries=MAX_RETRIES) 
    
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    links_finais = []
    for i in range(1, 4):
        links_tentativa = realizar_tentativa_resgate(soup, i)
        if len(links_tentativa) > len(links_finais):
            links_finais = links_tentativa
    
    return links_finais

def extrair_dados_vaga_em_tempo_real(url, is_initial_run=False):
    """
    Acessa a URL da vaga e extrai todos os detalhes. 
    A descrição é ignorada na primeira execução (is_initial_run=True).
    """
    vaga_data = {
        'url': url, 'titulo': 'N/A', 'empresa': 'N/A', 'localizacao': 'N/A',
        'salario': 'N/A', 'modalidade': 'N/A', 'descricao_completa': 'N/A', 'exigencias': 'N/A',
    }
    
    time.sleep(1.5) 
    
    simple_detail_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
    }
    
    html_content = fetch_page_infojobs(url, simple_detail_headers, retries=MAX_RETRIES) 

    if not html_content:
        return vaga_data

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        card_principal = soup.find('div', class_='card card-shadow px-32 py-20')
        
        if not card_principal: return vaga_data

        titulo_tag = card_principal.find('h2', class_='js_vacancyHeaderTitle')
        vaga_data['titulo'] = titulo_tag.text.strip() if titulo_tag else 'N/A'
        
        empresa_tag = card_principal.find('div', class_='h4')
        if empresa_tag: vaga_data['empresa'] = ' '.join(empresa_tag.text.split()).strip()
        
        local_sal_tags = card_principal.find_all('div', class_='text-medium mb-4')
        if local_sal_tags and len(local_sal_tags) > 0:
            vaga_data['localizacao'] = local_sal_tags[0].text.strip().split(',')[0]
        if local_sal_tags and len(local_sal_tags) > 1:
            vaga_data['salario'] = ' '.join(local_sal_tags[1].text.split())

        modalidade_tag = card_principal.find('div', class_='text-medium small font-weight-bold mb-4')
        if modalidade_tag: vaga_data['modalidade'] = modalidade_tag.text.strip().split('\n')[-1].strip()

        if not is_initial_run: 
            painel_detalhe = card_principal.find('div', class_='pt-24 text-medium js_vacancyDataPanels js_applyVacancyHidden')
            
            if painel_detalhe:
                descricao_p = painel_detalhe.find('p', class_='mb-16 text-break white-space-pre-line')
                if descricao_p: vaga_data['descricao_completa'] = ' '.join(descricao_p.text.split()) 

                exigencias_ul = painel_detalhe.find('ul', class_='custom-list')
                if exigencias_ul:
                    vaga_data['exigencias'] = ' | '.join([li.text.strip() for li in exigencias_ul.find_all('li')])
        
    except Exception as e:
        logging.error(f" [PARSING ERROR] Falha ao processar HTML da vaga: {url}. Erro: {e}")
        
    return vaga_data

def run_scraper_cycle(search_term):
    """Executa um ciclo completo de extração, salvamento e análise condicional."""
    
    is_initial_run = not has_data_for_term(search_term)
    
    global gemini_client 
    
    if is_initial_run:
        logging.info(f" [Modo] População Inicial (IA Off, Limite={VAGAS_LIMITE_POPULACAO})")
    else:
        logging.info(" [Modo] Monitoramento Contínuo (IA On para novas vagas)")

    logging.info(" Consultando links de vagas...")
    vaga_links = coletar_links_por_termo(search_term)
    logging.info(f" Encontrados {len(vaga_links)} links no total.")

    if is_initial_run and len(vaga_links) > 0:
        limpa_vagas_por_termo(search_term) 
    
    vagas_processadas_count = 0

    links_a_processar = vaga_links[:VAGAS_LIMITE_POPULACAO] if is_initial_run else vaga_links
    
    for link in links_a_processar:
        vagas_id = extract_infojobs_id(link)
        
        if vagas_id == 'N/A': continue

        if not is_initial_run and verifica_vaga_existe(vagas_id, search_term):
            logging.info(f"⛔ Vaga ID {vagas_id} já existe no DB para '{search_term}'. Parada imediata.")
            return 

        details = extrair_dados_vaga_em_tempo_real(link, is_initial_run)
        
        if is_initial_run:
            resumo_ia = "[Análise de IA ignorada no modo População Inicial]"
            logging.info(f" [VAGA] ID: {vagas_id}. População inicial. Salvando no DB.")
        else:
            logging.info(f" [VAGA NOVA] ID: {vagas_id}. IA: ON. Enviando para análise...")
            resumo_ia = analisa_vaga_com_ia(gemini_client, details.get('descricao_completa', ''))
            
            logging.info(f" [VAGA NOVA] ID: {vagas_id}. IA: ON. Enviando para análise...")
            resumo_ia = analisa_vaga_com_ia(gemini_client, details.get('descricao_completa', ''))
            
            resumo_html = build_resumo_html(resumo_ia)

            escaped_search_term = safe_escape(search_term).replace('\n', ' ')
            escaped_titulo = safe_escape(details.get('titulo', '')).replace('\n', ' ')
            escaped_empresa = safe_escape(details.get('empresa', '')).replace('\n', ' ')
            escaped_localizacao = safe_escape(details.get('localizacao', '')).replace('\n', ' ')
            escaped_modalidade = safe_escape(details.get('modalidade', '')).replace('\n', ' ')
            escaped_salario = safe_escape(details.get('salario', '')).replace('\n', ' ')

            safe_link = html.escape(link, quote=True)

            message = (
                f"<b>🚨 ALERTA: NOVA VAGA ENCONTRADA (INFOJOBS)! 🚨</b>\n"
                f"<b>Busca:</b> {escaped_search_term}\n"
                f"<b>Vaga:</b> <a href=\"{safe_link}\">{escaped_titulo}</a>\n"
                f"<b>Empresa:</b> {escaped_empresa}\n"
                f"<b>Localização/Modalidade:</b> {escaped_localizacao} ({escaped_modalidade})\n"
                f"<b>Salário:</b> {escaped_salario}\n\n"
                f"<b>Resumo da IA:</b>\n{resumo_html}"
            )

            send_telegram_message(message)

            print(f"\n🚨🚨 **NOVA VAGA ENCONTRADA [{search_term}]:** {details['titulo']} 🚨🚨")
            print(f"| Link: {link}")
            print(f"| Localização: {details.get('localizacao')}")
            print(f"| --- RESUMO DA IA ---")
            print(resumo_ia)
            print(f"--------------------------------------------------")
            
        registro_db = (
            vagas_id,
            search_term,
            details['titulo'].strip(),
            details['empresa'].strip(),
            details['localizacao'].strip(),
            details['salario'].strip(),
            details['modalidade'].strip(),
            link.strip(),
            resumo_ia
        )
        salva_vaga_no_db(registro_db)
        vagas_processadas_count += 1
        
        if is_initial_run and vagas_processadas_count >= VAGAS_LIMITE_POPULACAO:
            logging.info(f"✅ População Inicial concluída para '{search_term}'. {vagas_processadas_count} vagas salvas.")
            return 

    if not is_initial_run and vagas_processadas_count > 0:
        logging.info(f"✅ Ciclo de monitoramento concluído. {vagas_processadas_count} novas vagas processadas.")
    elif vagas_processadas_count == 0:
        logging.info(f"Nenhuma nova vaga encontrada neste ciclo para '{search_term}'.")


if __name__ == "__main__":
    inicializa_db_vagas() 
    
    gemini_client = None
    if API_KEY:
         try:
             gemini_client = Client(api_key=API_KEY)
             logging.info(" [IA] Cliente Gemini inicializado com sucesso.")
         except Exception as e:
             logging.error(f" [IA] Falha ao inicializar o cliente Gemini: {e}. Usando Mock.")

    while True:
        try:
            logging.info(f"\n##################################################") 
            logging.info(f"## INICIANDO CICLO DE MONITORAMENTO DE CLIENTES ##") 
            logging.info(f"##################################################") 
            
            clients = fetch_clients() 
            
            for client_id, client_name, search_term in clients:
                print("\nCRITÉRIO QA: Pausa de 5 segundos antes de iniciar a busca para evitar bloqueio.")
                time.sleep(5)
                
                logging.info(f"\n[CLIENTE: {client_name}] Buscando por: **{search_term}**")
                run_scraper_cycle(search_term) 
                time.sleep(RETRY_DELAY) 
            
            logging.info(f"\n💤 Todos os clientes processados. Sistema em espera por {TEMPO_ESPERA} segundos...") 
            time.sleep(TEMPO_ESPERA)
            
        except KeyboardInterrupt:
            logging.info("Execução interrompida pelo usuário.")
            break
        except Exception as e:
            logging.critical(f"Falha crítica no loop principal: {e}. Reiniciando em {TEMPO_ESPERA} segundos.")
            time.sleep(TEMPO_ESPERA)