from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from flask import Flask, request, render_template
from flask_socketio import SocketIO

import base64
import threading
import time
import os
import re
import shutil
from pathlib import Path
from PyPDF2 import PdfReader
import queue
import uuid


from gevent import monkey
monkey.patch_all()


# ================= CONFIG =================

NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "3"))

ARQUIVO_ENTRADA = "patrimonios.txt"
ARQUIVO_SAIDA = "resultado.txt"

LOGIN_USUARIO = "LOGIN"
LOGIN_SENHA = "SENHA"

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret"
socketio = SocketIO(app, cors_allowed_origins="*")

eventos_2fa = {}   # sid -> threading.Event
codigos_2fa = {} 
status_tarefas = {}              
lock_status_tarefas = threading.Lock()
tarefas = queue.Queue()


# ================= LOG / EMISSÃO PARA O FRONT =================

def emit_log(sid, mensagem, tipo="info"):
    """
    Substitui o antigo enviar_telegram(). Empurra uma mensagem de log/progresso
    para o cliente específico (sid) via WebSocket.
    """
    print(mensagem)
    if sid:
        socketio.emit("log", {"tipo": tipo, "mensagem": mensagem}, room=sid)

def worker():
    """Fica consumindo a fila 'tarefas' e executa cada uma via executar_com_login."""
    while True:
        item = tarefas.get()
        id_tarefa = item.get("id_tarefa")
        if id_tarefa:
            atualizar_status_tarefa(id_tarefa, status="executando")
        try:
            executar_com_login(**item)
        except Exception as e:
            print(f"Erro inesperado no worker: {e}")
        finally:
            tarefas.task_done()


def emit_resultado(sid, evento, dados):
    """Emite o resultado final de uma ação para o cliente específico."""
    if sid:
        socketio.emit(evento, dados, room=sid)

def emit_pdf(sid, nome_arquivo, caminho_arquivo):
    """Lê o PDF do disco e envia o conteúdo em base64 para o cliente poder baixar."""
    if not sid:
        return
    try:
        with open(caminho_arquivo, "rb") as f:
            conteudo = base64.b64encode(f.read()).decode("utf-8")
        socketio.emit("pdf_disponivel", {
            "nome_arquivo": nome_arquivo,
            "conteudo_base64": conteudo
        }, room=sid)
    except Exception as e:
        emit_log(sid, f"Erro ao preparar PDF para download: {e}", tipo="erro")

def _rotulo_objeto(item):
    """Extrai um texto amigável (código do bem) para exibir na fila, se existir."""
    args = item.get("args", ())
    return args[0] if args else None


def atualizar_status_tarefa(id_tarefa, **campos):
    with lock_status_tarefas:
        if id_tarefa not in status_tarefas:
            return
        status_tarefas[id_tarefa].update(campos)
        lista_atual = list(status_tarefas.values())

    socketio.emit("fila_atualizada", {"tarefas": lista_atual})


def registrar_tarefa_na_fila(item):
    """Cria a entrada de status e publica no broadcast antes de colocar na fila de execução."""
    id_tarefa = str(uuid.uuid4())
    item["id_tarefa"] = id_tarefa

    with lock_status_tarefas:
        status_tarefas[id_tarefa] = {
            "id": id_tarefa,
            "tipo": item["evento_resposta"],
            "objeto": _rotulo_objeto(item),
            "status": "aguardando",
        }
        lista_atual = list(status_tarefas.values())

    socketio.emit("fila_atualizada", {"tarefas": lista_atual})
    tarefas.put(item)


def remover_tarefa_apos_delay(id_tarefa, delay=15):
    """Remove a tarefa concluída/erro da lista exibida após alguns segundos."""
    def _remover():
        time.sleep(delay)
        with lock_status_tarefas:
            status_tarefas.pop(id_tarefa, None)
            lista_atual = list(status_tarefas.values())
        socketio.emit("fila_atualizada", {"tarefas": lista_atual})

    socketio.start_background_task(_remover)


# ================= UTILIDADES DE PDF =================

def renomear_e_mover_pdf(chamado_numero, sid=None):
    downloads = Path.home() / "Downloads"

    pdfs = list(downloads.glob("*.pdf"))
    if not pdfs:
        raise Exception("Nenhum PDF encontrado na pasta Downloads.")

    pdf_mais_recente = max(pdfs, key=os.path.getmtime)
    print(f"PDF encontrado: {pdf_mais_recente.name}")

    reader = PdfReader(pdf_mais_recente)
    texto_completo = ""
    for pagina in reader.pages:
        texto_completo += pagina.extract_text() + "\n"

    padrao_grp = r"Número\s*-\s*(\d+)"
    match = re.search(padrao_grp, texto_completo)
    if not match:
        raise Exception("Número da GRP não encontrado no PDF.")

    numero_grp = match.group(1)
    print(f"GRP encontrada: {numero_grp}")

    novo_nome = f"{chamado_numero}_{numero_grp}.pdf"

    emit_pdf(sid, novo_nome, pdf_mais_recente)  # entrega via download no navegador

    pdf_mais_recente.unlink()  # remove da pasta Downloads do host
    print(f"PDF entregue como: {novo_nome}")

    return novo_nome


def renomear_e_mover_pdf_em_massa(chamado_numero, sid=None):
    downloads = Path.home() / "Downloads"

    pdfs = list(downloads.glob("*.pdf"))
    if not pdfs:
        raise Exception("Nenhum PDF encontrado na pasta Downloads.")

    pdf_mais_recente = max(pdfs, key=os.path.getmtime)
    print(f"PDF encontrado: {pdf_mais_recente.name}")

    reader = PdfReader(pdf_mais_recente)
    texto_completo = ""
    for pagina in reader.pages:
        texto_completo += pagina.extract_text() + "\n"

    padrao_grp = r"Número\s*-\s*(\d+)"
    match = re.search(padrao_grp, texto_completo)
    if not match:
        raise Exception("Número da GRP não encontrado no PDF.")

    numero_grp = match.group(1)
    print(f"GRP encontrada: {numero_grp}")

    novo_nome = f"{chamado_numero}_{numero_grp}.pdf"

    emit_pdf(sid, novo_nome, pdf_mais_recente)  # entrega via download no navegador

    pdf_mais_recente.unlink()  # remove da pasta Downloads do host
    emit_log(sid, "GRP disponibilizado para download.")
    print(f"PDF entregue como: {novo_nome}")

    return novo_nome

# ================= SELENIUM: OPÇÕES =================

def novo_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 25)
    return driver, wait


# ================= LOGIN (executado a cada ação, não é global) =================

def verificar_e_tratar_2fa(driver, wait, sid=None, timeout=120):
    try:
        botao_opcao = driver.find_element(
            By.XPATH, "//*[@id=\"formOpcao2fa:selecionaOpcao2fa\"]"
        )
        if not botao_opcao.is_displayed():
            return False
    except Exception:
        return False

    emit_log(sid, "Confirmação em duas etapas solicitada.", tipo="alerta")
    driver.execute_script("arguments[0].click();", botao_opcao)

    botao_opcao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//*[@id=\"formOpcao2fa:selecionaOpcao2fa\"]"
    )))
    driver.execute_script("arguments[0].click();", botao_opcao)

    # Prepara a espera pelo código vindo do front
    evento = threading.Event()
    eventos_2fa[sid] = evento
    codigos_2fa[sid] = None

    emit_resultado(sid, "solicitar_2fa", {})

    recebido = evento.wait(timeout=timeout)
    eventos_2fa.pop(sid, None)
    codigo = codigos_2fa.pop(sid, None)

    if not recebido or not codigo:
        emit_log(sid, "Tempo esgotado aguardando o código de 2FA.", tipo="erro")
        raise Exception("Código de 2FA não informado a tempo.")

    if len(codigo) != 6 or not codigo.isdigit():
        raise Exception("Código de 2FA inválido.")

    for i, digito in enumerate(codigo, start=1):
        campo = wait.until(EC.presence_of_element_located((
            By.ID, f"formOpcao2fa:dfaValor{i}"
        )))
        campo.clear()
        campo.send_keys(digito)

    time.sleep(0.5)

    # Ajuste aqui se o site não enviar automaticamente após o 6º dígito
    try:
        driver.find_element(By.ID, "formOpcao2fa:dfaValor6").send_keys(Keys.ENTER)
    except Exception:
        pass

    emit_log(sid, "Código de 2FA enviado.")
    time.sleep(2)
    return True

def abrir(driver, wait, sid=None, usuario=None, senha=None):
    driver.get("https://tjse.thema.inf.br/grp/home.faces")

    wait.until(EC.presence_of_element_located((By.ID, "loginForm:usuario")))

    driver.find_element(By.ID, "loginForm:usuario").send_keys(usuario or LOGIN_USUARIO)
    driver.find_element(By.ID, "loginForm:senha").send_keys(senha or LOGIN_SENHA)

    botao = wait.until(EC.element_to_be_clickable((By.ID, "loginForm:login")))
    driver.execute_script("arguments[0].click();", botao)

    time.sleep(2)
    verificar_e_tratar_2fa(driver, wait, sid=sid)

    botao = wait.until(
        EC.element_to_be_clickable((By.ID, "formAdministracao:administracao_1"))
    )
    botao.click()

    emit_log(sid, "Login OK")


# ================= AÇÕES DE NEGÓCIO =================

def consulta(objeto, driver, wait, sid=None, retornar_local=False):
    botao = wait.until(
        EC.element_to_be_clickable((
            By.XPATH,
            "//span[text()='Consulta de Bens Móveis']/ancestor::a"
        ))
    )
    botao.click()
    time.sleep(1)
    driver.find_element(
        By.ID, "form_consultaBensMoveisM:codigoBem:fieldNumerico1:field"
    ).send_keys(objeto)
    driver.find_element(
        By.ID, "form_consultaBensMoveisM:codigoBem:fieldNumerico1:field"
    ).send_keys(Keys.ENTER)
    time.sleep(1)

    try:
        elemento = driver.find_element(
            By.XPATH, "//tr[contains(@id, 'dataTable:0')]//td[4]//a[@rel='detalhes']"
        )
        texto = elemento.text
        emit_log(sid, f"Objeto encontrado: {texto}")
    except Exception:
        emit_log(sid, "Esse objeto não existe.")
        botao = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//img[@title='Fechar todas as mensagens']"
        )))
        botao.click()
        texto = "inexistente"

    time.sleep(3)
    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//a[contains(@id,'botaoFecharJanela')]"
    )))
    botao.click()
    time.sleep(2)
    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//a[contains(@id,'botaoMenuModal')]"
    )))
    botao.click()
    time.sleep(2)
    img = wait.until(EC.presence_of_element_located((By.XPATH, "//img[@title='Voltar']")))
    ActionChains(driver).move_to_element(img).perform()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Voltar']]")))
    botao.click()

    if texto != "inexistente" and retornar_local:
        return texto.split(" ")[0]
    return texto


def consulta_em_massa(driver, wait, sid=None):
    with open(ARQUIVO_ENTRADA, "r", encoding="utf-8") as f:
        objetos = [linha.strip() for linha in f if linha.strip()]

    saida = open(ARQUIVO_SAIDA, "w", encoding="utf-8")

    botao = wait.until(EC.element_to_be_clickable((By.ID, "formModalMenu:j_id_9b_8")))
    botao.click()
    time.sleep(1)

    resultados = []

    for objeto in objetos:
        driver.find_element(
            By.ID, "form_consultaBensMoveisM:codigoBem:fieldNumerico1:field"
        ).send_keys(objeto)
        driver.find_element(
            By.ID, "form_consultaBensMoveisM:codigoBem:fieldNumerico1:field"
        ).send_keys(Keys.ENTER)
        time.sleep(1)
        try:
            elemento = driver.find_element(By.XPATH, "//a[@rel='detalhes']")
            texto = elemento.text
        except Exception:
            botao = wait.until(EC.element_to_be_clickable((
                By.XPATH, "//img[@title='Fechar todas as mensagens']"
            )))
            texto = "elemento não encontrado"
            botao.click()

        linha = f"{objeto} | {texto}"
        saida.write(linha + "\n")
        saida.flush()
        resultados.append(linha)
        emit_log(sid, f"OK: {linha}")

        driver.find_element(
            By.ID, "form_consultaBensMoveisM:codigoBem:fieldNumerico1:field"
        ).clear()

    saida.close()
    emit_log(sid, "Consulta em massa finalizada. Arquivo salvo em resultado.txt")
    return resultados


def _extrair_pdf_transferencia(driver, wait, chamado, sid=None, em_massa=False):
    """Lógica comum de: abrir relatório em PDF, clicar em download, mover o arquivo."""
    img = wait.until(EC.presence_of_element_located((
        By.XPATH, "//img[@title='Termo de Transferência de Bens Selecionados']"
    )))
    xpath_pdf = (
        "//div[contains(@class, 'rf-ddm-itm') and contains(@class, 'linkRelatorio')"
        " and .//span[text()='PDF']]"
    )
    try:
        impressora = wait.until(EC.presence_of_element_located((
            By.XPATH, "//*[contains(@id, '3z_1_2_1_2_3_label')]"
        )))
        ActionChains(driver).move_to_element(impressora).perform()
        time.sleep(1)

        pdf_item = wait.until(EC.presence_of_element_located((By.XPATH, xpath_pdf)))
        driver.execute_script("arguments[0].click();", pdf_item)
        emit_log(sid, "Relatório em PDF solicitado.")
    except Exception as e:
        emit_log(sid, f"Erro ao solicitar PDF: {e}", tipo="erro")

    time.sleep(20)

    wait_local = WebDriverWait(driver, 30)
    wait_local.until(EC.presence_of_element_located(("tag name", "apryse-webviewer")))

    driver.execute_script("""
    const viewer = document.querySelector("apryse-webviewer");
    const shadow = viewer.shadowRoot;
    const btn = shadow.querySelector('[data-element="downloadButton"]');
    if (btn) {
        btn.scrollIntoView({behavior: "smooth", block: "center"});
        btn.click();
    }
    """)

    time.sleep(3)
    if em_massa:
        caminho = renomear_e_mover_pdf_em_massa(chamado, sid=sid)
    else:
        caminho = renomear_e_mover_pdf(chamado, sid=sid)
        emit_log(sid, "GRP salvo na pasta correta.")

    return caminho


def _preencher_campo_autocomplete(driver, elemento_id, valor):
    """
    Preenche um campo de autocomplete RichFaces (campoComHint) de forma segura,
    limpando qualquer valor residual antes de digitar. Necessário porque .clear()
    nativo do Selenium nem sempre dispara os eventos JS que esses widgets escutam,
    o que pode deixar valor antigo concatenado com o novo.
    """
    campo = driver.find_element(By.ID, elemento_id)
    campo.click()
    campo.send_keys(Keys.CONTROL, "a")
    campo.send_keys(Keys.DELETE)
    campo.send_keys(valor)


def transferir(objeto, chamado, driver, wait, sid=None):
    """Fluxo original: transferência do objeto passando por duas rotas fixas."""
    try:
        origem = consulta(objeto, driver, wait, sid=sid, retornar_local=True)
    except Exception:
        emit_log(sid, "Objeto nem existe...", tipo="erro")
        raise

    emit_log(sid, "Iniciando transferência...")

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//span[text()='Transferência de Bens']/ancestor::a"
    )))
    botao.click()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@title='Criar']")))
    botao.click()
    time.sleep(1)

    _preencher_campo_autocomplete(
        driver, "form_transferenciaBemM:codigoLocalOrigem:field", origem
    )
    _preencher_campo_autocomplete(
        driver, "form_transferenciaBemM:codigoLocalDestino:field", "4128"
    )

    botao = wait.until(EC.element_to_be_clickable((By.ID, "form_transferenciaBemM:cmdl_salvar")))
    botao.click()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.ID, "form_transferenciaBemM:aba_670280:header:inactive"
    )))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Inserir bens' and @title='Inserir bens']")))
    botao.click()
    time.sleep(1)

    driver.find_element(
        By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
    ).send_keys(objeto)
    driver.find_element(
        By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
    ).send_keys(Keys.ENTER)
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.ID,
        "form_transferenciaBemM:dataTableModalInsereBens:0:divDataAquisicaoItemTransferenciaBem"
    )))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "//table[@id='form_transferenciaBemM:panelBotoes']//input[@value='Inserir']"
    )))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//img[@title='Fechar todas as mensagens']"
    )))
    botao.click()
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Encerrar']")))
    botao.click()
    time.sleep(1)

    wait.until(EC.alert_is_present())
    driver.switch_to.alert.accept()

    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()

    _extrair_pdf_transferencia(driver, wait, chamado, sid=sid)
    emit_log(sid, "GRP da transferência ao destino intermediário salvo na pasta correta.")

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "(//a[contains(@id, 'botaoFecharJanela')])[2]"
    )))
    driver.execute_script("arguments[0].click();", botao)
    time.sleep(3)

    # Segunda etapa da transferência (rota fixa 4128 -> 4126)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@title='Criar']")))
    botao.click()
    time.sleep(1)

    _preencher_campo_autocomplete(
        driver, "form_transferenciaBemM:codigoLocalOrigem:field", "4128"
    )
    _preencher_campo_autocomplete(
        driver, "form_transferenciaBemM:codigoLocalDestino:field", "4126"
    )

    botao = wait.until(EC.element_to_be_clickable((By.ID, "form_transferenciaBemM:cmdl_salvar")))
    botao.click()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.ID, "form_transferenciaBemM:aba_670280:header:inactive"
    )))
    botao.click()
    time.sleep(1)

    wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".carregandoFundo")))

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//input[@value='Inserir bens' and @title='Inserir bens']"
    )))
    botao.click()
    time.sleep(1)

    driver.find_element(
        By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
    ).send_keys(objeto)
    driver.find_element(
        By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
    ).send_keys(Keys.ENTER)
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.ID,
        "form_transferenciaBemM:dataTableModalInsereBens:0:divDataAquisicaoItemTransferenciaBem"
    )))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "//table[@id='form_transferenciaBemM:panelBotoes']//input[@value='Inserir']"
    )))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//img[@title='Fechar todas as mensagens']"
    )))
    botao.click()
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Encerrar']")))
    botao.click()
    time.sleep(1)

    wait.until(EC.alert_is_present())
    driver.switch_to.alert.accept()

    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()

    _extrair_pdf_transferencia(driver, wait, chamado, sid=sid)
    emit_log(sid, "GRP da transferência final salvo na pasta correta.")

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "(//a[contains(@id, 'botaoFecharJanela')])[2]"
    )))
    driver.execute_script("arguments[0].click();", botao)
    time.sleep(3)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//a[contains(@id, 'botaoFecharJanela')]"
    )))
    botao.click()
    time.sleep(2)
    botao = wait.until(EC.element_to_be_clickable((By.ID, "formMenuModal:botaoMenuModal")))
    botao.click()
    time.sleep(2)
    img = wait.until(EC.presence_of_element_located((By.XPATH, "//img[@title='Voltar']")))
    ActionChains(driver).move_to_element(img).perform()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Voltar']]")))
    botao.click()

    mensagem_final = (
        f"Transferência realizada com sucesso do {objeto} de {origem} "
        f"para a rota capital, sob o chamado {chamado}"
    )
    emit_log(sid, mensagem_final)
    return {"objeto": objeto, "origem": origem, "chamado": chamado, "mensagem": mensagem_final}


def transferir_recebimento(objeto, destino, chamado, driver, wait, sid=None):
    """Equivalente ao antigo transferiresp(): transferência objeto -> destino informado."""
    origem = consulta(objeto, driver, wait, sid=sid, retornar_local=True)

    if origem == "inexistente":
        emit_log(sid, "O Objeto não foi encontrado.", tipo="erro")
        return {"status": "erro", "mensagem": "objeto não encontrado"}

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//span[text()='Transferência de Bens']/ancestor::a"
    )))
    botao.click()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@title='Criar']")))
    botao.click()
    time.sleep(1)

    _preencher_campo_autocomplete(
        driver, "form_transferenciaBemM:codigoLocalOrigem:field", origem
    )

    try:
        _preencher_campo_autocomplete(
            driver, "form_transferenciaBemM:codigoLocalDestino:field", destino
        )
    except Exception:
        emit_log(sid, "Destino inválido", tipo="erro")
        time.sleep(3)
        botao = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//a[contains(@id, 'botaoFecharJanela')]"
        )))
        botao.click()
        time.sleep(2)
        botao = wait.until(EC.element_to_be_clickable((By.ID, "formMenuModal:botaoMenuModal")))
        botao.click()
        time.sleep(2)
        img = wait.until(EC.presence_of_element_located((By.XPATH, "//img[@title='Voltar']")))
        ActionChains(driver).move_to_element(img).perform()
        time.sleep(1)
        botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Voltar']]")))
        botao.click()
        return {"status": "erro", "mensagem": "destino inválido"}

    botao = wait.until(EC.element_to_be_clickable((By.ID, "form_transferenciaBemM:cmdl_salvar")))
    botao.click()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.ID, "form_transferenciaBemM:aba_670280:header:inactive"
    )))
    botao.click()
    time.sleep(1)

    #wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".carregandoFundo")))

    print('tentando clicar em inserir bens')
    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "//input[@value='Inserir bens' or @title='Inserir bens'] | "
        "//*[@id='form_transferenciaBemM:j_id_a7_d_2_so_38_ep']"
    )))
    botao.click()
    time.sleep(1)

    driver.find_element(
        By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
    ).send_keys(objeto)
    driver.find_element(
        By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
    ).send_keys(Keys.ENTER)
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.ID,
        "form_transferenciaBemM:dataTableModalInsereBens:0:divDataAquisicaoItemTransferenciaBem"
    )))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "//table[@id='form_transferenciaBemM:panelBotoes']//input[@value='Inserir']"
    )))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//img[@title='Fechar todas as mensagens']"
    )))
    botao.click()
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Encerrar']")))
    botao.click()
    time.sleep(1)

    wait.until(EC.alert_is_present())
    driver.switch_to.alert.accept()

    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()

    _extrair_pdf_transferencia(driver, wait, chamado, sid=sid)
    emit_log(sid, f"GRP da transferência a {destino} salvo na pasta correta.")

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "(//a[contains(@id, 'botaoFecharJanela')])[2]"
    )))
    driver.execute_script("arguments[0].click();", botao)
    time.sleep(3)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//a[contains(@id, 'botaoFecharJanela')]"
    )))
    botao.click()
    time.sleep(2)
    botao = wait.until(EC.element_to_be_clickable((By.ID, "formMenuModal:botaoMenuModal")))
    botao.click()
    time.sleep(2)
    img = wait.until(EC.presence_of_element_located((By.XPATH, "//img[@title='Voltar']")))
    ActionChains(driver).move_to_element(img).perform()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Voltar']]")))
    botao.click()

    mensagem_final = (
        f"Transferência realizada com sucesso do {objeto} de {origem} "
        f"para o {destino}, sob o chamado {chamado}"
    )
    emit_log(sid, mensagem_final)
    return {"status": "ok", "objeto": objeto, "origem": origem, "destino": destino,
            "chamado": chamado, "mensagem": mensagem_final}


def transferencia_em_massa(destino, chamado, driver, wait, sid=None):
    """Equivalente ao antigo tranferenciaEmMassa(): usa resultado_organizado.txt como entrada."""
    with open("resultado_organizado.txt", "r", encoding="utf-8") as f:
        linhas = f.readlines()
        primeira = linhas[0]
        origem = primeira.split(" | ")[1].split(" - ")[0]

    emit_log(sid, "Iniciando transferência em massa...")

    if origem == "inexistente":
        emit_log(sid, "O Objeto não foi encontrado.", tipo="erro")
        return {"status": "erro"}

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//span[text()='Transferência de Bens']/ancestor::a"
    )))
    botao.click()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@title='Criar']")))
    botao.click()
    time.sleep(1)

    driver.find_element(
        By.ID, "form_transferenciaBemM:codigoLocalOrigem:field"
    ).send_keys(origem)

    try:
        driver.find_element(
            By.ID, "form_transferenciaBemM:codigoLocalDestino:field"
        ).send_keys(destino)
    except Exception:
        emit_log(sid, "Destino inválido", tipo="erro")
        time.sleep(3)
        botao = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//a[contains(@id, 'botaoFecharJanela')]"
        )))
        botao.click()
        time.sleep(2)
        botao = wait.until(EC.element_to_be_clickable((By.ID, "formMenuModal:botaoMenuModal")))
        botao.click()
        time.sleep(2)
        img = wait.until(EC.presence_of_element_located((By.XPATH, "//img[@title='Voltar']")))
        ActionChains(driver).move_to_element(img).perform()
        time.sleep(1)
        botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Voltar']]")))
        botao.click()
        return {"status": "erro", "mensagem": "destino inválido"}

    botao = wait.until(EC.element_to_be_clickable((By.ID, "form_transferenciaBemM:cmdl_salvar")))
    botao.click()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()
    time.sleep(1)

    botao = wait.until(EC.element_to_be_clickable((
        By.ID, "form_transferenciaBemM:aba_670280:header:inactive"
    )))
    botao.click()
    time.sleep(1)

    qtd_patri = 0
    patrimonios_ruins = []

    for linha in linhas:
        objeto = linha.split(" | ")[0]
        emit_log(sid, f"Inserindo {objeto}")
        time.sleep(1)

        botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Inserir Bens']")))
        botao.click()
        time.sleep(1)

        driver.find_element(
            By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
        ).send_keys(objeto)
        driver.find_element(
            By.ID, "form_transferenciaBemM:campoCodigoBem:fieldNumerico1:field"
        ).send_keys(Keys.ENTER)
        time.sleep(1)

        try:
            botao = wait.until(EC.element_to_be_clickable((
                By.ID,
                "form_transferenciaBemM:dataTableModalInsereBens:0:divDataAquisicaoItemTransferenciaBem"
            )))
            botao.click()
            time.sleep(0.5)
            botao = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                "//div[contains(@id, 'msgModalInserirBensItemTransferencia')]//input[@value='Inserir']"
            )))
            botao.click()
            time.sleep(1)
            qtd_patri += 1
        except Exception:
            emit_log(sid, f"{objeto} não passou", tipo="alerta")
            patrimonios_ruins.append(objeto)
            try:
                botao = wait.until(EC.element_to_be_clickable((
                    By.XPATH, "//img[@title='Fechar todas as mensagens']"
                )))
                botao.click()
            except Exception:
                pass

        try:
            botao = wait.until(EC.element_to_be_clickable((
                By.XPATH, "//img[@title='Fechar todas as mensagens']"
            )))
            botao.click()
        except Exception:
            botao = wait.until(EC.element_to_be_clickable((
                By.ID, "form_transferenciaBemM:j_id_a7_d_2_so_38_ep"
            )))
            botao.click()
            time.sleep(1)

    if patrimonios_ruins:
        with open("Patrimonios_ruins.txt", "a", encoding="utf-8") as f:
            for p in patrimonios_ruins:
                f.write(p + "\n")

    if qtd_patri == 0:
        emit_log(sid, "Não teve patrimônios transferíveis")
        time.sleep(1)
        botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Excluir']")))
        botao.click()
        time.sleep(1)
        wait.until(EC.alert_is_present())
        driver.switch_to.alert.accept()
        time.sleep(1)
        botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
        botao.click()
        time.sleep(1)
        botao = wait.until(EC.element_to_be_clickable((By.ID, "j_id_a7_4:botaoFecharJanela")))
        botao.click()
        time.sleep(1)
        botao = wait.until(EC.element_to_be_clickable((By.ID, "formMenuModal:botaoMenuModal")))
        botao.click()
        time.sleep(2)
        img = wait.until(EC.presence_of_element_located((By.XPATH, "//img[@title='Voltar']")))
        ActionChains(driver).move_to_element(img).perform()
        time.sleep(1)
        botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Voltar']]")))
        botao.click()
        emit_log(sid, "Sem objetos transferíveis na transferência atual")
        return {"status": "vazio"}

    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Encerrar']")))
    botao.click()
    time.sleep(1)

    wait.until(EC.alert_is_present())
    driver.switch_to.alert.accept()

    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@title='Fechar Mensagem']")))
    botao.click()

    _extrair_pdf_transferencia(driver, wait, chamado, sid=sid, em_massa=True)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "(//a[contains(@id, 'botaoFecharJanela')])[2]"
    )))
    driver.execute_script("arguments[0].click();", botao)
    time.sleep(3)

    botao = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//a[contains(@id, 'botaoFecharJanela')]"
    )))
    botao.click()
    time.sleep(2)
    botao = wait.until(EC.element_to_be_clickable((By.ID, "formMenuModal:botaoMenuModal")))
    botao.click()
    time.sleep(2)
    img = wait.until(EC.presence_of_element_located((By.XPATH, "//img[@title='Voltar']")))
    ActionChains(driver).move_to_element(img).perform()
    time.sleep(1)
    botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[.//span[text()='Voltar']]")))
    botao.click()

    emit_log(sid, "Transferência em massa finalizada!")
    return {"status": "ok", "qtd_patrimonios": qtd_patri, "patrimonios_ruins": patrimonios_ruins}


# ================= EXECUÇÃO EM BACKGROUND (login por ação) =================

def executar_com_login(sid, evento_resposta, funcao_acao, args=(), id_tarefa=None,
                        usuario=None, senha=None, **kwargs):
    """
    Cria um driver novo, faz login e executa a ação; sempre fecha o driver ao final.
    Nenhuma ação reaproveita sessão/login de uma ação anterior.
    """
    driver, wait = novo_driver()
    try:
        abrir(driver, wait, sid=sid, usuario=usuario, senha=senha)
        resultado = funcao_acao(*args, driver=driver, wait=wait, sid=sid, **kwargs)
        emit_resultado(sid, evento_resposta, {"status": "ok", "resultado": resultado})
        if id_tarefa:
            atualizar_status_tarefa(id_tarefa, status="concluida")
    except Exception as e:
        emit_log(sid, f"Erro na ação: {e}", tipo="erro")
        emit_resultado(sid, evento_resposta, {"status": "erro", "mensagem": str(e)})
        if id_tarefa:
            atualizar_status_tarefa(id_tarefa, status="erro")
    finally:
        driver.quit()
        if id_tarefa:
            remover_tarefa_apos_delay(id_tarefa)


# ================= ROTAS HTTP =================

@app.route("/")
def index():
    return render_template("index.html")


# ================= EVENTOS SOCKET.IO =================

@socketio.on("connect")
def on_connect():
    emit_log(request.sid, "Conectado ao servidor.")

@socketio.on("enviar_2fa")
def handle_enviar_2fa(data):
    sid = request.sid
    codigo = (data or {}).get("codigo", "").strip()

    evento = eventos_2fa.get(sid)
    if not evento:
        emit_log(sid, "Nenhuma solicitação de 2FA pendente.", tipo="erro")
        return

    codigos_2fa[sid] = codigo
    evento.set()

@socketio.on("disconnect")
def on_disconnect():
    print(f"Cliente desconectado: {request.sid}")


@socketio.on("transferir")
def handle_transferir(data):
    sid = request.sid
    data = data or {}
    objeto = data.get("objeto")
    chamado = data.get("chamado")
    usuario = data.get("usuario")
    senha = data.get("senha")

    if not objeto or not chamado:
        emit_log(sid, "Payload inválido. Esperado: {objeto, chamado}", tipo="erro")
        return
    if not usuario or not senha:
        emit_log(sid, "Informe usuário e senha antes de executar a ação.", tipo="erro")
        return

    registrar_tarefa_na_fila({
        "sid": sid,
        "evento_resposta": "resultado_transferir",
        "funcao_acao": transferir,
        "args": (objeto, chamado),
        "usuario": usuario,
        "senha": senha,
    })


@socketio.on("transferir_recebimento")
def handle_transferir_recebimento(data):
    sid = request.sid
    data = data or {}
    objeto = data.get("objeto")
    destino = data.get("destino")
    chamado = data.get("chamado")
    usuario = data.get("usuario")
    senha = data.get("senha")

    if not objeto or not destino or not chamado:
        emit_log(sid, "Payload inválido. Esperado: {objeto, destino, chamado}", tipo="erro")
        return
    if not usuario or not senha:
        emit_log(sid, "Informe usuário e senha antes de executar a ação.", tipo="erro")
        return

    registrar_tarefa_na_fila({
        "sid": sid,
        "evento_resposta": "resultado_transferir_recebimento",
        "funcao_acao": transferir_recebimento,
        "args": (objeto, destino, chamado),
        "usuario": usuario,
        "senha": senha,
    })


@socketio.on("transferir_em_massa")
def handle_transferir_em_massa(data):
    sid = request.sid
    data = data or {}
    destino = data.get("destino")
    chamado = data.get("chamado")
    usuario = data.get("usuario")
    senha = data.get("senha")

    if not destino or not chamado:
        emit_log(sid, "Payload inválido. Esperado: {destino, chamado}", tipo="erro")
        return
    if not usuario or not senha:
        emit_log(sid, "Informe usuário e senha antes de executar a ação.", tipo="erro")
        return

    registrar_tarefa_na_fila({
        "sid": sid,
        "evento_resposta": "resultado_transferir_em_massa",
        "funcao_acao": transferencia_em_massa,
        "args": (destino, chamado),
        "usuario": usuario,
        "senha": senha,
    })


@socketio.on("consulta")
def handle_consulta(data):
    sid = request.sid
    data = data or {}
    objeto = data.get("objeto")
    usuario = data.get("usuario")
    senha = data.get("senha")

    if not objeto:
        emit_log(sid, "Payload inválido. Esperado: {objeto}", tipo="erro")
        return
    if not usuario or not senha:
        emit_log(sid, "Informe usuário e senha antes de executar a ação.", tipo="erro")
        return

    registrar_tarefa_na_fila({
        "sid": sid,
        "evento_resposta": "resultado_consulta",
        "funcao_acao": consulta,
        "args": (objeto,),
        "usuario": usuario,
        "senha": senha,
    })


@socketio.on("consulta_em_massa")
def handle_consulta_em_massa(_data):
    sid = request.sid
    registrar_tarefa_na_fila({
        "sid": sid,
        "evento_resposta": "resultado_consulta_em_massa",
        "funcao_acao": consulta_em_massa,
        "args": (),
        "usuario": None,
        "senha": None,
    })
    
socketio.start_background_task(worker)



# ================= START =================

if __name__ == "__main__":

    socketio.run(app, host="0.0.0.0", port=5000)
