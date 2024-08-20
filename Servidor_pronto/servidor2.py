import uuid
import rpyc
import os
import bcrypt
import mariadb
import tkinter as tk
from tkinter import messagebox, filedialog

from threading import Lock, Thread
from datetime import datetime

class MyService(rpyc.Service):
    def __init__(self):
        self.servidor_dir = os.path.dirname(os.path.abspath(__file__))
        self.upload_dir = os.path.join(self.servidor_dir, "uploads")
        self.conteudo_do_cliente = {}
        self.lock = Lock()
        self.chamada_de_retorno = {}

        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir)

        self.configurar_db = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'root'),
            'database': os.getenv('DB_DATABASE', 'testerpc')
        }

        self.criar_db()
    
    def criar_db(self):
        try:
            print("entrando na função para criar db")
            conn = mariadb.connect(
                host = self.configurar_db['host'],
                port = self.configurar_db['port'],
                user = self.configurar_db['user'],
                password = self.configurar_db['password'],
                database = self.configurar_db['database'],
            )
            print("saindo da configuração")
            cursor = conn.cursor()
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS usuario(
                            usuario VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci PRIMARY KEY,
                            senha VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL
                            )
                            ''')
            cursor.execute('''
                        CREATE TABLE IF NOT EXISTS interesse(
                        usuario_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                        nome_do_arquivo VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                        data_registro DATETIME,
                        UNIQUE(usuario_id, nome_do_arquivo),
                        FOREIGN KEY (usuario_id) REFERENCES usuario(usuario)
                        )
                        ''')
            conn.commit()
        except mariadb.Error as e:
            print(f"Erro ao criar o db: {e}")
        finally:
            cursor.close()
            conn.close()

    def on_connect(self, conn):
        print("Cliente Conectado")
        self.conteudo_do_cliente[conn] = {'id': None}

    def on_disconnect(self, conn):
        if conn in self.conteudo_do_cliente:
            id_cliente = self.conteudo_do_cliente[conn]['id']
        del self.conteudo_do_cliente[conn]
        print(f"cliente {id_cliente} desconectado")

    def exposed_login(self, usuario, senha):
        try:
            conn = mariadb.connect(
                host = self.configurar_db['host'],
                port = self.configurar_db['port'],
                user = self.configurar_db['user'],
                password = self.configurar_db['password'],
                database = self.configurar_db['database'],
            )
            cursor = conn.cursor()
            cursor.execute(
            'SELECT senha FROM usuario WHERE usuario = %s' , (usuario,)
            )
            resultado = cursor.fetchone()
            if resultado:
                senha_hash =  resultado[0].encode('utf-8')
                if bcrypt.checkpw(senha.encode('utf-8'), senha_hash):
                    id_cliente = str(uuid.uuid4())
                    with self.lock:
                        self.conteudo_do_cliente[conn] = {'id': id_cliente, 'usuario': usuario}
                    print(f"O Usuario {usuario}, Logou")
                    return f"Login Sucedido, Seu ID é {id_cliente}"
                else:
                    return "Credenciais Invalidas"
            else:
                return "Usuario não Encontrado"
        except mariadb.Error as e:
            return f"Erro ao consultar o Banco de Dados: {e}"
        finally:
            cursor.close()
            conn.close()
        
    def exposed_registrar(self, usuario, senha):
        print("Entrando na função de registro!")
        
        try:
            conn = mariadb.connect(
                host = self.configurar_db['host'],
                port = self.configurar_db['port'],
                user = self.configurar_db['user'],
                password = self.configurar_db['password'],
                database = self.configurar_db['database'],
            )
            cursor = conn.cursor()
            senha_hash = bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt())
            cursor.execute('INSERT INTO usuario (usuario, senha) VALUES (%s, %s)', (usuario, senha_hash))
            conn.commit()
            return "Usuario Registrado"
        except mariadb.Error as e:
            if "Duplicate entry" in str(e):
                return "Usuario Existente"
        finally:
            cursor.close()
            conn.close()

    def exposed_upload(self, nome_do_arquivo, conteudo_do_arquivo):
                
        caminho_do_arquivo = os.path.join(self.upload_dir, nome_do_arquivo)
        try:
            with open(caminho_do_arquivo, 'wb') as f:
                f.write(conteudo_do_arquivo)
            return f"O arquivo {nome_do_arquivo}, salvo com sucesso"
        except Exception as e:
            return f"Erro o Salvar o Arquivo: {str(e)}"

    def exposed_download(self, nome_do_arquivo):
        caminho_do_arquivo = os.path.join(self.upload_dir, nome_do_arquivo)

        if not os.path.exists(caminho_do_arquivo):
            return f"{nome_do_arquivo}, não encontrado"
        
        with open(caminho_do_arquivo, 'rb') as f:
            conteudo_do_arquivo = f.read()
        return conteudo_do_arquivo

    def exposed_consulta(self):
        arquivos = os.listdir(self.upload_dir)
        if not arquivos:
            return "Não há Arquivos no Servidor"
        return arquivos
        
    def exposed_registrar_interesse(self, usuario, nome_do_arquivo):
            try:
                conn = mariadb.connect(
                host = self.configurar_db['host'],
                port = self.configurar_db['port'],
                user = self.configurar_db['user'],
                password = self.configurar_db['password'],
                database = self.configurar_db['database'],
                )

                cursor = conn.cursor()
                cursor.execute('SELECT * FROM interesse WHERE usuario_id = %s AND nome_do_arquivo = %s', (usuario, nome_do_arquivo))
                if cursor.fetchone():
                    return "Você já registrou interesse por este arquivo"
                agora = datetime.now()
                cursor.execute('INSERT INTO interesse (usuario_id, nome_do_arquivo, data_registro) VALUES (%s, %s, %s)', (usuario, nome_do_arquivo, agora))
                conn.commit()
                return "Interesse Registrado"
            
            except mariadb.Error as e:
                return f"Erro ao registrar interesse: {e}"
            finally:
                cursor.close()
                conn.close()

    def exposed_cancelar_interesse(self, usuario, nome_do_arquivo):
        try:
            conn = mariadb.connect(
                host = self.configurar_db['host'],
                port = self.configurar_db['port'],
                user = self.configurar_db['user'],
                password = self.configurar_db ['password'],
                database = self.configurar_db['database'],
            )
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM interesse WHERE usuario_id = %s AND nome_do_arquivo = %s', (usuario, nome_do_arquivo))
            if cursor.fetchone():
                cursor.execute('DELETE FROM interesse WHERE usuario_id = %s AND nome_do_arquivo = %s', (usuario, nome_do_arquivo))
                conn.commit()
                return "Interesse Cancelado"
            else:
                return "Você não registrou interesse nesse arquivo"
        except mariadb.Error as e:
            return f"Erro ao consultar o Banco de Dados {e}"
        finally:
            cursor.close()
            conn.close()
            
    class exposed_Notificar_Usuario(object):
        def __init__(self, nome_do_arquivo, usuario, chamada_de_retorno):
            self.configurar_db = {
                'host':os.getenv('DB_HOST', 'localhost'),
                'port':int(os.getenv('DB_PORT', 3306)),
                'user': os.getenv('DB_USER', 'root'),
                'password': os.getenv('DB_PASSWORD', 'admin'),
                'database': os.getenv('DB_DATABASE', 'testerpc')
            }
            self.servidor_dir = os.path.dirname(os.path.abspath(__file__))
            self.upload_dir = os.path.join(self.servidor_dir, "uploads")
            self.usuario = usuario
            self.nome_do_arquivo = nome_do_arquivo
            self.chamada_de_retorno = rpyc.async_(chamada_de_retorno)
            self.thread = Thread(target = self.notificar_usuario)
            self.thread.start()

    def notificar_usuario(self):
            try:
                conn = mariadb.connect(
                    host = self.configurar_db['host'],
                    port = self.configurar_db['port'],
                    user = self.configurar_db['user'],
                    password = self.configurar_db ['password'],
                    database = self.configurar_db['database'],
                )
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM interesse WHERE usuario_id = %s AND nome_do_arquivo = %s', (self.usuario, self.nome_do_arquivo))
                interessados = cursor.fetchall()
                arquivos_no_servidor = os.listdir(self.upload_dir)
                if self.nome_do_arquivo in arquivos_no_servidor:
                    for interessado in interessados:
                        mensagem = f"{self.nome_do_arquivo} está disponível"
                        print("Notificando")
                        self.chamada_de_retorno(mensagem)
            except mariadb.Error as e:
                print(f"erro ao Consultar o DB: {e}")
            finally:
                cursor.close()
                conn.close()


class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Servidor de Arquivos RPyC")
        self.geometry("400x400")
        
        # Campos de entrada
        self.lbl_usuario = tk.Label(self, text="Usuário:")
        self.lbl_usuario.pack(pady=5)
        self.entry_usuario = tk.Entry(self)
        self.entry_usuario.pack(pady=5)

        self.lbl_senha = tk.Label(self, text="Senha:")
        self.lbl_senha.pack(pady=5)
        self.entry_senha = tk.Entry(self, show="*")
        self.entry_senha.pack(pady=5)

        # Botões
        self.btn_login = tk.Button(self, text="Login", command=self.login)
        self.btn_login.pack(pady=5)

        self.btn_registrar = tk.Button(self, text="Registrar", command=self.registrar)
        self.btn_registrar.pack(pady=5)

        self.btn_upload = tk.Button(self, text="Upload", command=self.upload)
        self.btn_upload.pack(pady=5)

        self.btn_download = tk.Button(self, text="Download", command=self.download)
        self.btn_download.pack(pady=5)

        self.btn_consultar = tk.Button(self, text="Consultar Arquivos", command=self.consultar)
        self.btn_consultar.pack(pady=5)

        self.btn_interesse = tk.Button(self, text="Registrar Interesse", command=self.registrar_interesse)
        self.btn_interesse.pack(pady=5)

        self.btn_cancelar_interesse = tk.Button(self, text="Cancelar Interesse", command=self.cancelar_interesse)
        self.btn_cancelar_interesse.pack(pady=5)

        # Área de mensagens
        self.msg_area = tk.Text(self, height=10)
        self.msg_area.pack(pady=5)
        
        # Inicialização do serviço RPyC
        self.my_service = MyService()
        self.server_thread = Thread(target=self.start_server)
        self.server_thread.start()

    def start_server(self):
        from rpyc.utils.server import ThreadedServer
        print("Servidor Iniciado")
        ThreadedServer(MyService, port=12345).start()

    def login(self):
        usuario = self.entry_usuario.get()
        senha = self.entry_senha.get()
        resultado = self.my_service.exposed_login(usuario, senha)
        self.show_message(resultado)

    def registrar(self):
        usuario = self.entry_usuario.get()
        senha = self.entry_senha.get()
        resultado = self.my_service.exposed_registrar(usuario, senha)
        self.show_message(resultado)

    def upload(self):
        file_path = filedialog.askopenfilename()
        if file_path:
            nome_do_arquivo = os.path.basename(file_path)
            with open(file_path, 'rb') as f:
                conteudo_do_arquivo = f.read()
            resultado = self.my_service.exposed_upload(nome_do_arquivo, conteudo_do_arquivo)
            self.show_message(resultado)

    def download(self):
        nome_do_arquivo = filedialog.asksaveasfilename()
        resultado = self.my_service.exposed_download(nome_do_arquivo)
        if isinstance(resultado, bytes):
            with open(nome_do_arquivo, 'wb') as f:
                f.write(resultado)
            self.show_message(f"Arquivo {nome_do_arquivo} baixado com sucesso")
        else:
            self.show_message(resultado)

    def consultar(self):
        resultado = self.my_service.exposed_consulta()
        self.show_message(resultado)

    def registrar_interesse(self):
        usuario = self.entry_usuario.get()
        nome_do_arquivo = filedialog.askopenfilename()
        nome_do_arquivo = os.path.basename(nome_do_arquivo)
        resultado = self.my_service.exposed_registrar_interesse(usuario, nome_do_arquivo)
        self.show_message(resultado)

    def cancelar_interesse(self):
        usuario = self.entry_usuario.get()
        nome_do_arquivo = filedialog.askopenfilename()
        nome_do_arquivo = os.path.basename(nome_do_arquivo)
        resultado = self.my_service.exposed_cancelar_interesse(usuario, nome_do_arquivo)
        self.show_message(resultado)

    def show_message(self, message):
        self.msg_area.delete(1.0, tk.END)
        self.msg_area.insert(tk.END, message)

if __name__ == "__main__":
    app = Application()
    app.mainloop()
