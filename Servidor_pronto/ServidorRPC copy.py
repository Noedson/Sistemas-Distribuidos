import uuid
import rpyc
import os
import bcrypt
import mariadb
import logging
from threading import Lock, Thread
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class MyService(rpyc.Service):
    def __init__(self):
        logger.info('entrando na pasta do servidor')
        self.servidor_dir = os.path.dirname(os.path.abspath(__file__))
        self.upload_dir = os.path.join(self.servidor_dir, "uploads")
        self.conteudo_do_cliente = {}
        self.lock = Lock()
        self.chamada_de_retorno = {}
    
        if not os.path.exists(self.upload_dir):
            logger.info("criando a pasta de uploads nas pasta em que o servidor está")
            os.makedirs(self.upload_dir)

        self.configurar_db = {
            'host':os.getenv('DB_HOST', 'localhost'),
            'port':int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'root'),
            'database': os.getenv('DB_DATABASE', 'testerpc')

        }
        self.criar_db()
        
    def criar_db(self):
        try:
            logger.info("Conectando com o DB")
            conn = mariadb.connect(
                host = self.configurar_db['host'],
                port = self.configurar_db['port'],
                user = self.configurar_db['user'],
                password = self.configurar_db['password'],
                database = self.configurar_db['database'],
            )
            logger.info("criando as tabelas, caso não exista")
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
                        validde DATETIME DEFAULT NULL,
                        interesse_ativo BOOL,
                        UNIQUE(usuario_id, nome_do_arquivo),
                        FOREIGN KEY (usuario_id) REFERENCES usuario(usuario)
                        )
                        ''')
            conn.commit()
            logger.info("fim da criação")
        except mariadb.Error as e:
            print(f"Erro ao criar o db: {e}")
        finally:
            
            cursor.close()
            conn.close()
            logger.info("saindo da função criar_db")

    def on_connect(self, conn):
        if conn in self.conteudo_do_cliente:
            id_cliente = self.conteudo_do_cliente[conn]['id']
            logger.info(f"cliente {id_cliente} conectado")
    
    def on_disconnect(self, conn):
       if conn in self.conteudo_do_cliente:
            id_cliente = self.conteudo_do_cliente[conn]['id']
            del self.conteudo_do_cliente[conn]
            logger.info(f"cliente {id_cliente} desconectado")

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
                    logger.info(f"O Usuario {usuario}, Logou")
                    return f"Login Sucedido, Seu ID é {id_cliente}"
                else:
                    logger.info("O Usuario não conseguiu entrar")
                    return "Credenciais Invalidas"
            else:
                logger.info("Usuario não encontrado")
                return "Usuario não Encontrado"
        except mariadb.Error as e:
            logger.info(f"Erro ao consultar o Banco de Dados: {e}")
            return f"Erro ao consultar o Banco de Dados: {e}"
        finally:
            cursor.close()
            conn.close()
            logger.info("saindo da função de Login")
        
    def exposed_registrar(self, usuario, senha):
        logger.info("Entrando na função de registro!")
        
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
            logger.info("Usuario Registrado")
            return "Usuario Registrado"
        except mariadb.Error as e:
            if "Duplicate entry" in str(e):
                logger.info("Usuario já tem cadastro no DB")
                return "Usuario Existente"
        finally:
            cursor.close()
            conn.close()
            logger.info("Usuario já tem cadastro no DB")

    def exposed_upload(self, nome_do_arquivo, conteudo_do_arquivo):
               
        caminho_do_arquivo = os.path.join(self.upload_dir, nome_do_arquivo)
        try:
            with open(caminho_do_arquivo, 'wb') as f:
                f.write(conteudo_do_arquivo)
            logger.info(f"O arquivo {nome_do_arquivo}, salvo no servidor")
            return f"O arquivo {nome_do_arquivo}, salvo com sucesso"
        except Exception as e:
            return f"Erro o Salvar o Arquivo: {str(e)}"
    
    def exposed_download(self, nome_do_arquivo):
        caminho_do_arquivo = os.path.join(self.upload_dir, nome_do_arquivo)

        if not os.path.exists(caminho_do_arquivo):
            return f"{nome_do_arquivo}, não encontrado"
        
        with open(caminho_do_arquivo, 'rb') as f:
            conteudo_do_arquivo = f.read()
        
        logger.info(f"O Usuario {self.conteudo_do_cliente}, solicitou o arquivo {nome_do_arquivo}")
        return conteudo_do_arquivo

    def exposed_consulta(self):
        arquivos = os.listdir(self.upload_dir)
        if not arquivos:
            return "Não há Arquivos no Servidor"
        logger.info(f"O Usuario {self.conteudo_do_cliente}, consultou os arquivos do servidor")
        return arquivos
        
    def exposed_registrar_interesse(self, usuario, nome_do_arquivo, days = 0, hours = 0, minutes = 0, seconds = 0):
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
                agora = datetime.now()
                validade = agora + timedelta(days=days) +timedelta(hours=hours) + timedelta(minutes= minutes) + timedelta(seconds=seconds)
                if cursor.fetchone(): 
                    cursor.execute('UPDATE interesse SET validade = %s, interesse_ativo = %s  WHERE usuario_id = %d', (validade, True,usuario))
                    conn.commit()
                    logger.info(f"o usuario {usuario} atualizou o seu interesse pelo arquivo {nome_do_arquivo}")
                    return "Interesse Atualizado"
                else:
                    cursor.execute('INSERT INTO interesse (usuario_id, nome_do_arquivo, data_registro, validade, interesse_ativo) VALUES (%s, %s, %s, %s, %b)', (usuario, nome_do_arquivo, agora, validade, True))
                    conn.commit()
                    logger.info(f"o usuario {usuario} registrou {nome_do_arquivo}")
                    return "Interesse Registrado"
            except mariadb.Error as e:
                logger.info(f"Erro ao registrar interesse: {e}")
                return f"Erro ao registrar interesse: {e}"
            finally:
                cursor.close()
                conn.close()
                logger.info("saindo da função registrar_interesse")
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
                cursor.execute('UPDATE interesse SET interesse_ativo = %s  WHERE usuario_id = %s AND nome_do_arquivo = %s' , (False ,usuario, nome_do_arquivo))
                # cursor.execute('DELETE FROM interesse WHERE usuario_id = %s AND nome_do_arquivo = %s', (usuario, nome_do_arquivo))
                logger.info(f"O Usuario {usuario} cancelou o interesse pelo arquivo {nome_do_arquivo}")
                conn.commit()
                return "Interesse Cancelado"
            else:
                logger.info(f"O Usuario {usuario} não registrou o interesse pelo arquivo {nome_do_arquivo}")
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
                'password': os.getenv('DB_PASSWORD', 'root'),
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
                            tempo_de_validade = interessado[3]
                            valido = interessado[4]
                            if tempo_de_validade > datetime.now():
                                if valido:
                                    mensagem = f"{self.nome_do_arquivo} está disponível"
                                    print("Notificando")
                                    self.chamada_de_retorno(mensagem)
                                else:
                                    mensagem = f"interesse pelo arquivo: {self.nome_do_arquivo}, cancelado pelo usuario"
                                    print("Notificando")
                                    self.chamada_de_retorno(mensagem)
                            else:
                                cursor.execute('UPDATE interesse SET interesse_ativo = %s  WHERE usuario_id = %s AND nome_do_arquivo = %s' , (False ,self.usuario, self.nome_do_arquivo))
                                conn.commit()
                                print(f"Interesse expirado para o usuário {self.usuario} e arquivo {self.nome_do_arquivo}")
                                mensagem = f"Interesse expirado"
                                self.chamada_de_retorno(mensagem)
                except mariadb.Error as e:
                    print(f"erro ao Consultar o DB: {e}")
                finally:
                    cursor.close()
                    conn.close()
    
if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    print("Servidor Iniciado")
    ThreadedServer(MyService, port = 12345).start()
    
