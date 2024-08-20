import rpyc
import os
import uuid

class MyClient:
    def __init__(self):
        self.conn = None
        self.username = None
        self.client_id = None

    def notificacao(self, mensagem):
        print(f"Mensagem do Servidor: {mensagem}")
    
    def connect(self, host='localhost', port=12345):
        self.conn = rpyc.connect(host, port)
        
        print("Conectado ao servidor")

    def login(self, username, password):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        response = self.conn.root.exposed_login(username, password)
        
        print(response)
        
        if "Seu ID é" in response:
            self.username = username
            self.client_id = response.split()[-1]  # Extrai o ID do retorno
            return response
        else:
            return response

    def registrar(self, username, password):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        response = self.conn.root.registrar(username, password)
        print(response)

    def upload(self, file_path):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        if not self.client_id:
            print("Você precisa estar autenticado para fazer upload.")
            return

        with open(file_path, 'rb') as f:
            file_content = f.read()
        
        # file_name = os.path.basename(file_path)
        response = self.conn.root.exposed_upload(os.path.basename(file_path), file_content)
        print(response)

    def download(self, file_name, save_path):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        if not self.client_id:
            print("Você precisa estar autenticado para fazer download.")
            return

        file_content = self.conn.root.exposed_download(file_name)
        if isinstance(file_content, str) and "não encontrado" in file_content:
            print(file_content)
        else:
            with open(save_path, 'wb') as f:
                f.write(file_content)
            print(f"Arquivo {file_name} salvo em {save_path}")

    def consulta(self):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        if not self.client_id:
            print("Você precisa estar autenticado para consultar arquivos.")
            return

        arquivos = self.conn.root.exposed_consulta()
        if isinstance(arquivos, str):
            print(arquivos)
        else:
            print("Arquivos disponíveis:")
            for arquivo in arquivos:
                print(arquivo)
    def registrar_interesse(self, nome_do_arquivo, days = 0, hours = 0, minutes = 0, seconds = 0):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        if not self.client_id:
            print("Você precisa estar autenticado para consultar arquivos.")
            return
        reposta = self.conn.root.registrar_interesse(self.username, nome_do_arquivo, days = days, hours = hours, minutes = minutes, seconds = seconds)
        print(reposta)
    
    def cancerlar_interesse(self, nome_do_arquivo):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        if not self.client_id:
            print("Você precisa estar autenticado para consultar arquivos.")
            return
        reposta = self.conn.root.cancelar_interesse(self.username, nome_do_arquivo)
        print(reposta)
    def arquivo_interessado_no_servidor(self, nome_do_arquivo):
        if not self.conn:
            print("Não conectado ao servidor")
            return
        
        if not self.client_id:
            print("Você precisa estar autenticado para registrar interesse.")
            return

        self.conn.root.exposed_Notificar_Usuario(nome_do_arquivo, self.username, self.notificacao)
        # print(resposta)

        

if __name__ == "__main__":
    cliente = MyClient()
    cliente.connect()
    
    # Teste de registro
    # cliente.registrar("user1", "password1")
    
    # Teste de login
    cliente.login("user1", "password1")
    
    cliente.arquivo_interessado_no_servidor("Opa.txt")
    # Teste de upload
    # cliente.upload("E:/RPC/Cliente/dowloads/Opa.txt")
    
    # Teste de consulta
    cliente.consulta()
    
    # Teste de download
    # cliente.download("teste.txt", "E:/RPC/Cliente/Opa.txt")

    # Teste de registrar interesse
    # cliente.registrar_interesse("Opa.txt", minutes = 3)

    # Teste Cancelar Interesse
    # cliente.cancerlar_interesse("Opa.txt")