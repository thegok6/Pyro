from Pyro5.api import expose, locate_ns, Proxy
from Pyro5.server import Daemon
from threading import Thread
from time import sleep

@expose
class Follower:
    def __init__(self, role):
        self.log = []
        self.role = role
        self.epoch = 1
        self.uri = ""
        self.total_members = 0

    def set_uri(self, uri):
        self.uri = uri

    @expose
    def update_total_members(self, total_members):
        self.total_members = total_members
        print(f"{self.role.capitalize()} recebeu atualização: Total de membros = {total_members}")
    def send_heartbeat(self, follower_name):
        """Envia *heartbeats* periódicos ao líder."""
        while True:
            try:
                leader_uri = locate_ns().lookup("Lider_Epoca1")
                leader = Proxy(leader_uri)
                # Envia o nome do votante registrado no serviço de nomes
                leader.receive_heartbeat(self.uri)
                print(f"{self.role.capitalize()} enviou heartbeat.")
            except Exception as e:
                print(f"Erro ao enviar heartbeat: {e}")
            sleep(3)  # Intervalo de envio

    @expose
    def promote_to_voter(self):
        """Promove este seguidor para votante e solicita dados ao líder."""
        if self.role == "observador":
            self.role = "votante"
            print(f"{self.role.capitalize()} promovido a votante. Solicitando dados ao líder...")

            # Solicitar dados ao líder para atualização
            leader_uri = locate_ns().lookup("Lider_Epoca1")
            leader = Proxy(leader_uri)

            try:
                offset = len(self.log)  # Offset do log local
                response = leader.fetch_data(self.epoch, offset)

                # Tratar inconsistências e atualizar o log
                while "error" in response:
                    print(f"Inconsistência detectada: {response}")
                    self.epoch = response["max_epoch"]
                    self.log = self.log[:response["last_offset"]]
                    print(f"{self.role.capitalize()} truncou log até o offset {response['last_offset']}.")

                    # Reenviar solicitação com estado corrigido
                    offset = len(self.log)
                    response = leader.fetch_data(self.epoch, offset)


            except Exception as e:
                print(f"Erro ao solicitar dados ao líder: {e}")
        else:
            print(f"{self.role.capitalize()} já é um votante.")

    @expose
    def notify_new_voter(self, new_voter_uri):
        """Recebe notificação do líder sobre um novo votante."""
        print(f"Notificação recebida: Novo votante incluído no cluster: {new_voter_uri}")

    @expose
    def atualiza(self, response):
        if "data" in response:
            self.log.extend(response["data"])
            print(f"{self.role.capitalize()} atualizou log: {self.log}")

    def notify_update(self):
        """Notificado pelo líder para buscar dados."""
        print(f"{self.role.capitalize()} notificado de atualização.")
        leader_uri = locate_ns().lookup("Lider_Epoca1")
        leader = Proxy(leader_uri)

        # Solicitar dados do líder
        offset = len(self.log)  # Tamanho atual do log local
        response = leader.fetch_data(self.epoch, offset)

        # Tratar inconsistências
        while "error" in response:
            print(f"Inconsistência detectada: {response}")

            # Atualizar época e truncar log local conforme resposta do líder
            self.epoch = response["max_epoch"]
            self.log = self.log[:response["last_offset"]]
            print(f"{self.role.capitalize()} truncou log até o offset {response['last_offset']}.")

            # Reenviar solicitação com estado corrigido
            offset = len(self.log)
            response = leader.fetch_data(self.epoch, offset)

        # Processar dados válidos
        if "data" in response:
            self.log.extend(response["data"])  # Adiciona novos dados ao log
            print(f"{self.role.capitalize()} atualizou log: {self.log}")

            # Enviar confirmação de cada entrada ao líder
            for entry_index in range(len(self.log)):
                leader.confirm_commit(locate_ns().lookup("Lider_Epoca1"), entry_index)

    def get_log(self):
        """Retorna o log replicado."""
        return self.log


def start_follower(role, follower_name):
    follower = Follower(role)
    daemon = Daemon()
    ns = locate_ns()
    uri = daemon.register(follower)
    follower.set_uri(uri)
    ns.register(follower_name, uri)
    print(f"{role.capitalize()} registrado no serviço de nomes como '{follower_name}' com URI: {uri}")

    # Inicia envio de *heartbeats* em uma *thread*
    Thread(target=follower.send_heartbeat, args=(follower_name,), daemon=True).start()

    # Registrar no líder
    leader_uri = ns.lookup("Lider_Epoca1")
    leader = Proxy(leader_uri)
    leader.register_follower(uri, role)

    daemon.requestLoop()


if __name__ == "__main__":
    import sys
    role = sys.argv[1]  # 'votante' ou 'observador'
    name = sys.argv[2]  # Nome único para o serviço
    start_follower(role, name)
