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

    def send_heartbeat(self, follower_name):
        """Envia *heartbeats* periódicos ao líder."""
        while True:
            try:
                leader_uri = locate_ns().lookup("Lider_Epoca1")
                leader = Proxy(leader_uri)
                # Envia o nome do votante registrado no serviço de nomes
                leader.receive_heartbeat(follower_name)
                print(f"{self.role.capitalize()} enviou heartbeat.")
            except Exception as e:
                print(f"Erro ao enviar heartbeat: {e}")
            sleep(2)  # Intervalo de envio

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
