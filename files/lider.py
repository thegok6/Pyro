from Pyro5.api import expose, locate_ns, Proxy
from Pyro5.server import Daemon
from threading import Timer
from time import time

@expose
class Leader:
    def __init__(self):
        self.log = []
        self.followers = []
        self.epoch = 1
        self.confirmations = {}  # Para acompanhar as confirmações por entrada
        self.heartbeats = {}  # Últimos *heartbeats* recebidos
        self.heartbeat_timeout = 20  # Tempo limite para considerar um broker "offline"
        self.quorum_size = 2

    def register_follower(self, follower_uri, state):
        """Registra votantes e observadores."""
        self.followers.append({"uri": follower_uri, "state": state})
        self.heartbeats[follower_uri] = time()  # Marca o momento do registro
        print(f"Follower registrado: {follower_uri} como {state}")

    def receive_heartbeat(self, follower_uri):
        """Atualiza o último *heartbeat* recebido de um votante."""
        self.heartbeats[follower_uri] = time()
        print(f"Heartbeat recebido de {follower_uri}")

    def monitor_heartbeats(self):
        """Verifica periodicamente se os votantes estão ativos."""
        current_time = time()
        for follower in self.followers:
            uri = follower["uri"]
            last_heartbeat = self.heartbeats.get(uri, 0)
            if current_time - last_heartbeat <= self.heartbeat_timeout:
                active_followers += 1
            else:
                print(f"Atenção: {uri} está offline!")
            current_time = 0
            last_heartbeat = 0
        # Reagendar a verificação
        Timer(15, self.monitor_heartbeats).start()  # Executa a cada 5 segundos

    def publish(self, data):
        """Adiciona dados ao log e notifica os votantes."""

        log_entry = {"epoch": self.epoch, "data": data}
        self.log.append(log_entry)
        entry_index = len(self.log) - 1
        self.confirmations[entry_index] = 1  # O líder sempre confirma sua própria entrada

        print(f"Líder recebeu: {data}, notificando votantes...")

        # Notifica votantes
        for follower in self.followers:
            if follower["state"] == "votante":
                try:
                    proxy = Proxy(follower["uri"])
                    proxy.notify_update()
                except Exception as e:
                    print(f"Erro ao notificar {follower['uri']}: {e}")

        return "Dados adicionados ao log e votantes notificados."

    def fetch_data(self, epoch, offset):
        """Votantes solicitam dados a partir de um epoch e offset."""
        # Verificar se o epoch ou offset está inconsistente
        if epoch > self.epoch or offset > len(self.log):
            max_epoch = self.epoch
            last_offset = len(self.log)
            return {
                "error": "Inconsistência detectada",
                "max_epoch": max_epoch,
                "last_offset": last_offset,
            }

        # Retornar dados consistentes a partir do offset
        return {"data": self.log[offset:]}

    def confirm_commit(self, follower_uri, entry_index):
        """Recebe confirmações dos votantes e verifica o quórum."""
        if entry_index in self.confirmations:
            self.confirmations[entry_index] += 1
        else:
            self.confirmations[entry_index] = 1

        commit_count = self.confirmations[entry_index]
        if commit_count >= (len(self.followers) // 2) + 1:
            print(f"Entrada {entry_index} confirmada pelo quórum!")
            return True
        return False

    @expose
    def get_log(self):
        """Retorna o log comprometido."""
        print("Log comprometido enviado ao consumidor.")
        return self.log


def start_leader():
    leader = Leader()
    daemon = Daemon()
    ns = locate_ns()
    uri = daemon.register(leader)
    ns.register("Lider_Epoca1", uri)
    print(f"Líder registrado no serviço de nomes como 'Lider_Epoca1' com URI: {uri}")

    # Inicia a monitoração dos *heartbeats*
    Timer(5, leader.monitor_heartbeats).start()

    daemon.requestLoop()


if __name__ == "__main__":
    start_leader()
