import threading

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
        self.heartbeat_timeout = 5  # Tempo limite para considerar um broker "offline"
        self.quorum_size = 2
        self.temp_log = []

        Timer(5, self.monitor_heartbeats).start()

    def register_follower(self, follower_uri, state):
        """Registra votantes e observadores."""
        self.followers.append({"uri": follower_uri, "state": state})
        self.heartbeats[follower_uri] = time()  # Marca o momento do registro
        print(f"Follower registrado: {follower_uri} como {state}")



    def receive_heartbeat(self, follower_uri):
        heartbeat_lock = threading.Lock()
        """Atualiza o último *heartbeat* recebido de um votante."""
        with heartbeat_lock:
            self.heartbeats[follower_uri] = time()
        print(f"Heartbeat recebido de {follower_uri} - Atualizado: {self.heartbeats[follower_uri]}")

    def monitor_heartbeats(self):
        """Verifica periodicamente se os votantes estão ativos e gerencia redistribuição."""
        current_time = time()
        active_voters = []
        inactive_voters = []

        for follower in self.followers:
            uri = follower["uri"]
            last_heartbeat = self.heartbeats.get(uri, 0)

            # Verifica se o votante está ativo
            print(
                f"current_time: {current_time}, last_heartbeat: {last_heartbeat}, heartbeat_timeout: {self.heartbeat_timeout} of {uri}")
            print(f"Comparison result: {current_time - last_heartbeat <= self.heartbeat_timeout}")
            if current_time - last_heartbeat <= self.heartbeat_timeout:
                if follower["state"] == "votante":
                    active_voters.append(follower)
            else:
                print(f"Atenção: {uri} está offline!")
                if follower["state"] == "votante":
                    inactive_voters.append(follower)

        # Verifica se o número de votantes ativos atende ao quórum
        if len(active_voters) < self.quorum_size:
            print(f"Atenção: Quórum insuficiente! Votantes ativos: {len(active_voters)}. Necessário: {self.quorum_size}.")
            self.redistribute_partitions(inactive_voters)

        # Reagendar a verificação
        Timer(15, self.monitor_heartbeats).start()

    def redistribute_partitions(self, inactive_voters):
        """Redistribui partições e promove observadores para votantes."""
        for inactive_voter in inactive_voters:
            print(f"Redistribuindo dados de votante inativo: {inactive_voter['uri']}")

            # Promover um observador para votante, se possível
            for follower in self.followers:
                if follower["state"] == "observador":
                    follower["state"] = "votante"
                    print(f"Observador {follower['uri']} promovido a votante.")
                    self.notify_promotion(follower)

                    # Notificar todos os votantes sobre o novo votante
                    self.notify_all_voters(follower)
                    break
            else:
                print("Erro: Não há observadores disponíveis para promover!")

    def notify_all_voters(self, new_voter):
        """Notifica todos os votantes sobre a inclusão de um novo votante."""
        for follower in self.followers:
            if follower["state"] == "votante" and follower["uri"] != new_voter["uri"]:
                try:
                    proxy = Proxy(follower["uri"])
                    proxy.notify_new_voter(new_voter["uri"])
                    print(f"Votante {follower['uri']} notificado sobre o novo votante {new_voter['uri']}.")
                except Exception as e:
                    print(f"Erro ao notificar votante {follower['uri']} sobre o novo votante: {e}")

    def notify_total_members(self):
        total_members = len(self.followers)
        for follower in self.followers:
            if follower["state"] == "votante":
                try:
                    proxy = Proxy(follower["uri"])
                    proxy.update_total_members(total_members)
                    print(f"Notificado {follower['uri']} sobre total de membros: {total_members}")
                except Exception as e:
                    print(f"Erro ao notificar {follower['uri']} sobre total de membros: {e}")

    def notify_promotion(self, follower):
        """Notifica o observador promovido sobre sua nova responsabilidade."""
        try:
            proxy = Proxy(follower["uri"])
            proxy.promote_to_voter()
            print(f"Observador {follower['uri']} notificado sobre promoção a votante.")
        except Exception as e:
            print(f"Erro ao notificar promoção de {follower['uri']}: {e}")

    def publish(self, data):
        """Adiciona dados ao log e notifica os votantes."""
        if len([f for f in self.followers if
                time() - self.heartbeats.get(f["uri"], 0) <= self.heartbeat_timeout]) < self.quorum_size:
            return "Erro: Quórum insuficiente. Dados não publicados."

        log_entry = {"epoch": self.epoch, "data": data}
        self.temp_log = self.log
        self.temp_log.append(log_entry)
        entry_index = len(self.temp_log) - 1
        self.confirmations[entry_index] = 1  # O líder sempre confirma sua própria entrada

        print(f"Líder recebeu: {data}, notificando votantes...")

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
        if epoch > self.epoch or offset > len(self.temp_log):
            max_epoch = self.epoch
            last_offset = len(self.temp_log)
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
            self.log = self.temp_log
            self.temp_log = []
            for follower in self.followers:
                try:
                    proxy = Proxy(follower["uri"])
                    proxy.atualiza(self.log[len(self.log) - 1])
                except Exception as e:
                    print(f"Erro ao notificar {follower['uri']}: {e}")
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
    daemon.requestLoop()


if __name__ == "__main__":
    start_leader()
