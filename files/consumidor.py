import Pyro5.api

def consume_data():
    ns = Pyro5.api.locate_ns()
    leader_uri = ns.lookup("Lider_Epoca1")
    leader = Pyro5.api.Proxy(leader_uri)

    print("Consumidor conectado ao líder. Lendo log...")
    while True:
        input("Pressione Enter para buscar mensagens comprometidas.")
        log = leader.get_log()
        print("Log comprometido:", log)


if __name__ == "__main__":
    consume_data()