import Pyro5.api

def publish_data():
    ns = Pyro5.api.locate_ns()
    leader_uri = ns.lookup("Lider_Epoca1")
    leader = Pyro5.api.Proxy(leader_uri)

    while True:
        data = input("Digite uma mensagem para publicar: ")
        ack = leader.publish(data)
        print(f"Líder respondeu: {ack}")


if __name__ == "__main__":
    publish_data()