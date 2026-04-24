import socket

HOST        = '127.0.0.1'
PORT        = 9999
BUFFER_SIZE = 1024

clienti_conectati = {}
mesaje = {}
next_id = 1

server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind((HOST, PORT))

print("=" * 50)
print(f"  SERVER UDP pornit pe {HOST}:{PORT}")
print("  Asteptam mesaje de la clienti...")
print("=" * 50)

while True:
    try:
        date_brute, adresa_client = server_socket.recvfrom(BUFFER_SIZE)
        mesaj_primit = date_brute.decode('utf-8').strip()

        parti = mesaj_primit.split(' ', 1)
        comanda = parti[0].upper()
        argumente = parti[1] if len(parti) > 1 else ''

        print(f"\n[PRIMIT] De la {adresa_client}: '{mesaj_primit}'")

        if comanda == 'CONNECT':
            if adresa_client in clienti_conectati:
                raspuns = "EROARE: Esti deja conectat la server."
            else:
                clienti_conectati[adresa_client] = True
                nr_clienti = len(clienti_conectati)
                raspuns = f"OK: Conectat cu succes. Clienti activi: {nr_clienti}"
                print(f"[SERVER] Client nou conectat: {adresa_client}")

        elif comanda == 'DISCONNECT':
            if adresa_client in clienti_conectati:
                del clienti_conectati[adresa_client]
                raspuns = "OK: Deconectat cu succes. La revedere!"
                print(f"[SERVER] Client deconectat: {adresa_client}")
            else:
                raspuns = "EROARE: Nu esti conectat la server."

        elif comanda == 'PUBLISH':
            if adresa_client not in clienti_conectati:
                raspuns = "EROARE: Nu esti conectat la server."
            elif not argumente:
                raspuns = "EROARE: Mesajul nu poate fi gol."
            else:
                mesaje[next_id] = {'text': argumente, 'autor': adresa_client}
                raspuns = f"OK: Mesaj publicat cu ID={next_id}"
                next_id += 1

        elif comanda == 'DELETE':
            if adresa_client not in clienti_conectati:
                raspuns = "EROARE: Nu esti conectat la server."
            elif not argumente.isdigit():
                raspuns = "EROARE: Argumentul trebuie sa fie un numar intreg."
            else:
                msg_id = int(argumente)
                if msg_id not in mesaje:
                    raspuns = f"EROARE: Mesajul cu ID={msg_id} nu a fost gasit."
                elif mesaje[msg_id]['autor'] != adresa_client:
                    raspuns = "EROARE: Nu esti autorul acestui mesaj."
                else:
                    del mesaje[msg_id]
                    raspuns = "OK: Mesajul a fost sters cu succes."

        elif comanda == 'LIST':
            if adresa_client not in clienti_conectati:
                raspuns = "EROARE: Nu esti conectat la server."
            else:
                if not mesaje:
                    raspuns = "OK: Nu exista mesaje."
                else:
                    linii_mesaje = [f"[{msg_id}] {data['text']}" for msg_id, data in mesaje.items()]
                    raspuns = "OK:\n" + "\n".join(linii_mesaje)

        else:
            raspuns = f"EROARE: Comanda '{comanda}' este necunoscuta. Comenzi valide: CONNECT, DISCONNECT, PUBLISH, DELETE, LIST"

        server_socket.sendto(raspuns.encode('utf-8'), adresa_client)
        print(f"[TRIMIS]  Catre {adresa_client}: '{raspuns}'")

    except KeyboardInterrupt:
        print("\n[SERVER] Oprire server...")
        break
    except Exception as e:
        print(f"[EROARE] {e}")

server_socket.close()
print("[SERVER] Socket inchis.")
