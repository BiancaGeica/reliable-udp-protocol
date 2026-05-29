Geica Elena-Bianca - 323CC

Repository for the third homework of the Communication Networks class. In this homework the students will implement a protocol over UDP that provides reliable transport.

### Scop tema:
    Implementarea unui protocol de transport de tip TCP, construit peste UDP. Acesta asigura trimiterea corecta si completa a unor imagini si a unor fisiere text foarte mari.

### Implementare:
    Pentru a ne asigura ca datele ajung exact asa cum au plecat de la client, am implementat functii specifice protocolului TCP, precum three-way handshake, Selective repeat ARQ (fereastra glisanta) pentru controlul fluxului de pachete, gestionarea pachetelor venite out of order si multiplexarea I/O pentru a reusi sa rezolv cererile date simultan de mai multi clienti.

### 1. Stabilierea conexiunii
    Stabilirea conexiunii se face folosind three-way handshake, implementat astfel incat sa suporte conexiuni multiple, pentru a evita blocajele care pot aparea pe portul principal al serverului.

    In timpul initializarii conexiunii clientul trimite un pachet de tip SYN catre portul serverului (8032, specificat in schelet) si adresa sa publica, dupa care asteapta un SYN de la server pentru a stii ca s-a conectat, in cazul in care nu primeste (apare timeout pentru ca a expirat cronometrul pus la plecarea SYN-ului de la client) incearca sa retrimita SYN-ul.

    Serverul ruleaza un socket pe portul 8032 specificat mai sus, care asculta cereri de tip SYN. In momentul in care primeste SYN, serverul salveaza portul sursa al clientului intr-un map. Daca din diverse motive precum latenta, clientul mai trimite un syn, serverul verifica in map daca stie acel client si daca il gaseste doar ignora ultimul syn ca sa nu produca deadlock.

    Pentru a asigura multiconexiunea, serverul creeaza un scocket nou pentru clientul venit, pe un port liber dat aleatoriu de sistemul de operare si raspunde clientului cu un pachet SYN-ACK, oferind la finalul header-ului portul dedicat.

    La final, clientul preia noul port si ID-ul dat de server, se conecteaza la el si trimite un pachet ACK final de mai multe ori, in stilul UDP, pentru a se asigura ca cel putin unul ajunge la destinatie.

### 2. Transmisia datelor

    Dupa ce a fost efectuat handshake-ul, logica de trimitere se bazeaza pe o fereastra glisanta.

    Datele sunt adaugate intr-un buffer circular. Daca numarul de pachete neconfirmate atinge limita maxima a ferestrei, thread-ul asteapta eliberarea ferestrei.

    Dupa ce datele au fost transmise se asteapta un ACK final. In cazul unui timeout, senderul nu retrimite toate datele pentru a evuta congestia retelei, ci trimite doar ultimele 4 pachete la care nu s-a primit ACK.

    La primirea unui ACK, senderul sterge toate pachetele din fereastra cu o secventa mai mica decat cea confirmata, dupa care muta fereastra la dreapta pentru urmatorul pachet.

### 3. Primirea datelor si controlul fluxului

    La partea de I/O multiplexing, fiecare socket nou creat este inregistrat intr-un array. Serverul asculta, in acelasi timp, toti clientii conectati fara sa faca busy waiting.

    Pachetele ajunse mai devreme decat secventa dinaintea lor sunt stocate intr-o structura separata. Pachetele primite sunt memorate, iar de fiecare data cand apare un pachet asteptat anterior, se verifica fiecare pachet din cele stocate anterior, sunt puse in ordine in fisierul de primire si pachetele care erau in asteptare sunt sterse din structura in care erau puse.
    
    Pentru a preveni coruperea datelor, serverul foloseste un buffer din care aplicatia principala sa isi extraga datele. Atunci cand apare mai mult spatiu liber, datele sunt mutate spre stanga (inceputul bufferului). In caz contrar, daca este plin, functia returneaza 0, aruncand pachetul chiar daca a ajuns corect.

    Daca un pachet nu a fost salvat cu succes, chiar daca a fost primit corect, contorul care numara pachetele nu este incrementat, fortand clientul sa il retrimita pentru a asigura integritatea fisierului final.

    Fereastra ramasa disponibila, este trimisa inapoi impreuna cu fiecare ack.