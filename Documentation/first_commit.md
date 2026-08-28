Implementiert
- Durable RabbitMQ-Queue delivery.queue, Exchange chat_events, Binding msg.#.
- Konfigurierbarer Pool aus exakt Delivery:WorkerCount langlebigen Worker-Tasks.
- Prefetch entspricht der Workerzahl; interner bounded Channel verhindert unkontrollierte Parallelität.
- Einzel-Ack erst nach erfolgreichem Redis-Routing oder festgestelltem Offline-Status.
- Transiente Redis-/Verarbeitungsfehler: Nack mit Requeue.
- Ungültige oder nicht unterstützte Nachrichten: Nack ohne Requeue.
- Kontrolliertes Herunterfahren: Consumer stoppen, laufende Nachrichten abarbeiten, bei Timeout abbrechen und requeue.
- Redis-Lookup über gateway_for_user:<userId>; die Zuordnung besitzt denselben TTL wie der Presence-Key und dient deshalb zugleich als Online-Signal.
- Weiterleitung an gateway:delivery:<gatewayId>.
- Kein Datenbankzugriff im Delivery Service.
- Gateway registriert Presence und Gateway-Zuordnung, verwaltet lokale WebSockets und empfängt Redis-Pub/Sub-Nachrichten.
- Worker besitzt keine öffentlichen Ports.
Die RabbitMQ-Implementierung kopiert den Message-Body innerhalb des Consumer-Callbacks, bestätigt Nachrichten einzeln und serialisiert Channel-Acknowledgements entsprechend den offiziellen Concurrency-Empfehlungen des .NET RabbitMQ Client Guide.
Die Recovery besitzt nur einen Pfad: Bei einem Verbindungsabbruch beendet der Consumer seine Session und Worker.cs erstellt nach dem konfigurierten Delay eine neue. Acknowledgements bleiben dabei an den Channel gebunden, über den die Nachricht empfangen wurde.
Vertragsentscheidung
Der widersprüchliche Altvertrag wurde als Contract v2 vereinheitlicht:
- messageId
- roomId
- senderId
- targetId
- Unix-Timestamp in Millisekunden
- payload.encryptedKey
- payload.iv
- payload.ciphertext
- payload.signature
roomId wurde aufgenommen, obwohl es im JSON-Beispiel einzelner Wiki-Abschnitte fehlt, weil es in der Aufgabenstellung und den Signatur-/Raumanforderungen ausdrücklich benötigt wird. Das ältere ChatMessageEvent bleibt als veralteter Kompatibilitätstyp erhalten.
Für Routing wurde die neuere Baseline msg.private.<targetId> und msg.group.<groupId> statt des älteren chat.private.* gewählt.
Geänderte Dateien
Delivery Service:
- Stammdateien: [Program.cs], [Worker.cs], [Delivery-Service.csproj], [appsettings.json], [Dockerfile], Dockerfile.dockerignore, [README.md], nuget.config
- Configuration/: DeliveryOptions.cs, RabbitMqOptions.cs, RedisOptions.cs
- Messaging/: DeliveryQueueMessage.cs, IDeliveryQueueConsumer.cs, RabbitMqDeliveryQueueConsumer.cs
- Processing/: DeliveryDispatcher.cs, DeliveryMessageProcessor.cs, IDeliveryMessageProcessor.cs
- Routing/: IDeliveryRouter.cs, IRedisGatewayStore.cs, RedisDeliveryRouter.cs, RedisGatewayStore.cs
- [Delivery-Service.Tests]: Testprojekt und sieben Testdateien
Gateway:
- [Program.cs], [ChatController.cs], [Gateway.csproj]
- Services/ChatManagerService.cs, IChatManagerService.cs
- Services/RedisUserPresenceStore.cs, IUserPresenceStore.cs
- Services/WebSocketConnectionRegistry.cs, IWebSocketConnectionRegistry.cs
- Services/RedisDeliverySubscriber.cs
- Configuration/GatewayOptions.cs, RedisRoutingOptions.cs
- appsettings.json, Dockerfile, Dockerfile.dockerignore, README.md, global.json
Contracts:
- [ChatMessagePublishedEvent.cs]
- Events/ChatMessageEvent.cs
- [SendMessageRequest.cs]
- Messages/EncryptedMessagePayload.cs
- EVA-ECS.Chat.Contracts.csproj
- README.md
Docker:
- [docker-compose.yaml]
Das Wiki wurde nicht verändert. Der bereits vorhandene unversionierte .serena-Ordner blieb unangetastet.
Tests und Prüfungen
- Delivery Unit-/Lifecycle-Tests: 16/16 bestanden
  - gültige RabbitMQ-Nachricht und gemeinsamer Contract
  - MassTransit-Envelope
  - parallele Workerbegrenzung
  - korrektes Gateway-Routing
  - fehlende Redis-Zuordnung
  - Redis-Fehler und Requeue
  - Ack-/Nack-Verhalten
  - RabbitMQ-Ack-Fehler
  - RabbitMQ-Nack-Fehler beendet keinen Worker
  - ungültige Nachrichten
  - Shutdown-Drain und Shutdown-Timeout
  - RabbitMQ-Startfehler mit Retry
- Delivery Build: erfolgreich, 0 Fehler.
- Gateway Build: erfolgreich, 0 Fehler.
- Contracts Build: erfolgreich, 0 Fehler.
- dotnet format --verify-no-changes: alle Projekte erfolgreich.
- docker compose config --quiet: erfolgreich.
- Delivery- und Gateway-Docker-Images: erfolgreich gebaut.
- Lokaler Smoke-Test:
  - delivery.queue ist durable.
  - Binding chat_events -> delivery.queue mit msg.# vorhanden.
  - Consumer startete mit 4 Workern und Prefetch 4.
  - Container stoppte sauber.
  - Temporärer Worker-Container wurde anschließend entfernt; Infrastruktur und Volumes blieben bestehen.
Noch erforderliche Teamarbeit
Gateway-Team:
- ECDSA-Signaturprüfung und Raum-/Mitgliedschaftsprüfung vor dem RabbitMQ-Publish implementieren.
- Den dokumentierten einmaligen /api/ws-ticket-Ablauf umsetzen; aktuell verwendet das Gateway weiterhin den älteren Token-Query-Ablauf.
- delivery_ack definieren und erst nach tatsächlicher Client-Zustellung erzeugen. Redis-Publish bestätigt nur den Gateway-Subscriber, nicht den Client.
- Für mehrere Gateway-Container eindeutige Gateway-IDs und Compose-Skalierung ohne feste container_name-/Host-Port-Kollisionen konfigurieren.
Contracts-Team:
- Contract v2 veröffentlichen und alle Consumer koordiniert migrieren.
- Kanonische Bytekodierung der signierten Felder festlegen.
- Für Gruppenchats Empfängerliste beziehungsweise per-user Schlüsselumschläge definieren. Bis dahin verarbeitet Delivery ausschließlich msg.private.*.
Storage-Team:
- Contract v2 übernehmen.
- storage.queue an chat_events/msg.# binden.
- Idempotente Speicherung über messageId, Ack erst nach Commit, maximal drei Versuche und chat_dlq_storage implementieren.
- Offline-Historie und server_ack bereitstellen.
Sicherheitshinweis: Ein zuvor eingechecktes Package-Credential wurde aus der aktuellen Delivery-Konfiguration entfernt und durch Umgebungsvariablen ersetzt. Da es bereits im Git-Verlauf lag, sollte das zuständige Team es widerrufen beziehungsweise rotieren.
