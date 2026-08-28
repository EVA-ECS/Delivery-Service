# EVA Delivery Service

Der Delivery Service konsumiert die dauerhafte RabbitMQ-Queue `delivery.queue`
und leitet private, Ende-zu-Ende-verschlüsselte Chatnachrichten an die Gateway-
Instanz weiter, an der der Empfänger aktuell verbunden ist. Der Service besitzt
keinen Datenbankzugriff.

## Verarbeitungsmodell

- `Delivery:WorkerCount` erzeugt exakt diese Anzahl langlebiger Worker-Tasks.
- RabbitMQ-Prefetch und die Kapazität des internen bounded Channels entsprechen
  der Workerzahl. Dadurch sind nie mehr Nachrichten unbestätigt als Worker
  verfügbar sind.
- `basic.ack` erfolgt einzeln und erst nach erfolgreichem Redis-Routing oder nach
  der erfolgreichen Feststellung, dass der Empfänger offline ist.
- Ungültige beziehungsweise nicht unterstützte Nachrichten werden ohne Requeue
  abgelehnt. Transiente Redis-/Verarbeitungsfehler werden mit Requeue abgelehnt.
- Beim Herunterfahren wird der RabbitMQ-Consumer zuerst gestoppt; laufende Arbeit
  darf bis `Delivery:ShutdownTimeoutSeconds` auslaufen.
- Verbindungsabbrüche werden an einer Stelle behandelt: Der Worker erstellt nach
  einer kurzen Pause eine neue RabbitMQ-Session. Die automatische Client-Recovery
  ist deshalb deaktiviert.

Offline ist im Delivery-Pfad ein erfolgreich verarbeitetes Ergebnis: Der Worker
speichert nichts und bestätigt keine Persistenz. Die getrennte `storage.queue`
und der Storage Service sind für dauerhafte Speicherung und spätere Synchronisation
verantwortlich.

## Gemeinsamer Vertrag und MVP-Grenze

Verwendet wird `ChatMessagePublishedEvent` aus dem Contracts-Repository (v2) mit
`messageId`, `roomId`, `senderId`, `targetId`, Unix-Timestamp in Millisekunden
sowie `payload.encryptedKey`, `payload.iv`, `payload.ciphertext` und
`payload.signature`. Der Consumer akzeptiert sowohl rohes JSON als auch den
MassTransit-Envelope mit einer `message`-Eigenschaft.

Der aktuelle MVP liefert private Nachrichten (`msg.private.<targetId>`) aus.
Für `msg.group.<groupId>` fehlt im bestehenden Vertrag eine Liste der einzelnen
Empfänger beziehungsweise ihrer Schlüsselumschläge. Solche Ereignisse werden
nicht endlos erneut zugestellt, sondern als nicht unterstützt abgelehnt; der
Storage-Pfad bleibt davon unabhängig.

## Konfiguration

Alle Einstellungen können über die übliche .NET-Schreibweise überschrieben
werden, beispielsweise `Delivery__WorkerCount`, `RabbitMQ__Host`,
`RabbitMQ__Username`, `RabbitMQ__Password`, `Redis__ConnectionString`,
`Redis__GatewayMappingKeyPrefix` und `Redis__DeliveryChannelPrefix`.

Der Docker-Build wird aus dem gemeinsamen EVA-Verzeichnis gestartet:

```powershell
docker build -f Delivery-Service/Dockerfile .
```
